//! Read the content of a [`crate::document`] through a [`crate::connector`].
//!
//! ### Actions
//!
//! 1 - Get a [`crate::Context`] from the input queue.  
//! 2 - Extract the [`crate::DataResult`] from the [`crate::Context`].  
//! 3 - Put the data in the parameter of the [`crate::connector`].  
//! 4 - Read bytes from the [`crate::document`] through the [`crate::connector`].  
//! 5 - Create a new [`crate::Context`] and attach the [`crate::DataResult`] to it.  
//! 6 - Push the new [`crate::Context`] into the output queue.  
//! 7 - Go to step 1 until the input queue is not empty.  
//!
//! ### Configuration
//!
//! | key         | alias | Description                                                                     | Default Value | Possible Values                              |
//! | ----------- | ----- | ------------------------------------------------------------------------------- | ------------- | -------------------------------------------- |
//! | type        | -     | Required in order to use reader step                                            | `reader`      | `reader` / `read` / `r`                      |
//! | connector   | conn  | Connector type to use in order to read a resource                               | `io`          | See [`crate::connector`]                     |
//! | document    | doc   | Document type to use in order to manipulate the resource                        | `json`        | See [`crate::document`]                      |
//! | name        | alias | Step name                                                                       | `null`        | Auto generate alphanumeric value             |
//! | data_type   | data  | Type of data the reader push in the queue : [ ok / err ]                        | `ok`          | `ok` / `err`                                 |
//! | concurrency_limit | - | Limit of steps to run in conccuence.                                          | `1`           | unsigned number                              |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "reader",
//!         "name": "read_a",
//!         "connector": {
//!             "type": "io"
//!         },
//!         "document": {
//!             "type": "json"
//!         },
//!         "data_type": "ok",
//!         "concurrency_limit": 1
//!     }
//!     ...
//! ]
//! ```
use crate::connector::Connector;
use crate::document::{Document, DocumentType};
use crate::step::Step;
use crate::DataResult;
use crate::{connector::ConnectorType, Context};
use async_channel::{Receiver, Sender};
use async_std::task;
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use std::io;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Reader {
    #[serde(rename = "connector")]
    #[serde(alias = "conn")]
    pub connector_type: ConnectorType,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    pub document_type: DocumentType,
    #[serde(alias = "alias")]
    pub name: String,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(skip)]
    pub receiver: Option<Receiver<Context>>,
    #[serde(skip)]
    pub sender: Option<Sender<Context>>,
    pub concurrency_limit: usize,
}

impl Default for Reader {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Reader {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            name: uuid.simple().to_string(),
            data_type: DataResult::OK.to_string(),
            receiver: None,
            sender: None,
            concurrency_limit: 1,
        }
    }
}

#[async_trait]
impl Step for Reader {
    /// See [`Step::set_receiver`] for more details.
    fn set_receiver(&mut self, receiver: Receiver<Context>) {
        self.receiver = Some(receiver);
    }
    /// See [`Step::receiver`] for more details.
    fn receiver(&self) -> Option<&Receiver<Context>> {
        self.receiver.as_ref()
    }
    /// See [`Step::set_sender`] for more details.
    fn set_sender(&mut self, sender: Sender<Context>) {
        self.sender = Some(sender);
    }
    /// See [`Step::sender`] for more details.
    fn sender(&self) -> Option<&Sender<Context>> {
        self.sender.as_ref()
    }
    #[instrument(name = "reader::exec",
        skip(self),
        fields(name=self.name, 
        data_type=self.data_type,
        concurrency_limit=self.concurrency_limit))]
    async fn exec(&self) -> io::Result<()> {
        info!("Start reading data...");
        
        let mut connector = self.connector_type.clone().boxed_inner();
        let document = self.document_type.ref_inner();
        connector.set_metadata(connector.metadata().merge(&document.metadata()));
        let mut receiver_stream = self.receive().await;
        // Used to check if one data has been received.
        let mut has_data_been_received = false;
        
        while let Some(context_received) = receiver_stream.next().await {
            if !has_data_been_received {
                has_data_been_received = true;
            }

            if !context_received.input().is_type(self.data_type.as_ref()) {
                trace!("Handles only this data type");
                self.send(&context_received).await;
                continue;
            }

            connector.set_parameters(context_received.to_value()?);
            
            connector.paginate().await?
                .filter_map(|connector_result| async { match connector_result {
                    Ok(connector) => Some(connector),
                    Err(e) => {
                        warn!(
                            error = e.to_string().as_str(),
                            "Pagination through the paginator failed"
                        );
                        None
                    }
                }})
                .for_each_concurrent(None, |connector| {
                    let step = self.clone();
                    let context = Some(context_received.clone());
                    async move {
                        read(&step, &mut connector.clone(), document, &context).await;
                }}).await;
        }

        // If data has not been received and the channel has been close, run last time the step.
        // It arrive when the previous step don't push data through the pipe.
        if !has_data_been_received {
            connector.paginate().await?
                .filter_map(|connector_result| async { match connector_result {
                    Ok(connector) => Some(connector),
                    Err(e) => {
                        warn!(
                            error = e.to_string().as_str(),
                            "Pagination through the paginator failed"
                        );
                        None
                    }
                }})
                .for_each_concurrent(None, |connector| {
                    let step = self.clone();
                    async move {
                        read(&step, &mut connector.clone(), document, &None).await;
                }}).await;
        }

        info!("Stops reading data and sending context in the channel");

        Ok(())
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

async fn read<'step>(
    step: &'step Reader,
    connector: &'step mut Box<dyn Connector>,
    document: &'step dyn Document,
    context: &'step Option<Context>,
) {            
    let dataset = match connector.fetch(document).await {
        Ok(Some(dataset)) => {
            info!("forward read");
            dataset
        },
        Ok(None) => {
            info!("No data found through the connector");
            return
        },
        Err(e) => {
            warn!(
                error = e.to_string().as_str(),
                "fetch data failed"
            );
            return;
        }
    };

    let step = step.clone();
    let context = context.clone();

    task::spawn(async move {
        let step: Reader = step.clone();
        dataset.map(|data_result| async {
            let context = match context.clone() {
                Some(ref mut context) => {
                    context.insert_step_result(step.name(), data_result);
                    context.clone()
                },
                None => Context::new(step.name(), data_result),
            };
            step.send(&context).await;
        }).buffer_unordered(usize::MAX)
        .collect::<Vec<_>>()
        .await;
    }).await;
}

#[cfg(test)]
mod tests {
    use crate::connector::in_memory::InMemory;

    use super::*;
    use serde_json::Value;
    use std::io::{Error, ErrorKind};
    use std::thread;

    #[async_std::test]
    async fn exec_with_different_data_result_type() {
        let mut step = Reader::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let error = Error::new(ErrorKind::InvalidData, "My error");
        let context = Context::new("before".to_string(), DataResult::Err((data, error)));
        let expected_context = context.clone();

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
    #[async_std::test]
    async fn exec_with_same_data_result_type() {
        let mut step = Reader::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone()));

        let mut expected_context = context.clone();
        let data2: Value = serde_json::from_str(r#"{"field_1":"value_2"}"#).unwrap();
        expected_context.insert_step_result("my_step".to_string(), DataResult::Ok(data2));

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.connector_type = ConnectorType::InMemory(InMemory::new(r#"{"field_1":"value_2"}"#));
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
}