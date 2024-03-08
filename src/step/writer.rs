//! Write data into the [`crate::document`] through the [`crate::connector`].
//!
//! ### Actions
//!
//! 1 - Get a [`crate::Context`] from the input queue.  
//! 2 - Extract the [`crate::DataResult`] from the [`crate::Context`].  
//! 2 - Write data in the [`crate::document`] though the [`crate::connector`].  
//! 5 - Clone the current [`crate::Context`].  
//! 6 - Push the [`crate::Context`] into the output queue.  
//! 7 - Go to step 1 until the input queue is not empty.  
//!
//! ### Configuration
//!
//! | key           | alias   | Description                                                                     | Default Value | Possible Values                              |
//! | ------------- | ------- | ------------------------------------------------------------------------------- | ------------- | -------------------------------------------- |
//! | type          | -       | Required in order to use writer step.                                            | `writer`      | `writer` / `write` / `w`                     |
//! | connector_tyoe     | conn / connector    | Connector type to use in order to read a resource.                               | `io`          | See [`crate::connector`] |
//! | document_tyoe      | doc / document    | Document type to use in order to manipulate the resource.                        | `json`        | See [`crate::document`]   |
//! | name          | alias   | Name step.                                                                       | `null`        | Auto generate alphanumeric value             |
//! | data_type     | data    | Data type read for writing. skip other data type.                             | `ok`          | `ok` / `err`                                 |
//! | concurrency_limit | -| Limit of steps to run in concurrence.                                        | `1`           | unsigned number                              |
//! | dataset_limit  | batch   | Stack size limit before to push data into the resource though the connector.     | `1000`        | unsigned number                              |
//!
//! ### Examples
//!
//! ```json
//! [
//!     ...
//!     {
//!         "type": "writer",
//!         "name": "write_a",
//!         "connector": {
//!             "type": "io"
//!         },
//!         "document": {
//!             "type": "json"
//!         },
//!         "data": "ok",
//!         "concurrency_limit": 1,
//!         "dataset_limit": 1000
//!     },
//!     {
//!         "type": "writer",
//!         "name": "write_b",
//!         "connector": {
//!             "type": "local",
//!             "path": "./data/my_data.{{ metadata.mime_subtype }}"
//!         },
//!         "document": {
//!             "type": "json"
//!         }
//!     }
//!     ...
//! ]
//! ```
use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use crate::Context;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::io;
use uuid::Uuid;
use crate::helper::string::DisplayOnlyForDebugging;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Writer {
    #[serde(rename = "connector")]
    #[serde(alias = "conn")]
    connector_type: ConnectorType,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    document_type: DocumentType,
    #[serde(alias = "alias")]
    pub name: String,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    pub dataset_limit: usize,
    pub concurrency_limit: usize,
    #[serde(skip)]
    pub receiver: Option<Receiver<Context>>,
    #[serde(skip)]
    pub sender: Option<Sender<Context>>,
}

impl Default for Writer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Writer {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            name: uuid.simple().to_string(),
            data_type: DataResult::OK.to_string(),
            dataset_limit: 1000,
            concurrency_limit: 1,
            receiver: None,
            sender: None,
        }
    }
}

// This Step write data from somewhere into another stream.
#[async_trait]
impl Step for Writer {
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
    #[instrument(name = "writer::exec",
        skip(self),
        fields(name=self.name, 
        data_type=self.data_type,
        concurrency_limit=self.concurrency_limit,
        dataset_limit=self.dataset_limit,
    ))]
    async fn exec(&self) -> io::Result<()> {
        info!("Start writing data...");
        
        let mut total_written: usize = 0;
        let mut connector = self.connector_type.clone().boxed_inner();
        let document = self.document_type.clone().boxed_inner();
        connector.set_document(&document.clone_box())?;
        
        let mut dataset = Vec::default();

        let mut receiver_stream = self.receive().await;

        // Use to init the connector during the loop
        let default_connector = connector.clone();
        let mut last_context_received = None;

        while let Some(context_received) = receiver_stream.next().await {
            if !context_received.input().is_type(self.data_type.as_ref()) {
                trace!("Handles only this data type");
                self.send(&context_received).await;
                continue;
            }
            last_context_received = Some(context_received.clone());

            {
                // If the path change and the inner connector not empty, the connector
                // flush and send the data to the remote document before to load a new document.
                if connector.is_resource_will_change(context_received.to_value()?)?
                    && !dataset.is_empty()
                {
                    info!(dataset_length = dataset.len(), "Next write");

                    match connector.send(&dataset).await {
                        Ok(_) => {
                            total_written+=dataset.len();
                            info!(dataset_length = dataset.len(), total = &total_written, "Write with success");

                            for data in dataset {
                                let mut context = context_received.clone();
                                context.insert_step_result(self.name(), data);

                                self.send(&context).await;
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = format!("{:?}", &e).as_str(),
                                dataset = &dataset.display_only_for_debugging(),
                                "Can't write data"
                            );

                            for data in dataset {
                                let mut context = context_received.clone();
                                context.insert_step_result(
                                    self.name(),
                                    DataResult::Err((
                                        data.to_value(),
                                        io::Error::new(e.kind(), e.to_string()),
                                    )),
                                );

                                self.send(&context).await;
                            }
                        }
                    };
                    dataset = Vec::default();
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(context_received.to_value()?);
            dataset.push(context_received.input());

            if self.dataset_limit <= dataset.len() && document.can_append() {
                info!(dataset_length = dataset.len(), "Next write");

                match connector.send(&dataset).await {
                    Ok(_) => {
                        total_written+=dataset.len();
                        info!(dataset_length = dataset.len(), total = total_written, "Write with success");

                        for data in dataset {
                            let mut context = context_received.clone();
                            context.insert_step_result(self.name(), data);
                            self.send(&context).await;
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = format!("{:?}", &e).as_str(),
                            dataset = &dataset.display_only_for_debugging(),
                            "Can't write data"
                        );

                        for data in dataset {
                            let mut context = context_received.clone();
                            context.insert_step_result(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            );

                            self.send(&context).await;
                        }
                    }
                };

                dataset = Vec::default();
            }
        }

        if !dataset.is_empty() {
            info!(dataset_length = dataset.len(), "Last write");

            match connector.send(&dataset).await {
                Ok(_) => {
                    total_written+=dataset.len();
                    info!(dataset_length = dataset.len(), total = total_written, "Write with success");

                    for data in dataset {
                        let context = match &last_context_received {
                            Some(context_received) => {
                                let mut context = context_received.clone();
                                context.insert_step_result(self.name(), data);
                                context
                            }
                            None => Context::new(self.name(), data),
                        };

                        self.send(&context).await;
                    }
                }
                Err(e) => {
                    warn!(
                        error = format!("{:?}", &e).as_str(),
                        dataset = &dataset.display_only_for_debugging(),
                        "Can't write data"
                    );

                    for data in dataset {
                        let context = match &last_context_received {
                            Some(context_received) => {
                                let mut context = context_received.clone();
                                context.insert_step_result(
                                    self.name(),
                                    DataResult::Err((
                                        data.to_value(),
                                        io::Error::new(e.kind(), e.to_string()),
                                    )),
                                );
                                context
                            }
                            None => Context::new(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            ),
                        };

                        self.send(&context).await;
                    }
                }
            };
        }

        info!(
            total = total_written,
            "Stops writing data and sending context in the channel"
        );

        Ok(())
    }
    fn number(&self) -> usize {
        self.concurrency_limit
    }
    fn name(&self) -> String {
        self.name.clone()
    }
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
        let mut step = Writer::default();
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
        let mut step = Writer::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone()));

        let mut expected_context = context.clone();
        expected_context.insert_step_result("my_step".to_string(), DataResult::Ok(data.clone()));

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.connector_type = ConnectorType::InMemory(InMemory::default());
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
}