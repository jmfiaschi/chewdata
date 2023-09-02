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
//! | type          | -       | Required in order to use writer step                                            | `writer`      | `writer` / `write` / `w`                     |
//! | connector     | conn    | Connector type to use in order to read a resource                               | `io`          | See [`crate::connector`] |
//! | document      | doc     | Document type to use in order to manipulate the resource                        | `json`        | See [`crate::document`]   |
//! | name          | alias   | Name step                                                                       | `null`        | Auto generate alphanumeric value             |
//! | description   | desc    | Describ your step and give more visibility                                      | `null`        | String                                       |
//! | data_type     | data    | Data type read for writing. skip other data type                             | `ok`          | `ok` / `err`                                 |
//! | thread_number | threads | Parallelize the step in multiple threads                                        | `1`           | unsigned number                              |
//! | dataset_size  | batch   | Stack size limit before to push data into the resource though the connector     | `1000`        | unsigned number                              |
//!
//! ### Examples
//! 
//! ```json
//! [
//!     ...
//!     {
//!         "type": "writer",
//!         "name": "write_a",
//!         "description": "My description of the step",
//!         "connector": {
//!             "type": "io"
//!         },
//!         "document": {
//!             "type": "json"
//!         },
//!         "data": "ok",
//!         "thread_number": 1,
//!         "dataset_size": 1000
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
use std::{fmt, io};
use uuid::Uuid;

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
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    pub dataset_size: usize,
    #[serde(alias = "threads")]
    pub thread_number: usize,
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
            description: None,
            data_type: DataResult::OK.to_string(),
            dataset_size: 1000,
            thread_number: 1,
            receiver: None,
            sender: None,
        }
    }
}

impl fmt::Display for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Writer {{'{}','{}'}}",
            self.name,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
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
    #[instrument(name = "writer::exec")]
    async fn exec(&self) -> io::Result<()> {
        let mut connector = self.connector_type.clone().boxed_inner();
        let document = self.document_type.clone().boxed_inner();
        let mut dataset = Vec::default();

        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Use to init the connector during the loop
        let default_connector = connector.clone();
        let mut last_context_received = None;

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(context_received) = receiver_stream.next().await {
            if !context_received
                .input()
                .is_type(self.data_type.as_ref())
            {
                trace!("This step handle only this data type");
                super::send(self as &dyn Step, &context_received.clone()).await?;
                continue;
            }
            last_context_received = Some(context_received.clone());

            {
                // If the path change and the inner connector not empty, the connector
                // flush and send the data to the remote document before to load a new document.
                if connector.is_resource_will_change(context_received.to_value()?)?
                    && !dataset.is_empty()
                {
                    match connector.send(&*document, &dataset).await {
                        Ok(_) => {
                            info!("Dataset sended with success into the connector");
                            for data in dataset {
                                let mut context = context_received.clone();
                                context.insert_step_result(self.name(), data)?;

                                super::send(self as &dyn Step, &context).await?;
                            }
                        }
                        Err(e) => {
                            warn!(
                                error = format!("{:?}", &e).as_str(),
                                dataset = match enabled!(tracing::Level::DEBUG) {
                                    true => format!("{:?}", &dataset),
                                    false => String::from("[See data in debug mode]"),
                                }
                                .as_str(),
                                "Can't send the dataset through the connector"
                            );

                            for data in dataset {
                                let mut context = context_received.clone();
                                context.insert_step_result(
                                    self.name(),
                                    DataResult::Err((
                                        data.to_value(),
                                        io::Error::new(e.kind(), e.to_string()),
                                    )),
                                )?;

                                super::send(self as &dyn Step, &context).await?;
                            }
                        }
                    };
                    dataset = Vec::default();
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(context_received.to_value()?);
            dataset.push(context_received.input());

            if self.dataset_size <= dataset.len() && document.can_append() {
                match connector.send(&*document, &dataset).await {
                    Ok(_) => {
                        info!("Dataset sended with success into the connector");
                        for data in dataset {
                            let mut context = context_received.clone();
                            context.insert_step_result(self.name(), data)?;
                            super::send(self as &dyn Step, &context).await?;
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = format!("{:?}", &e).as_str(),
                            dataset = match enabled!(tracing::Level::DEBUG) {
                                true => format!("{:?}", &dataset),
                                false => String::from("[See data in debug mode]"),
                            }
                            .as_str(),
                            "Can't send the dataset through the connector"
                        );

                        for data in dataset {
                            let mut context = context_received.clone();
                            context.insert_step_result(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            )?;

                            super::send(self as &dyn Step, &context).await?;
                        }
                    }
                };

                dataset = Vec::default();
            }
        }

        if !dataset.is_empty() {
            info!(
                dataset_size = dataset.len(),
                "Last send data into the connector"
            );

            match connector.send(&*document, &dataset).await {
                Ok(_) => {
                    info!("Dataset sended with success into the connector");
                    for data in dataset {
                        let context = match &last_context_received {
                            Some(context_received) => {
                                let mut context = context_received.clone();
                                context.insert_step_result(self.name(), data)?;
                                context
                            }
                            None => Context::new(self.name(), data)?,
                        };

                        super::send(self as &dyn Step, &context).await?;
                    }
                }
                Err(e) => {
                    warn!(
                        error = format!("{:?}", &e).as_str(),
                        dataset = match enabled!(tracing::Level::DEBUG) {
                            true => format!("{:?}", &dataset),
                            false => String::from("[See data in debug mode]"),
                        }
                        .as_str(),
                        "Can't send the dataset through the connector"
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
                                )?;
                                context
                            }
                            None => Context::new(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            )?,
                        };

                        super::send(self as &dyn Step, &context).await?;
                    }
                }
            };
        }

        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
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
        let context =
            Context::new("before".to_string(), DataResult::Err((data, error))).unwrap();
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
        let context =
            Context::new("before".to_string(), DataResult::Ok(data.clone())).unwrap();

        let mut expected_context = context.clone();
        expected_context
            .insert_step_result("my_step".to_string(), DataResult::Ok(data.clone()))
            .unwrap();

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
