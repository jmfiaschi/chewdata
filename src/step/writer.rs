use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use crate::StepContext;
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
    // Time in millisecond to wait before to fetch/send new data from/in the pipe.
    #[serde(alias = "sleep")]
    pub wait: u64,
    #[serde(skip)]
    pub receiver: Option<Receiver<StepContext>>,
    #[serde(skip)]
    pub sender: Option<Sender<StepContext>>,
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
            wait: 10,
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
    fn set_receiver(&mut self, receiver: Receiver<StepContext>) {
        self.receiver = Some(receiver);
    }
    /// See [`Step::receiver`] for more details.
    fn receiver(&self) -> Option<&Receiver<StepContext>> {
        self.receiver.as_ref()
    }
    /// See [`Step::set_sender`] for more details.
    fn set_sender(&mut self, sender: Sender<StepContext>) {
        self.sender = Some(sender);
    }
    /// See [`Step::sender`] for more details.
    fn sender(&self) -> Option<&Sender<StepContext>> {
        self.sender.as_ref()
    }
    /// See [`Step::sleep`] for more details.
    fn sleep(&self) -> u64 {
        self.wait
    }
    #[instrument(name = "writer::exec")]
    async fn exec(&self) -> io::Result<()> {
        let mut connector = self.connector_type.clone().boxed_inner();
        let document = self.document_type.clone().boxed_inner();
        let mut dataset = Vec::default();

        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Use to init the connector during the loop
        let default_connector = connector.clone();
        let mut last_step_context_received = None;

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(step_context_received) = receiver_stream.next().await {
            if !step_context_received
                .data_result()
                .is_type(self.data_type.as_ref())
            {
                trace!("This step handle only this data type");
                super::send(self as &dyn Step, &step_context_received.clone()).await?;
                continue;
            }
            last_step_context_received = Some(step_context_received.clone());

            {
                // If the path change and the inner connector not empty, the connector
                // flush and send the data to the remote document before to load a new document.
                if connector.is_resource_will_change(step_context_received.to_value()?)?
                    && !dataset.is_empty()
                {
                    match connector.send(&*document, &dataset).await {
                        Ok(_) => {
                            info!("Dataset sended with success into the connector");
                            for data in dataset {
                                let mut step_context = step_context_received.clone();
                                step_context.insert_step_result(self.name(), data)?;

                                super::send(self as &dyn Step, &step_context).await?;
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
                                let mut step_context = step_context_received.clone();
                                step_context.insert_step_result(
                                    self.name(),
                                    DataResult::Err((
                                        data.to_value(),
                                        io::Error::new(e.kind(), e.to_string()),
                                    )),
                                )?;

                                super::send(self as &dyn Step, &step_context).await?;
                            }
                        }
                    };
                    dataset = Vec::default();
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(step_context_received.to_value()?);
            dataset.push(step_context_received.data_result());

            if self.dataset_size <= dataset.len() && document.can_append() {
                match connector.send(&*document, &dataset).await {
                    Ok(_) => {
                        info!("Dataset sended with success into the connector");
                        for data in dataset {
                            let mut step_context = step_context_received.clone();
                            step_context.insert_step_result(self.name(), data)?;
                            super::send(self as &dyn Step, &step_context).await?;
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
                            let mut step_context = step_context_received.clone();
                            step_context.insert_step_result(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            )?;

                            super::send(self as &dyn Step, &step_context).await?;
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
                        let step_context = match &last_step_context_received {
                            Some(step_context_received) => {
                                let mut step_context = step_context_received.clone();
                                step_context.insert_step_result(self.name(), data)?;
                                step_context
                            }
                            None => StepContext::new(self.name(), data)?,
                        };

                        super::send(self as &dyn Step, &step_context).await?;
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
                        let step_context = match &last_step_context_received {
                            Some(step_context_received) => {
                                let mut step_context = step_context_received.clone();
                                step_context.insert_step_result(
                                    self.name(),
                                    DataResult::Err((
                                        data.to_value(),
                                        io::Error::new(e.kind(), e.to_string()),
                                    )),
                                )?;
                                step_context
                            }
                            None => StepContext::new(
                                self.name(),
                                DataResult::Err((
                                    data.to_value(),
                                    io::Error::new(e.kind(), e.to_string()),
                                )),
                            )?,
                        };

                        super::send(self as &dyn Step, &step_context).await?;
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
        let step_context =
            StepContext::new("before".to_string(), DataResult::Err((data, error))).unwrap();
        let expected_step_context = step_context.clone();

        thread::spawn(move || {
            sender_input.try_send(step_context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.exec().await.unwrap();

        assert_eq!(expected_step_context, receiver_output.recv().await.unwrap());
    }
    #[async_std::test]
    async fn exec_with_same_data_result_type() {
        let mut step = Writer::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let step_context =
            StepContext::new("before".to_string(), DataResult::Ok(data.clone())).unwrap();

        let mut expected_step_context = step_context.clone();
        expected_step_context
            .insert_step_result("my_step".to_string(), DataResult::Ok(data.clone()))
            .unwrap();

        thread::spawn(move || {
            sender_input.try_send(step_context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.connector_type = ConnectorType::InMemory(InMemory::default());
        step.exec().await.unwrap();

        assert_eq!(expected_step_context, receiver_output.recv().await.unwrap());
    }
}
