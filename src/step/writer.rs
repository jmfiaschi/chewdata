use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use crate::StepContext;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
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
            name: uuid.to_simple().to_string(),
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
    #[instrument]
    async fn exec(
        &self
    ) -> io::Result<()> {
        info!("Start");

        let mut current_dataset_size = 0;


        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.document();
        let position = -(document.entry_point_path_end().len() as isize);

        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Use to init the connector during the loop
        let default_connector = connector.clone();

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(step_context_received) = receiver_stream.next().await {
            
            super::send(self as &dyn Step, &step_context_received.clone()).await?;

            if !step_context_received
                .data_result()
                .is_type(self.data_type.as_ref())
            {
                trace!("This step handle only this data type");
                continue;
            }

            {
                // If the path change and the inner connector not empty, the connector
                // flush and send the data to the remote document before to load a new document.
                if connector.is_resource_will_change(step_context_received.to_value()?)?
                    && !connector.inner().is_empty()
                {
                    document.close(&mut *connector).await?;
                    match connector.send(Some(position)).await {
                        Ok(_) => (),
                        Err(e) => {
                            warn!(
                                error = e.to_string().as_str(),
                                data = String::from_utf8_lossy(connector.inner())
                                    .to_string()
                                    .as_str(),
                                "Can't send the data through the connector"
                            )
                        }
                    };
                    current_dataset_size = 0;
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(step_context_received.to_value()?);

            document
                .write_data(
                    &mut *connector,
                    step_context_received.data_result().to_value(),
                )
                .await?;

            current_dataset_size += 1;

            if self.dataset_size <= current_dataset_size {
                document.close(&mut *connector).await?;

                match connector.send(Some(position)).await {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(
                            error = e.to_string().as_str(),
                            data = String::from_utf8_lossy(connector.inner())
                                .to_string()
                                .as_str(),
                            "Can't send the data through the connector"
                        )
                    }
                };

                current_dataset_size = 0;
            }
        }

        if 0 < current_dataset_size {
            info!(
                dataset_size = current_dataset_size,
                "Send data before to end the step"
            );

            document.close(&mut *connector).await?;

            match connector.send(Some(position)).await {
                Ok(_) => (),
                Err(e) => {
                    warn!(
                        error = e.to_string().as_str(),
                        data = String::from_utf8_lossy(connector.inner())
                            .to_string()
                            .as_str(),
                        "Can't send the data through the connector"
                    )
                }
            };
        }

        info!("End");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}
