use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
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
    pub alias: Option<String>,
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    pub dataset_size: usize,
    #[serde(alias = "threads")]
    pub thread_number: usize,
}

impl Default for Writer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Writer {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            data_type: DataResult::OK.to_string(),
            dataset_size: 1000,
            thread_number: 1,
        }
    }
}

impl fmt::Display for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Writer {{'{}','{}'}}",
            self.alias
                .to_owned()
                .unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

// This Step write data from somewhere into another stream.
#[async_trait]
impl Step for Writer {
    #[instrument]
    async fn exec(
        &self,
        receiver_option: Option<Receiver<DataResult>>,
        sender_option: Option<Sender<DataResult>>,
    ) -> io::Result<()> {
        info!("Start");

        let mut current_dataset_size = 0;

        let receiver = match receiver_option {
            Some(receiver) => receiver,
            None => {
                info!("This step is skipped. Need a step before or a receiver");
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.document();
        let position = -(document.entry_point_path_end().len() as isize);

        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Use to init the connector during the loop
        let default_connector = connector.clone();

        for data_result in receiver {
            if let Some(ref sender) = sender_option {
                trace!("Send data to the queue");
                sender
                    .send(data_result.clone())
                    .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))?;
            }

            if !data_result.is_type(self.data_type.as_ref()) {
                trace!(
                    data_type = self.data_type.to_string().as_str(),
                    data = format!("{:?}", data_result).as_str(),
                    "This step handle only this data type"
                );
                continue;
            }

            {
                // If the path change, the writer flush and send the data in the buffer though the connector.
                if connector.is_resource_will_change(data_result.to_json_value())? {
                    document.close(&mut *connector).await?;
                    match connector.send(Some(position)).await {
                        Ok(_) => (),
                        Err(e) => {
                            warn!(
                                error = e.to_string().as_str(),
                                data = String::from_utf8_lossy(connector.inner())
                                    .to_string()
                                    .as_str(),
                                "Can't send the data througth the connector"
                            )
                        }
                    };
                    current_dataset_size = 0;
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(data_result.to_json_value());

            document
                .write_data(&mut *connector, data_result.to_json_value())
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
            info!("Send data before to end the step");

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

        if let Some(sender) = sender_option {
            drop(sender);
        }

        info!("End");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
}
