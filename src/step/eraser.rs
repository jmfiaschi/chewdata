use crate::connector::ConnectorType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use serde::Deserialize;
use std::{fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Eraser {
    #[serde(rename = "connector")]
    #[serde(alias = "conn")]
    connector_type: ConnectorType,
    pub alias: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "wait")]
    pub wait_in_millisecond: usize,
    #[serde(alias = "exclude")]
    pub exclude_paths: Vec<String>,
}

impl Default for Eraser {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Eraser {
            connector_type: ConnectorType::default(),
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            wait_in_millisecond: 10,
            exclude_paths: Vec::default(),
        }
    }
}

impl fmt::Display for Eraser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Eraser {{'{}','{}'}}",
            self.alias
                .to_owned()
                .unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

#[async_trait]
impl Step for Eraser {
    #[instrument]
    async fn exec(
        &self,
        receiver_option: Option<Receiver<DataResult>>,
        sender_option: Option<Sender<DataResult>>,
    ) -> io::Result<()> {
        info!("Start");

        let connector_type = self.connector_type.clone();
        let mut connector = connector_type.connector();
        let mut exclude_paths = self.exclude_paths.clone();

        match (receiver_option, connector.is_variable()) {
            (Some(receiver), true) => {
                for data_result in receiver {
                    let json_value = data_result.to_json_value();
                    connector.set_parameters(json_value.clone());
                    let path = connector.path();

                    if !exclude_paths.contains(&path) {
                        connector.erase().await?;

                        exclude_paths.push(path);
                    }

                    if let Some(ref sender) = sender_option {
                        trace!("Send data to the queue");
                        sender
                            .send(data_result.clone())
                            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))?;
                    }
                }
            }
            (Some(receiver), false) => {
                for _data_result in receiver {}

                connector.erase().await?;
            }
            (_, _) => {
                connector.erase().await?;
            }
        };

        if let Some(sender) = sender_option {
            drop(sender);
        }

        info!("End");
        Ok(())
    }
}
