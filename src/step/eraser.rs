use crate::step::Step;
use crate::DataResult;
use crate::{connector::ConnectorType, StepContext};
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
    pub alias: String,
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "exclude")]
    pub exclude_paths: Vec<String>,
}

impl Default for Eraser {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Eraser {
            connector_type: ConnectorType::default(),
            alias: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            exclude_paths: Vec::default(),
        }
    }
}

impl fmt::Display for Eraser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Eraser {{'{}','{}'}}",
            self.alias,
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
        receiver_option: Option<Receiver<StepContext>>,
        sender_option: Option<Sender<StepContext>>,
    ) -> io::Result<()> {
        info!("Start");

        let connector_type = self.connector_type.clone();
        let mut connector = connector_type.connector();
        let mut exclude_paths = self.exclude_paths.clone();

        match (receiver_option, connector.is_variable()) {
            (Some(receiver), true) => {
                // Used to check if one data has been received.
                let mut has_data_been_received = false;

                for mut step_context_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }

                    if !step_context_received
                        .data_result()
                        .is_type(self.data_type.as_ref())
                    {
                        trace!("This step handle only this data type");
                        continue;
                    }

                    connector.set_parameters(step_context_received.to_value()?);
                    let path = connector.path();

                    if !exclude_paths.contains(&path) {
                        connector.erase().await?;

                        exclude_paths.push(path);
                    }

                    if let Some(ref sender) = sender_option {
                        step_context_received.insert_step_result(
                            self.alias(),
                            step_context_received.data_result(),
                        )?;

                        self.send(step_context_received, sender)?;
                    }
                }

                // No data has been received, clean the connector.
                if !has_data_been_received {
                    connector.erase().await?;
                }
            }
            (Some(receiver), false) => {
                // Used to check if one data has been received.
                let mut has_data_been_received = false;

                for step_result_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }
                    let path = connector.path();

                    if !step_result_received
                        .data_result()
                        .is_type(self.data_type.as_ref())
                    {
                        trace!("This step handle only this data type");
                        continue;
                    }

                    // erase when the step receive the first message
                    if !exclude_paths.contains(&path) {
                        connector.erase().await?;

                        exclude_paths.push(path);
                    }

                    if let Some(ref sender) = sender_option {
                        self.send(step_result_received, sender)?;
                    }
                }

                // No data has been received, clean the connector.
                if !has_data_been_received {
                    connector.erase().await?;
                }
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
    fn alias(&self) -> String {
        self.alias.clone()
    }
}
