use crate::connector::ConnectorType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::Deserialize;
use std::{fmt, io};
use std::{thread, time};
use tracing::Instrument;
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
    async fn exec(
        &self,
        pipe_outbound_option: Option<MPMCReceiver<DataResult>>,
        pipe_inbound_option: Option<MPMCSender<DataResult>>,
    ) -> io::Result<()> {
        trace!(step = format!("{}", self).as_str(), "Exec");

        let connector_type = self.connector_type.clone();
        let mut connector = connector_type.connector();
        let mut exclude_paths = self.exclude_paths.clone();

        match (pipe_outbound_option, connector.is_variable()) {
            (Some(pipe_outbound), true) => {
                for data_result in pipe_outbound {
                    let json_value = data_result.to_json_value();
                    connector.set_parameters(json_value.clone());
                    let path = connector.path();

                    if !exclude_paths.contains(&path) {
                        trace!(
                            step = format!("{}", self.clone()).as_str(),
                            "Erase data started"
                        );

                        connector
                            .erase()
                            .instrument(tracing::info_span!("erase"))
                            .await?;

                        trace!(
                            step = format!("{}", self.clone()).as_str(),
                            "Erase data ended"
                        );

                        exclude_paths.push(path);
                    }

                    if let Some(ref pipe_inbound) = pipe_inbound_option {
                        trace!(
                            data = format!("{:?}", data_result).as_str(),
                            step = format!("{}", self.clone()).as_str(),
                            pipe_outbound = false,
                            "Data send to the queue"
                        );

                        let mut current_retry = 0;

                        while pipe_inbound.try_send(data_result.clone()).is_err() {
                            warn!(
                                step = format!("{}", self).as_str(),
                                wait_in_millisecond = self.wait_in_millisecond,
                                current_retry = current_retry,
                                "The pipe is full, wait before to retry"
                            );
                            thread::sleep(time::Duration::from_millis(
                                self.wait_in_millisecond as u64,
                            ));
                            current_retry += 1;
                        }
                    }
                }
            }
            (Some(pipe_outbound), false) => {
                for _data_result in pipe_outbound {}

                trace!(
                    step = format!("{}", self.clone()).as_str(),
                    "Erase data started"
                );

                connector
                    .erase()
                    .instrument(tracing::info_span!("erase"))
                    .await?;

                trace!(
                    step = format!("{}", self.clone()).as_str(),
                    "Erase data ended"
                );
            }
            (_, _) => {
                trace!(
                    step = format!("{}", self.clone()).as_str(),
                    "Erase data started"
                );

                connector
                    .erase()
                    .instrument(tracing::info_span!("erase"))
                    .await?;

                trace!(
                    step = format!("{}", self.clone()).as_str(),
                    "Erase data ended"
                );
            }
        };

        if let Some(pipe_inbound) = pipe_inbound_option {
            drop(pipe_inbound);
        }

        trace!(step = format!("{}", self).as_str(), "Exec ended");
        Ok(())
    }
}
