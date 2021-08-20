use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::Deserialize;
use std::{fmt, io};
use std::{thread, time};
use slog::Drain;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Eraser {
    #[serde(alias = "connector")]
    connector_type: ConnectorType,
    #[serde(alias = "document")]
    document_type: DocumentType,
    pub alias: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "wait")]
    pub wait_in_milisec: u64,
    #[serde(alias = "exclude")]
    pub exclude_paths: Vec<String>,
}

impl Default for Eraser {
    fn default() -> Self {
        Eraser {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            alias: None,
            description: None,
            wait_in_milisec: 10,
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
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

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
                        debug!(slog_scope::logger(), "Erase data started"; "step" => format!("{}", self.clone()));
                        connector.erase().await?;
                        debug!(slog_scope::logger(), "Erase data ended"; "step" => format!("{}", self.clone()));
                        exclude_paths.push(path);
                    }

                    if let Some(ref pipe_inbound) = pipe_inbound_option {
                        info!(slog_scope::logger(),
                            "Data send to the queue";
                            "data" => match slog::Logger::is_debug_enabled(&slog_scope::logger()) {
                                true => format!("{:?}", data_result),
                                false => "truncated, available only in debug mode".to_string(),
                            },
                            "step" => format!("{}", self.clone()),
                            "pipe_outbound" => false
                        );
                        let mut current_retry = 0;
                        while pipe_inbound.try_send(data_result.clone()).is_err() {
                            warn!(slog_scope::logger(), "The pipe is full, wait before to retry"; "step" => format!("{}", self), "wait_in_milisec"=>self.wait_in_milisec, "current_retry" => current_retry);
                            thread::sleep(time::Duration::from_millis(self.wait_in_milisec));
                            current_retry += 1;
                        }
                    }
                }
            }
            (Some(pipe_outbound), false) => {
                for _data_result in pipe_outbound {}
                debug!(slog_scope::logger(), "Erase data started"; "step" => format!("{}", self.clone()));
                connector.erase().await?;
                debug!(slog_scope::logger(), "Erase data ended"; "step" => format!("{}", self.clone()));
            }
            (_, _) => {
                debug!(slog_scope::logger(), "Erase data started"; "step" => format!("{}", self.clone()));
                connector.erase().await?;
                debug!(slog_scope::logger(), "Erase data ended"; "step" => format!("{}", self.clone()));
            }
        };

        if let Some(pipe_inbound) = pipe_inbound_option {
            drop(pipe_inbound);
        }

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
}
