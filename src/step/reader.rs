use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use futures::StreamExt;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::Deserialize;
use slog::Drain;
use std::{fmt, io};
use std::{thread, time};

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Reader {
    #[serde(alias = "connector")]
    connector_type: ConnectorType,
    #[serde(alias = "document")]
    document_type: DocumentType,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    #[serde(alias = "wait")]
    pub wait_in_milisec: u64,
}

impl Default for Reader {
    fn default() -> Self {
        Reader {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            alias: None,
            description: None,
            data_type: DataResult::OK.to_string(),
            wait_in_milisec: 10,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Reader {{'{}','{}'}}",
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
impl Step for Reader {
    async fn exec(
        &self,
        pipe_outbound_option: Option<MPMCReceiver<DataResult>>,
        pipe_inbound_option: Option<MPMCSender<DataResult>>,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let pipe_inbound = match pipe_inbound_option {
            Some(pipe_inbound) => pipe_inbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No inbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.clone().document_inner();
        connector.set_metadata(connector.metadata().merge(document.metadata()));

        match (pipe_outbound_option, connector.is_variable()) {
            (Some(pipe_outbound), true) => {
                for data_result in pipe_outbound {
                    if !data_result.is_type(self.data_type.as_ref()) {
                        info!(slog_scope::logger(),
                            "This step handle only this data type";
                            "data_type" => self.data_type.to_string(),
                            "data" => match slog::Logger::is_debug_enabled(&slog_scope::logger()) {
                                true => format!("{:?}", data_result),
                                false => "truncated, available only in debug mode".to_string(),
                            },
                            "step" => format!("{}", self.clone())
                        );
                        continue;
                    }

                    connector.set_parameters(data_result.to_json_value());
                    let mut data = connector.pull_data(document.clone()).await?;
                    while let Some(data_result) = data.next().await {
                        info!(slog_scope::logger(),
                            "Data send to the queue";
                            "data" => match slog::Logger::is_debug_enabled(&slog_scope::logger()) {
                                true => format!("{:?}", data_result),
                                false => "truncated, available only in debug mode".to_string(),
                            },
                            "step" => format!("{}", self.clone()),
                            "pipe_outbound" => true
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
                let mut data = connector.pull_data(document.clone()).await?;
                while let Some(data_result) = data.next().await {
                    info!(slog_scope::logger(),
                        "Data send to the queue";
                        "data" => match slog::Logger::is_debug_enabled(&slog_scope::logger()) {
                            true => format!("{:?}", data_result),
                            false => "truncated, available only in debug mode".to_string(),
                        },
                        "step" => format!("{}", self.clone()),
                        "pipe_outbound" => true
                    );
                    let mut current_retry = 0;
                    while pipe_inbound.try_send(data_result.clone()).is_err() {
                        warn!(slog_scope::logger(), "The pipe is full, wait before to retry"; "step" => format!("{}", self), "wait_in_milisec"=>self.wait_in_milisec, "current_retry" => current_retry);
                        thread::sleep(time::Duration::from_millis(self.wait_in_milisec));
                        current_retry += 1;
                    }
                }
            }
            (None, _) => {
                let mut data = connector.pull_data(document.clone()).await?;
                while let Some(data_result) = data.next().await {
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
        };

        drop(pipe_inbound);

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
}
