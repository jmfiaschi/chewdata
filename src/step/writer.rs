use crate::connector::ConnectorType;
use crate::step::{DataResult, Step};
use async_trait::async_trait;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::{Deserialize, Serialize};
use std::{fmt, io};
use std::{thread, time};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Writer {
    #[serde(alias = "connector")]
    connector_type: ConnectorType,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    // Write in parallel mode. The data order write into the document is not respected.
    // By default, set to true in order to parallize the writting.
    pub is_parallel: bool,
    pub dataset_size: usize,
    #[serde(alias = "wait")]
    pub wait_in_milisec: u64,
    pub thread_number: i32,
}

impl Default for Writer {
    fn default() -> Self {
        Writer {
            connector_type: ConnectorType::default(),
            alias: None,
            description: None,
            data_type: DataResult::OK.to_string(),
            is_parallel: true,
            dataset_size: 1000,
            wait_in_milisec: 10,
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
    async fn exec(
        &self,
        pipe_outbound_option: Option<MPMCReceiver<DataResult>>,
        pipe_inbound_option: Option<MPMCSender<DataResult>>,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let mut current_dataset_size = 0;

        let pipe_outbound = match pipe_outbound_option {
            Some(pipe_outbound) => pipe_outbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No outbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector_inner();

        for data_result in pipe_outbound {
            if let Some(ref pipe_inbound) = pipe_inbound_option {
                info!(slog_scope::logger(),
                    "Data send to the queue";
                    "data" => format!("{:?}", data_result),
                    "step" => format!("{}", self.clone())
                );
                let mut current_retry = 0;
                while let Err(_) = pipe_inbound.try_send(data_result.clone()) {
                    warn!(slog_scope::logger(), "The pipe is full, wait before to retry"; "step" => format!("{}", self), "wait_in_milisec"=>self.wait_in_milisec, "current_retry" => current_retry);
                    thread::sleep(time::Duration::from_millis(self.wait_in_milisec));
                    current_retry = current_retry + 1;
                }
            }

            if !data_result.is_type(self.data_type.as_ref()) {
                info!(slog_scope::logger(),
                    "This step handle only this data type";
                    "data_type" => self.data_type.to_string(),
                    "data" => format!("{:?}", data_result),
                    "step" => format!("{}", self.clone())
                );
                continue;
            }

            {
                // If the path change, the writer flush and send the data in the buffer though the connector.
                if connector.is_resource_will_change(data_result.to_json_value())? {
                    match connector.send().await {
                        Ok(_) => (),
                        Err(e) => {
                            warn!(slog_scope::logger(), "Can't send the data througth the connector"; "error" => e.to_string(), "step" => format!("{}", self.clone()), "data" => String::from_utf8_lossy(connector.inner()).to_string())
                        }
                    };
                    current_dataset_size = 0;
                    connector = self.connector_type.clone().connector_inner();
                }
            }

            connector.set_parameters(data_result.to_json_value());
            info!(slog_scope::logger(),
                "Push data";
                "connector" => format!("{:?}", &connector),
                "data" => format!("{:?}", data_result),
                "step" => format!("{}", self.clone()),
            );
            connector.push_data(data_result).await?;

            if self.dataset_size <= current_dataset_size {
                info!(slog_scope::logger(),
                    "Send data";
                    "step" => format!("{}", self.clone()),
                );
                match connector.send().await {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't send the data through the connector"; "error" => e.to_string(), "step" => format!("{}", self.clone()), "data" => String::from_utf8_lossy(connector.inner()).to_string())
                    }
                };
                current_dataset_size = 0;
            } else {
                current_dataset_size = current_dataset_size + 1;
            }
        }

        if 0 < current_dataset_size {
            info!(slog_scope::logger(),
                "Send data before to end the step";
                "step" => format!("{}", self.clone()),
            );
            match connector.send().await {
                Ok(_) => (),
                Err(e) => {
                    warn!(slog_scope::logger(), "Can't send the data through the connector"; "error" => e.to_string(), "step" => format!("{}", self.clone()), "data" => String::from_utf8_lossy(connector.inner()).to_string())
                }
            };
        }

        if let Some(ref pipe_inbound) = pipe_inbound_option {
            drop(pipe_inbound);
        }

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
    fn thread_number(&self) -> i32 {
        self.thread_number
    }
}
