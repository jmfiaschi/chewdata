use super::{DataResult};
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::UpdaterType;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io};
use multiqueue::{MPMCReceiver, MPMCSender};
use std::{thread, time};
use async_trait::async_trait;
use slog::Drain;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    #[serde(alias = "updater")]
    #[serde(alias = "u")]
    updater_type: UpdaterType,
    #[serde(alias = "refs")]
    referentials: Option<Vec<Reader>>,
    // Option in order to keep the referentials up-to-date everytime.
    pub can_refreshed_referentials: bool,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    // transform in parallel mode. The data order write into the document is not respected.
    // By default, set to true in order to parallize the writting.
    pub is_parallel: bool,
    #[serde(alias = "wait")]
    pub wait_in_milisec: u64,
    pub thread_number: i32,
}

impl Default for Transformer {
    fn default() -> Self {
        Transformer {
            updater_type: UpdaterType::default(),
            referentials: None,
            alias: None,
            description: None,
            is_parallel: true,
            can_refreshed_referentials: true,
            data_type: DataResult::OK.to_string(),
            wait_in_milisec: 10,
            thread_number: 1
        }
    }
}

impl fmt::Display for Transformer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Transformer {{'{}','{}' }}",
            self.alias
                .to_owned()
                .unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}
/// Return a referentials hashmap indexed by the alias of the referential.
async fn referentials_hashmap(referentials: Vec<Reader>) -> Option<HashMap<String, Vec<Value>>> {
    let mut referentials_hashmap = HashMap::new();

    // For each reader, try to build the referential.
    for referential in referentials {
        let alias: String = match &referential.alias {
            Some(alias) => alias.to_string(),
            None => {
                warn!(slog_scope::logger(), "Alias required for this referential"; "referential" => format!("{}", referential));
                return None;
            }
        };

        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        match referential.exec(None, Some(pipe_inbound)).await {
            Ok(dataset_option) => dataset_option,
            Err(e) => {
                warn!(slog_scope::logger(), "Can't read the referentiel"; "error" => format!("{}", e), "referential" => format!("{}", referential));
                return None;
            }
        };

        let mut referential_dataset: Vec<Value> = Vec::new();
        for data_result in pipe_outbound {
            referential_dataset.push(data_result.to_json_value());
        }
        referentials_hashmap.insert(alias, referential_dataset);
    }

    Some(referentials_hashmap)
}
/// This Step transform a dataset.
#[async_trait]
impl Step for Transformer {
    async fn exec(&self, pipe_outbound_option: Option<MPMCReceiver<DataResult>>, pipe_inbound_option: Option<MPMCSender<DataResult>>) -> io::Result<()> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let pipe_inbound = match pipe_inbound_option {
            Some(pipe_inbound) => pipe_inbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No inbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(())
            }
        };

        let pipe_outbound = match pipe_outbound_option {
            Some(pipe_outbound) => pipe_outbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No outbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(())
            }
        };

        let mapping = match self.referentials.clone() {
            Some(referentials) => referentials_hashmap(referentials).await,
            None => None
        };

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

            let record = data_result.to_json_value();

            let new_data_results = match self.updater_type
                .updater()
                .update(record.clone(), mapping.clone()) {
                    Ok(new_record) => {
                        debug!(slog_scope::logger(), "Record transformation success"; "step" => format!("{}", self), "record" => format!("{}", new_record));

                        if Value::Null == new_record {
                            debug!(slog_scope::logger(), "Record skip because the value si null"; "step" => format!("{}", self), "record" => format!("{}", new_record));
                            continue;
                        }

                        let new_data_result = DataResult::Ok(new_record);
                        debug!(slog_scope::logger(), "Yield data result"; "step" => format!("{}", self), "data_result" => format!("{:?}", new_data_result));
                        new_data_result
                    }
                    Err(e) => {
                        let new_data_result = DataResult::Err((record, e));
                        warn!(slog_scope::logger(), "Record transformation alert. Yield data result";"step" => format!("{}", self), "data_result" => format!("{:?}", new_data_result));
                        new_data_result
                    }
                };
            
            info!(slog_scope::logger(),
                "Data send to the queue";
                "data" => match slog::Logger::is_debug_enabled(&slog_scope::logger()) {
                    true => format!("{:?}", new_data_results),
                    false => "truncated, available only in debug mode".to_string(),
                },
                "step" => format!("{}", self.clone())
            );
            let mut current_retry = 0;
            while pipe_inbound.try_send(new_data_results.clone()).is_err() {
                warn!(slog_scope::logger(), "The pipe is full, wait before to retry"; "step" => format!("{}", self), "wait_in_milisec"=>self.wait_in_milisec, "current_retry" => current_retry);
                thread::sleep(time::Duration::from_millis(self.wait_in_milisec));
                current_retry += 1;
            }
        }

        drop(pipe_inbound);

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
    fn thread_number(&self) -> i32 {
        self.thread_number
    }
}
