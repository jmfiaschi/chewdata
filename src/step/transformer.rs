use super::{DataResult};
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::{Action,  UpdaterType};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io};
use multiqueue::{MPMCReceiver, MPMCSender};
use std::{thread, time};
use async_trait::async_trait;
use slog::Drain;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    #[serde(rename = "updater")]
    #[serde(alias = "u")]
    pub updater_type: UpdaterType,
    #[serde(alias = "refs")]
    pub referentials: Option<HashMap<String, Reader>>,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    #[serde(alias = "wait")]
    pub wait_in_millisecond: usize,
    #[serde(alias = "threads")]
    pub thread_number: usize,
    // Use Vec in order to keep the order FIFO.
    pub actions: Vec<Action>,
    #[serde(alias = "input")]
    input_name: String,
    #[serde(alias = "output")]
    output_name: String,
}

impl Default for Transformer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Transformer {
            updater_type: UpdaterType::default(),
            referentials: None,
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            data_type: DataResult::OK.to_string(),
            wait_in_millisecond: 10,
            thread_number: 1,
            actions: Vec::default(),
            input_name: "input".to_string(),
            output_name: "output".to_string(),
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
async fn referentials_reader_to_dataset(referentials: HashMap<String, Reader>) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_dataset = HashMap::new();

    // For each reader, try to build the referential.
    for (alias, referential) in referentials {
        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        let mut referential_dataset: Vec<Value> = Vec::new();

        referential.exec(None, Some(pipe_inbound)).await?;

        for data_result in pipe_outbound {
            referential_dataset.push(data_result.to_json_value());
        }
        referentials_dataset.insert(alias, referential_dataset);
    }

    Ok(referentials_dataset)
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
            Some(referentials) => Some(referentials_reader_to_dataset(referentials).await?),
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
                .update(record.clone(), mapping.clone(), self.actions.clone(), self.input_name.clone(), self.output_name.clone()) {
                    Ok(new_record) => {
                        debug!(slog_scope::logger(), "Record transformation success"; "step" => format!("{}", self), "record" => format!("{}", new_record));

                        if Value::Null == new_record {
                            debug!(slog_scope::logger(), "Record skip because the value si null"; "step" => format!("{}", self), "record" => format!("{}", new_record));
                            continue;
                        }

                        let new_data_result = DataResult::Ok(new_record);
                        debug!(slog_scope::logger(), "New data result"; "step" => format!("{}", self), "data_result" => format!("{:?}", new_data_result));
                        new_data_result
                    }
                    Err(e) => {
                        let new_data_result = DataResult::Err((record, e));
                        warn!(slog_scope::logger(), "Record transformation error. New data result with error";"step" => format!("{}", self), "data_result" => format!("{:?}", new_data_result));
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
                warn!(slog_scope::logger(), "The pipe is full, wait before to retry"; "step" => format!("{}", self), "wait_in_millisecond"=>self.wait_in_millisecond, "current_retry" => current_retry);
                thread::sleep(time::Duration::from_millis(self.wait_in_millisecond as u64));
                current_retry += 1;
            }
        }

        drop(pipe_inbound);

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
}
