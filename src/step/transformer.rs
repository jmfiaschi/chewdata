use super::{DataResult};
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::UpdaterType;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io::Result};
use multiqueue::{MPMCReceiver, MPMCSender};

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    #[serde(alias = "updater")]
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
fn referentials_hashmap(referentials: Vec<Reader>) -> Option<HashMap<String, Vec<Value>>> {
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

        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(10);

        match referential.exec_with_pipe(None, Some(pipe_inbound)) {
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
impl Step for Transformer {
    fn exec_with_pipe(&self, pipe_outbound_option: Option<MPMCReceiver<DataResult>>, pipe_inbound_option: Option<MPMCSender<DataResult>>) -> Result<()> {
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
            Some(referentials) => referentials_hashmap(referentials),
            None => None
        };

        for data_result in pipe_outbound {
            if !data_result.is_type(self.data_type.as_ref()) {
                info!(slog_scope::logger(),
                    "This step handle only this data type";
                    "data_type" => self.data_type.to_string(),
                    "data" => format!("{:?}", data_result),
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
            
            pipe_inbound
                .try_send(new_data_results)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
        }

        drop(pipe_inbound);

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
}
