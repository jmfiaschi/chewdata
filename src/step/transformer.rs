use super::{DataResult, Dataset};
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::UpdaterType;
use genawaiter::sync::GenBoxed;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io::Result};

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
fn referentials_hashmap(referentials: &[Reader]) -> Option<HashMap<String, Vec<Value>>> {
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

        let dataset_option = match referential.exec(None) {
            Ok(dataset_option) => dataset_option,
            Err(e) => {
                warn!(slog_scope::logger(), "Can't read the referentiel"; "error" => format!("{}", e), "referential" => format!("{}", referential));
                return None;
            }
        };

        let mut referential_dataset: Vec<Value> = Vec::new();
        if let Some(dataset) = dataset_option {
            for data_results in dataset {
                for data_result in data_results {
                    let record = match data_result {
                        DataResult::Ok(record) => record,
                        DataResult::Err(_) => continue,
                    };
                    referential_dataset.push(record);
                }
            }
        }
        referentials_hashmap.insert(alias, referential_dataset);
    }

    Some(referentials_hashmap)
}
/// This Step transform a dataset.
impl Step for Transformer {
    fn exec(&self, dataset_opt: Option<Dataset>) -> Result<Option<Dataset>> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let dataset = match dataset_opt {
            Some(dataset) => dataset,
            None => {
                info!(slog_scope::logger(), "No data to transform"; "step" => format!("{}", self));
                return Ok(None);
            }
        };

        let transformer = self.to_owned();
        let updater_type = self.updater_type.clone();
        let is_parallel = self.is_parallel;

        let dataset = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator"; "step" => format!("{}", transformer));
            let mut mapping = None;
            for data_results in dataset {
                mapping = match (
                    mapping,
                    &transformer.referentials,
                    transformer.can_refreshed_referentials,
                ) {
                    (None, Some(referentials), _) => {
                        info!(slog_scope::logger(), "Refresh the referentials"; "step" => format!("{}", transformer));
                        referentials_hashmap(referentials)
                    }
                    (_, Some(referentials), true) => {
                        info!(slog_scope::logger(), "Refresh the referentials"; "step" => format!("{}", transformer));
                        referentials_hashmap(referentials)
                    }
                    (Some(mapping), Some(_), false) => Some(mapping),
                    (_, None, _) => None,
                };

                info!(slog_scope::logger(), "Transform a new dataset"; "dataset_size" => data_results.len(), "step" => format!("{}", transformer));
                let new_data_results = match is_parallel {
                    true => data_results
                        .into_par_iter()
                        .filter_map(|data_result| {
                            transform_data_result(
                                &transformer,
                                &updater_type,
                                &mapping,
                                data_result,
                            )
                        })
                        .collect(),
                    false => data_results
                        .into_iter()
                        .filter_map(|data_result| {
                            transform_data_result(
                                &transformer,
                                &updater_type,
                                &mapping,
                                data_result,
                            )
                        })
                        .collect(),
                };

                co.yield_(new_data_results).await;
            }
            debug!(slog_scope::logger(), "End generator"; "step" => format!("{}", transformer));
        });

        Ok(Some(dataset))
    }
}

// Transform a data_result.
fn transform_data_result(
    transformer: &Transformer,
    updater_type: &UpdaterType,
    mapping: &Option<HashMap<String, Vec<Value>>>,
    data_result: DataResult,
) -> Option<DataResult> {
    let record = match data_result {
        DataResult::Ok(record) => record,
        DataResult::Err(_) => return None,
    };

    match updater_type
        .updater()
        .update(record.clone(), mapping.clone())
    {
        Ok(new_record) => {
            debug!(slog_scope::logger(), "Record transformation success"; "step" => format!("{}", transformer), "record" => format!("{}", new_record));

            if Value::Null == new_record {
                debug!(slog_scope::logger(), "Record skip because the value si null"; "step" => format!("{}", transformer), "record" => format!("{}", new_record));
                return None;
            }

            let new_data_result = DataResult::Ok(new_record);
            debug!(slog_scope::logger(), "Yield data result"; "step" => format!("{}", transformer), "data_result" => format!("{:?}", new_data_result));
            Some(new_data_result)
        }
        Err(e) => {
            let new_data_result = DataResult::Err((record, e));
            warn!(slog_scope::logger(), "Record transformation alert. Yield data result";"step" => format!("{}", transformer), "data_result" => format!("{:?}", new_data_result));
            Some(new_data_result)
        }
    }
}
