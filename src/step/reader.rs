use super::{DataResult, Dataset};
use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use genawaiter::sync::GenBoxed;
use serde::Deserialize;
use std::{fmt, io};

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Reader {
    #[serde(alias = "document")]
    document_type: DocumentType,
    #[serde(alias = "connector")]
    connector_type: ConnectorType,
    pub alias: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "batch_size")]
    pub dataset_size: usize,
    pub data_type: String,
}

impl Default for Reader {
    fn default() -> Self {
        Reader {
            document_type: DocumentType::default(),
            connector_type: ConnectorType::default(),
            alias: None,
            description: None,
            dataset_size: 1000,
            data_type: DataResult::OK.to_string(),
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Reader {{'{}','{}'}}",
            self.alias.to_owned().unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

impl Step for Reader {
    fn exec(&self, dataset_opt: Option<Dataset>) -> io::Result<Option<Dataset>> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let document_type = self.document_type.clone();
        let mut connector_type = self.connector_type.clone();
        let step_cloned = self.to_owned();
        let dataset_size = self.dataset_size;
        let data_type = self.data_type.to_owned();

        let dataset = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Generator start"; "step" => format!("{}", step_cloned));
            info!(
                slog_scope::logger(),
                "Read document inner through the connector";"step" => format!("{}", step_cloned)
            );
            let mut dataset: Vec<DataResult> = Vec::default();
            match dataset_opt {
                Some(input_dataset) => {
                    for data_results in input_dataset {
                        data_results.into_iter().for_each(|data_result| {
                            let json_value = match (data_result.clone(), data_type.as_ref()) {
                                (DataResult::Ok(_), DataResult::OK) => data_result.to_json_value(),
                                (DataResult::Err(_), DataResult::ERR) => data_result.to_json_value(),
                                _ => {
                                    info!(slog_scope::logger(),
                                        "This step handle only this data type";
                                        "data_type" => &data_type,
                                        "data" => format!("{:?}", data_result),
                                        "step" => format!("{}", &step_cloned)
                                    );
                                    return;
                                }
                            };

                            let connector = connector_type.connector_mut();
                            connector.set_parameters(json_value);
                            
                            let data = match document_type
                                .document()
                                .read_data(connector_type.clone().connector_inner())
                            {
                                Ok(data) => data,
                                Err(e) => {
                                    error!(slog_scope::logger(), "Can't read the document"; "error" => format!("{}",e));
                                    return;
                                }
                            };

                            for data_result in data {
                                debug!(slog_scope::logger(), "Add data into the dataset"; "step" => format!("{}", step_cloned), "data_result" => format!("{:?}", data_result));
                                dataset.push(data_result);
                            }
                        });
                        if dataset_size <= dataset.len() {
                            info!(
                                slog_scope::logger(),
                                "Read data from the document and yield a new dataset"; "dataset_size" => dataset.len(), "step" => format!("{}", step_cloned)
                            );
                            co.yield_(dataset).await;
                            dataset = Vec::default();
                        }
                    }
                }
                None => {
                    let data = match document_type
                        .document()
                        .read_data(connector_type.clone().connector_inner())
                    {
                        Ok(data) => data,
                        Err(e) => {
                            error!(slog_scope::logger(), "Can't read the document"; "error" => format!("{}",e));
                            return;
                        }
                    };
                    for data_result in data {
                        debug!(slog_scope::logger(), "Add data into the dataset"; "step" => format!("{}", step_cloned), "data_result" => format!("{:?}", data_result));
                        dataset.push(data_result);

                        if dataset_size <= dataset.len() {
                            info!(
                                slog_scope::logger(),
                                "Yield a new dataset"; "dataset_size" => dataset.len(), "step" => format!("{}", step_cloned)
                            );
                            co.yield_(dataset).await;
                            dataset = Vec::default();
                        }
                    }
                }
            }

            if !dataset.is_empty() {
                info!(
                    slog_scope::logger(),
                    "Yield readed new dataset"; "dataset_size" => dataset.len(), "step" => format!("{}", step_cloned)
                );
                co.yield_(dataset).await;
            }

            debug!(slog_scope::logger(), "Generator ended"; "step" => format!("{}", step_cloned));
        });

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(Some(dataset))
    }
}
