use super::Dataset;
use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use genawaiter::sync::GenBoxed;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::{fmt, io::Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Writer {
    #[serde(alias = "document")]
    document_type: DocumentType,
    #[serde(alias = "connector")]
    connector_type: ConnectorType,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    // Write in parallel mode. The data order write into the document is not respected.
    // By default, set to true in order to parallize the writting.
    pub is_parallel: bool,
}

impl Default for Writer {
    fn default() -> Self {
        Writer {
            document_type: DocumentType::default(),
            connector_type: ConnectorType::default(),
            alias: None,
            description: None,
            data_type: DataResult::OK.to_string(),
            is_parallel: true,
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
impl Step for Writer {
    fn exec(&self, dataset_opt: Option<Dataset>) -> Result<Option<Dataset>> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let dataset = match dataset_opt {
            Some(data) => data,
            None => {
                info!(slog_scope::logger(), "No data to write"; "step" => format!("{}", self));
                return Ok(None);
            }
        };

        let writer = self.clone();
        let document_type = self.document_type.clone();
        let connector_type = self.connector_type.clone();
        let document_type_arc = Arc::new(Mutex::new(document_type));
        let connector_type_arc = Arc::new(Mutex::new(connector_type));
        let data_type = self.data_type.to_owned();
        let is_parallel = self.is_parallel;

        let dataset = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator"; "step" => format!("{}", &writer));
            for data_results in dataset {
                info!(slog_scope::logger(), "Write a new dataset"; "dataset_size" => data_results.len(), "step" => format!("{}", writer));
                let data_results_clone = data_results.clone();

                match is_parallel {
                    true => write_with_parallelism(
                        &writer,
                        &document_type_arc,
                        &connector_type_arc,
                        data_results,
                        &data_type,
                    ),
                    false => write_without_parallelism(
                        &writer,
                        &document_type_arc,
                        &connector_type_arc,
                        data_results,
                        &data_type,
                    ),
                };

                let data_results = match document_type_arc.lock() {
                    Ok(ref mut document_type) => {
                        let mut connector_type = match connector_type_arc.lock() {
                            Ok(mutext) => mutext,
                            Err(e) => {
                                warn!(slog_scope::logger(),"Impossible to unlock the connector into the thread"; "error" => e.to_string(), "step" => format!("{}", &writer));
                                return;
                            }
                        };

                        match document_type
                            .document_mut()
                            .flush(connector_type.connector_mut())
                        {
                            Ok(_) => (),
                            Err(e) => {
                                warn!(slog_scope::logger(), "Can't flush data"; "error" => format!("{}",e))
                            }
                        };

                        match connector_type.connector().inner().is_empty() {
                            true => data_results_clone,
                            false => {
                                let mut new_data_results: Vec<DataResult> = Vec::default();
                                let data_results = match document_type
                                    .document()
                                    .read_data(connector_type.clone().connector_inner())
                                {
                                    Ok(data_results) => data_results,
                                    Err(e) => {
                                        warn!(slog_scope::logger(), "Can't read the document"; "error" => format!("{}",e));
                                        GenBoxed::new_boxed(|_| async move {})
                                    }
                                };
                                for data_result in data_results {
                                    new_data_results.push(data_result.clone());
                                }
                                new_data_results
                            }
                        }
                    }
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't acquire the mutex"; "error" => format!("{}",e));
                        Vec::default()
                    }
                };

                co.yield_(data_results).await;
            }

            debug!(slog_scope::logger(), "End generator"; "step" => format!("{}", writer));
        });

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(Some(dataset))
    }
}
// Write data_results with parallel mode.
pub fn write_with_parallelism(
    writer: &Writer,
    document_type_arc: &Arc<Mutex<DocumentType>>,
    connector_type_arc: &Arc<Mutex<ConnectorType>>,
    data_results: Vec<DataResult>,
    data_type: &str,
) {
    data_results.into_par_iter().for_each(|data_result| {
        write_data_result(
            &writer,
            &document_type_arc,
            &connector_type_arc,
            data_result,
            data_type,
        )
    });
}
// Write data_results without parallel mode.
pub fn write_without_parallelism(
    writer: &Writer,
    document_type_arc: &Arc<Mutex<DocumentType>>,
    connector_type_arc: &Arc<Mutex<ConnectorType>>,
    data_results: Vec<DataResult>,
    data_type: &str,
) {
    data_results.into_iter().for_each(|data_result| {
        write_data_result(
            &writer,
            &document_type_arc,
            &connector_type_arc,
            data_result,
            data_type,
        )
    });
}

// Write a data_result type into the document though the connector.
fn write_data_result(
    writer: &Writer,
    document_type_arc: &Arc<Mutex<DocumentType>>,
    connector_type_arc: &Arc<Mutex<ConnectorType>>,
    data_result: DataResult,
    data_type: &str,
) {
    let json_value = match (data_result.clone(), data_type) {
        (DataResult::Ok(_), DataResult::OK) => data_result.to_json_value(),
        (DataResult::Err(_), DataResult::ERR) => data_result.to_json_value(),
        _ => {
            info!(slog_scope::logger(),
                "This step handle only this data type";
                "data_type" => data_type,
                "data" => format!("{:?}", data_result),
                "step" => format!("{}", &writer)
            );
            return;
        }
    };

    let mut document_type = match document_type_arc.lock() {
        Ok(mutext) => mutext,
        Err(e) => {
            warn!(slog_scope::logger(),"Impossible to unlock the document into the thread"; "error" => e.to_string(), "step" => format!("{}", &writer));
            return;
        }
    };
    let mut connector_type = match connector_type_arc.lock() {
        Ok(mutext) => mutext,
        Err(e) => {
            warn!(slog_scope::logger(),"Impossible to unlock the connector into the thread"; "error" => e.to_string(), "step" => format!("{}", &writer));
            return;
        }
    };

    {
        let mut connector_tmp = connector_type.clone().connector_inner();
        connector_tmp.set_parameters(json_value.clone());
        let new_path = connector_tmp.path();
        let current_path = connector_type.connector().path();
        if current_path != new_path && !connector_type.connector().inner().is_empty() {
            info!(slog_scope::logger(), "Document will change"; "step" => format!("{}", writer), "current_path"=>current_path,"new_path"=>new_path);
            match document_type
                .document_mut()
                .flush(connector_type.connector_mut())
            {
                Ok(_) => (),
                Err(e) => error!(slog_scope::logger(), "Can't flush data. {}", e),
            };
        }
    }

    debug!(slog_scope::logger(), "Write data result"; "step" => format!("{}", writer), "data_result" => format!("{:?}", data_result));
    match document_type
        .document_mut()
        .write_data_result(connector_type.connector_mut(), data_result.clone())
    {
        Ok(_) => (),
        Err(e) => {
            let new_data_result = DataResult::Err((json_value.clone(), e));
            error!(slog_scope::logger(),
                "Can't write into the document. Yield data result";
                "data" => format!("{}", &json_value),
                "data_result" => format!("{:?}", new_data_result)
            );
            return;
        }
    };

    debug!(slog_scope::logger(), "Yield data result"; "step" => format!("{}", writer), "data_result" => format!("{:?}", data_result));
}
