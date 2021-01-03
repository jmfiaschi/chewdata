use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use serde::{Deserialize, Serialize};
use std::{fmt, io::Result};
use multiqueue::{MPMCReceiver, MPMCSender};

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
    pub dataset_size: usize,
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
            dataset_size: 1000,
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
    fn exec_with_pipe(&self, pipe_outbound_option: Option<MPMCReceiver<DataResult>>, pipe_inbound_option: Option<MPMCSender<DataResult>>) -> Result<()> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let reader = self.clone();
        let mut document_type = reader.document_type.clone();
        let mut connector_type = reader.connector_type.clone();
        let document = document_type.document();
        let connector = connector_type.connector_mut();

        let metadata = document.metadata();
        connector.set_metadata(metadata);

        let mut current_dataset_size = 0;

        let pipe_outbound = match pipe_outbound_option {
            Some(pipe_outbound) => pipe_outbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No outbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(())
            }
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

            let json_value = data_result.to_json_value();

            {
                // If the path change, the writer flush and send the data in the buffer though the connector.
                let mut connector_tmp = connector_type.clone().connector_inner();
                connector_tmp.set_parameters(json_value.clone());
                let new_path = connector_tmp.path();
                let current_path = connector_type.connector().path();
                if current_path != new_path && !connector_type.connector().inner().is_empty() {
                    info!(slog_scope::logger(), "Document will change"; "step" => format!("{}", self), "current_path"=>current_path,"new_path"=>new_path);
                    match document_type
                        .document_mut()
                        .flush(connector_type.connector_mut())
                    {
                        Ok(_) => (),
                        Err(e) => error!(slog_scope::logger(), "Can't flush data. {}", e),
                    };
                }
            }

            debug!(slog_scope::logger(), "Write data result"; "step" => format!("{}", self), "data_result" => format!("{:?}", data_result));
            match document_type
                .document_mut()
                .write_data_result(connector_type.connector_mut(), data_result.clone()) {
                    Ok(_) => (),
                    Err(e) => {
                        let new_data_result = DataResult::Err((json_value.clone(), e));
                        error!(slog_scope::logger(),
                            "Can't write into the document. Yield data result";
                            "data" => format!("{}", &json_value),
                            "data_result" => format!("{:?}", new_data_result)
                        );
                        continue;
                    }
                };

            if self.dataset_size <= current_dataset_size {
                document_type.document_mut().flush(connector_type.connector_mut())?;

                current_dataset_size = 0;
            }

            if let Some(ref pipe_inbound) = pipe_inbound_option {
                pipe_inbound
                    .try_send(data_result)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            }

            current_dataset_size = current_dataset_size + 1;
        }

        if 0 < current_dataset_size {
            document_type.document_mut().flush(connector_type.connector_mut())?;
        }

        if let Some(ref pipe_inbound) = pipe_inbound_option {
            drop(pipe_inbound);
        }
        
        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
}
