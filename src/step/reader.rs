use super::{DataResult};
use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use serde::Deserialize;
use std::{fmt, io};
use multiqueue::{MPMCReceiver, MPMCSender};

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
    fn exec_with_pipe(&self, pipe_outbound_option: Option<MPMCReceiver<DataResult>>, pipe_inbound_option: Option<MPMCSender<DataResult>>) -> io::Result<()> {
        debug!(slog_scope::logger(), "Exec"; "step" => format!("{}", self));

        let document_type = self.document_type.clone();
        let mut connector_type = self.connector_type.clone();
        let document = document_type.document();
        let connector = connector_type.connector_mut();
        connector.set_metadata(document.metadata());

        let pipe_inbound = match pipe_inbound_option {
            Some(pipe_inbound) => pipe_inbound,
            None => {
                info!(slog_scope::logger(), "This step is skipped. No inbound pipe found"; "step" => format!("{}", self.clone()));
                return Ok(())
            }
        };

        match pipe_outbound_option {
            Some(pipe_outbound) => {
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

                    let connector = connector_type.connector_mut();
                    connector.set_parameters(json_value);

                    let data = document_type
                        .document()
                        .read_data(connector_type.clone().connector_inner())?;
                    
                    for data_result in data {
                        pipe_inbound
                            .try_send(data_result)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
                    }
                }
            },
            None => {
                let data = document_type
                    .document()
                    .read_data(connector_type.clone().connector_inner())?;
                
                for data_result in data {
                    pipe_inbound
                        .try_send(data_result)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
                }
            }
        };

        drop(pipe_inbound);

        debug!(slog_scope::logger(), "Exec ended"; "step" => format!("{}", self));
        Ok(())
    }
}
