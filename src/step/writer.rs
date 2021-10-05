use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::{DataResult, Step};
use async_trait::async_trait;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::{Deserialize, Serialize};
use std::{fmt, io};
use std::{thread, time};
use tracing::Instrument;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Writer {
    #[serde(rename = "connector")]
    #[serde(alias = "conn")]
    connector_type: ConnectorType,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    document_type: DocumentType,
    pub alias: Option<String>,
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    pub dataset_size: usize,
    #[serde(alias = "wait")]
    pub wait_in_millisecond: usize,
    #[serde(alias = "threads")]
    pub thread_number: usize,
}

impl Default for Writer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Writer {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            data_type: DataResult::OK.to_string(),
            dataset_size: 1000,
            wait_in_millisecond: 10,
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
        trace!(step = format!("{}", self).as_str(), "Exec");

        let mut current_dataset_size = 0;

        let pipe_outbound = match pipe_outbound_option {
            Some(pipe_outbound) => pipe_outbound,
            None => {
                info!(
                    step = format!("{}", self.clone()).as_str(),
                    "This step is skipped. No outbound pipe found"
                );
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.document();
        let position = -(document.entry_point_path_end().len() as isize);

        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Use to init the connector during the loop
        let default_connector = connector.clone();

        for data_result in pipe_outbound {
            if let Some(ref pipe_inbound) = pipe_inbound_option {
                trace!(
                    data = format!("{:?}", data_result).as_str(),
                    step = format!("{}", self.clone()).as_str(),
                    "Data send to the queue"
                );

                let mut current_retry = 0;
                
                while pipe_inbound.try_send(data_result.clone()).is_err() {
                    warn!(
                        step = format!("{}", self).as_str(),
                        "wait_in_millisecond" = self.wait_in_millisecond,
                        current_retry = current_retry,
                        "The pipe is full, wait before to retry"
                    );
                    thread::sleep(time::Duration::from_millis(self.wait_in_millisecond as u64));
                    current_retry += 1;
                }
            }

            if !data_result.is_type(self.data_type.as_ref()) {
                trace!(
                    data_type = self.data_type.to_string().as_str(),
                    data = format!("{:?}", data_result).as_str(),
                    step = format!("{}", self.clone()).as_str(),
                    "This step handle only this data type"
                );
                continue;
            }

            {
                // If the path change, the writer flush and send the data in the buffer though the connector.
                if connector.is_resource_will_change(data_result.to_json_value())? {
                    document
                        .close(&mut *connector)
                        .instrument(tracing::info_span!("close"))
                        .await?;
                    match connector
                        .send(Some(position))
                        .instrument(tracing::info_span!("send"))
                        .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            warn!(
                                error = e.to_string().as_str(),
                                step = format!("{}", self.clone()).as_str(),
                                data = String::from_utf8_lossy(connector.inner())
                                    .to_string()
                                    .as_str(),
                                "Can't send the data througth the connector"
                            )
                        }
                    };
                    current_dataset_size = 0;
                    connector = default_connector.clone();
                }
            }

            connector.set_parameters(data_result.to_json_value());

            trace!(
                connector = format!("{:?}", &connector).as_str(),
                document = format!("{:?}", &document).as_str(),
                data = format!("{:?}", data_result).as_str(),
                step = format!("{}", self.clone()).as_str(),
                "Push data"
            );

            document
                .write_data(&mut *connector, data_result.to_json_value())
                .instrument(tracing::info_span!("write_data"))
                .await?;

            current_dataset_size += 1;

            if self.dataset_size <= current_dataset_size {
                info!(step = format!("{}", self.clone()).as_str(), "Send data");

                document
                    .close(&mut *connector)
                    .instrument(tracing::info_span!("close"))
                    .await?;

                match connector
                    .send(Some(position))
                    .instrument(tracing::info_span!("send"))
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(
                            error = e.to_string().as_str(),
                            step = format!("{}", self.clone()).as_str(),
                            data = String::from_utf8_lossy(connector.inner())
                                .to_string()
                                .as_str(),
                            "Can't send the data through the connector"
                        )
                    }
                };

                current_dataset_size = 0;
            }
        }

        if 0 < current_dataset_size {
            info!(
                step = format!("{}", self.clone()).as_str(),
                "Send data before to end the step"
            );

            document.close(&mut *connector).await?;

            match connector
                .send(Some(position))
                .instrument(tracing::info_span!("send"))
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    warn!(
                        error = e.to_string().as_str(),
                        step = format!("{}", self.clone()).as_str(),
                        data = String::from_utf8_lossy(connector.inner())
                            .to_string()
                            .as_str(),
                        "Can't send the data through the connector"
                    )
                }
            };
        }

        if let Some(pipe_inbound) = pipe_inbound_option {
            drop(pipe_inbound);
        }

        trace!(step = format!("{}", self).as_str(), "Exec ended");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
}
