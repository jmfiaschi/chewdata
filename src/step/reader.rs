use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use futures::StreamExt;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::Deserialize;
use std::{fmt, io};
use std::{thread, time};
use tracing::Instrument;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Reader {
    #[serde(rename = "connector")]
    #[serde(alias = "conn")]
    pub connector_type: ConnectorType,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    pub document_type: DocumentType,
    pub alias: Option<String>,
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "wait")]
    pub wait_in_millisecond: usize,
}

impl Default for Reader {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Reader {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            data_type: DataResult::OK.to_string(),
            wait_in_millisecond: 10,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Reader {{'{}','{}'}}",
            self.alias
                .to_owned()
                .unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

#[async_trait]
impl Step for Reader {
    async fn exec(
        &self,
        pipe_outbound_option: Option<MPMCReceiver<DataResult>>,
        pipe_inbound_option: Option<MPMCSender<DataResult>>,
    ) -> io::Result<()> {
        trace!(step = format!("{}", self).as_str(), "Exec");

        let pipe_inbound = match pipe_inbound_option {
            Some(pipe_inbound) => pipe_inbound,
            None => {
                info!(
                    step = format!("{}", self.clone()).as_str(),
                    "This step is skipped. No inbound pipe found"
                );
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.clone().document_inner();
        connector.set_metadata(connector.metadata().merge(document.metadata()));

        match (pipe_outbound_option, connector.is_variable()) {
            (Some(pipe_outbound), true) => {
                // Used to check if the data has been received.
                let mut has_data_been_received = false;
                for data_result in pipe_outbound {
                    if !has_data_been_received {
                        has_data_been_received = true;
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

                    connector.set_parameters(data_result.to_json_value());
                    let mut data = connector
                        .pull_data(document.clone())
                        .instrument(tracing::info_span!("pull_data"))
                        .await?;

                    while let Some(data_result) = data.next().await {
                        self.send(data_result, &pipe_inbound)?;
                    }
                }
                // If data has not been received and the channel has been close, run last time the step.
                // It arrive when the previous step don't push data through the pipe.
                if !has_data_been_received {
                    let mut data = connector
                        .pull_data(document.clone())
                        .instrument(tracing::info_span!("pull_data"))
                        .await?;

                    while let Some(data_result) = data.next().await {
                        self.send(data_result, &pipe_inbound)?;
                    }
                }
            }
            (Some(pipe_outbound), false) => {
                for _data_result in pipe_outbound {}
                let mut data = connector
                    .pull_data(document.clone())
                    .instrument(tracing::info_span!("pull_data"))
                    .await?;

                while let Some(data_result) = data.next().await {
                    self.send(data_result, &pipe_inbound)?;
                }
            }
            (None, _) => {
                let mut data = connector
                    .pull_data(document.clone())
                    .instrument(tracing::info_span!("pull_data"))
                    .await?;

                while let Some(data_result) = data.next().await {
                    self.send(data_result, &pipe_inbound)?;
                }
            }
        };

        drop(pipe_inbound);

        trace!(step = format!("{}", self).as_str(), "Exec ended");
        Ok(())
    }
}

impl Reader {
    fn send(&self, data_result: DataResult, pipe: &MPMCSender<DataResult>) -> io::Result<()> {
        trace!(
            data = format!("{:?}", data_result).as_str(),
            step = format!("{}", self.clone()).as_str(),
            pipe_outbound = false,
            "Data send to the queue"
        );
        let mut current_retry = 0;
        while pipe.try_send(data_result.clone()).is_err() {
            warn!(
                step = format!("{}", self).as_str(),
                "wait_in_millisecond" = self.wait_in_millisecond,
                current_retry = current_retry,
                "The pipe is full, wait before to retry"
            );
            thread::sleep(time::Duration::from_millis(self.wait_in_millisecond as u64));
            current_retry += 1;
        }

        Ok(())
    }
}
