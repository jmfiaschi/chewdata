use crate::connector::ConnectorType;
use crate::document::DocumentType;
use crate::step::Step;
use crate::DataResult;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use futures::StreamExt;
use serde::Deserialize;
use std::{fmt, io};
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
    #[instrument]
    async fn exec(
        &self,
        receiver_option: Option<Receiver<DataResult>>,
        sender_option: Option<Sender<DataResult>>,
    ) -> io::Result<()> {
        info!("Start");

        let sender = match sender_option {
            Some(sender) => sender,
            None => {
                info!("This step is skipped. Need a step after or a sender");
                return Ok(());
            }
        };

        let mut connector = self.connector_type.clone().connector();
        let document = self.document_type.clone().document_inner();
        connector.set_metadata(connector.metadata().merge(document.metadata()));

        match (receiver_option, connector.is_variable()) {
            (Some(receiver), true) => {
                // Used to check if the data has been received.
                let mut has_data_been_received = false;
                
                for data_result_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }

                    if !data_result_received.is_type(self.data_type.as_ref()) {
                        trace!(
                            data_type_accepted = self.data_type.to_string().as_str(),
                            data_result = format!("{:?}", data_result_received).as_str(),
                            "This step handle only this data type"
                        );
                        continue;
                    }

                    connector.set_parameters(data_result_received.clone().to_json_value());
                    let mut data = connector.pull_data(document.clone()).await?;

                    while let Some(ref mut sender_data_result) = data.next().await {
                        sender_data_result.merge(data_result_received.clone());
                        self.send(sender_data_result.clone(), &sender)?;
                    }
                }

                // If data has not been received and the channel has been close, run last time the step.
                // It arrive when the previous step don't push data through the pipe.
                if !has_data_been_received {
                    let mut data = connector.pull_data(document.clone()).await?;

                    while let Some(data_result) = data.next().await {
                        self.send(data_result, &sender)?;
                    }
                }
            }
            (Some(receiver), false) => {
                // Used to check if the data has been received.
                let mut has_data_been_received = false;

                for data_result_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }

                    // TODO: See if we can use stream::cycle and remove the pull_data from the loop.
                    // Useless to loop on the same document
                    let mut data = connector.pull_data(document.clone()).await?;

                    while let Some(ref mut sender_data_result) = data.next().await {
                        sender_data_result.merge(data_result_received.clone());
                        self.send(sender_data_result.clone(), &sender)?;
                    }
                }

                // If data has not been received and the channel has been close, run last time the step.
                // It arrive when the previous step don't push data through the pipe.
                if !has_data_been_received {
                    let mut data = connector.pull_data(document.clone()).await?;

                    while let Some(data_result) = data.next().await {
                        self.send(data_result, &sender)?;
                    }
                }
            }
            (None, _) => {
                let mut data = connector.pull_data(document.clone()).await?;

                while let Some(data_result) = data.next().await {
                    self.send(data_result, &sender)?;
                }
            }
        };

        drop(sender);

        info!("End");
        Ok(())
    }
}
