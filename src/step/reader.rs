use crate::connector::Connector;
use crate::document::{Document, DocumentType};
use crate::step::Step;
use crate::DataResult;
use crate::{connector::ConnectorType, StepContext};
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
    #[serde(alias = "alias")]
    pub name: String,
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "threads")]
    pub thread_number: usize,
}

impl Default for Reader {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Reader {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            name: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            thread_number: 1,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Reader {{'{}','{}'}}",
            self.name,
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
        receiver_option: Option<Receiver<StepContext>>,
        sender_option: Option<Sender<StepContext>>,
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
                // Used to check if one data has been received.
                let mut has_data_been_received = false;

                for step_context_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }

                    if !step_context_received
                        .data_result()
                        .is_type(self.data_type.as_ref())
                    {
                        trace!("This step handle only this data type");
                        continue;
                    }

                    connector.set_parameters(step_context_received.to_value()?);

                    exec_connector(
                        self,
                        &mut connector,
                        &document,
                        &sender,
                        &Some(step_context_received),
                    )
                    .await?
                }

                // If data has not been received and the channel has been close, run last time the step.
                // It arrive when the previous step don't push data through the pipe.
                if !has_data_been_received {
                    exec_connector(self, &mut connector, &document, &sender, &None).await?
                }
            }
            (Some(receiver), false) => {
                // Used to check if one data has been received.
                let mut has_data_been_received = false;

                for step_context_received in receiver {
                    if !has_data_been_received {
                        has_data_been_received = true;
                    }

                    exec_connector(
                        self,
                        &mut connector,
                        &document,
                        &sender,
                        &Some(step_context_received),
                    )
                    .await?
                }

                // If data has not been received and the channel has been close, run last time the step.
                // It arrive when the previous step don't push data through the pipe.
                if !has_data_been_received {
                    exec_connector(self, &mut connector, &document, &sender, &None).await?
                }
            }
            (None, _) => exec_connector(self, &mut connector, &document, &sender, &None).await?,
        };

        drop(sender);

        info!("End");
        Ok(())
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

async fn exec_connector<'step>(
    step: &'step Reader,
    connector: &'step mut Box<dyn Connector>,
    document: &'step Box<dyn Document>,
    pipe: &'step Sender<StepContext>,
    context: &'step Option<StepContext>,
) -> io::Result<()> {
    // todo: remove paginator mutability
    let mut paginator = connector.paginator().await?;
    let mut stream = paginator.stream().await?;

    match paginator.is_parallelizable() {
        true => {
            // Concurrency stream
            // The loop cross the paginator never stop. The paginator mustn't return indefinitely a connector.
            stream.for_each_concurrent(step.thread_number, |connector_result| async move {
                    let mut connector = match connector_result {
                        Ok(connector) => connector,
                        Err(e) => {
                            warn!(error = e.to_string().as_str(), "Pagination througth the paginator failed. The concurrency loop in the paginator continue");
                            return;
                        }
                    };
                    match send_data_into_pipe(step, &mut connector, document, pipe, context).await
                    {
                        Ok(Some(_)) => trace!("All data pushed into the pipe"),
                        Ok(None) => trace!("Connector doesn't have any data to pushed into the pipe. The concurrency loop in the paginator continue"),
                        Err(e) => warn!(error = e.to_string().as_str(), "Impossible to push data into the pipe. The concurrency loop in the paginator continue")
                    };
                })
                .await;
        }
        false => {
            // Iterative stream
            // The loop cross the paginator stop if
            //  * An error raised
            //  * The current connector is empty: [], {}, "", etc...
            while let Some(ref mut connector_result) = stream.next().await {
                let connector = match connector_result {
                    Ok(connector) => connector,
                    Err(e) => {
                        warn!(error = e.to_string().as_str(), "Pagination througth the paginator failed. The iterative loop in the paginator is stoped");
                        break;
                    }
                };
                match send_data_into_pipe(step, connector, document, pipe, context).await? {
                    Some(_) => trace!("All data pushed into the pipe"),
                    None => {
                        trace!("Connector doesn't have any data to pushed into the pipe. The iterative loop in the paginator is stoped");
                        break;
                    }
                };
            }
        }
    };
    Ok(())
}

async fn send_data_into_pipe<'step>(
    step: &'step Reader,
    connector: &'step mut Box<dyn Connector>,
    document: &'step Box<dyn Document>,
    pipe: &'step Sender<StepContext>,
    context: &'step Option<StepContext>,
) -> io::Result<Option<()>> {
    connector.fetch().await?;

    let mut dataset = match connector.pull_dataset(document.clone()).await? {
        Some(dataset) => dataset,
        None => return Ok(None),
    };

    while let Some(data_result) = dataset.next().await {
        let context = match context.clone() {
            Some(ref mut context) => {
                context.insert_step_result(step.name(), data_result)?;
                context.clone()
            }
            None => StepContext::new(step.name(), data_result)?,
        };

        step.send(context, pipe)?;
    }

    Ok(Some(()))
}
