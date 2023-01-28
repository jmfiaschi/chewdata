use crate::connector::Connector;
use crate::document::{Document, DocumentType};
use crate::step::Step;
use crate::DataResult;
use crate::{connector::ConnectorType, StepContext};
use async_trait::async_trait;
use async_channel::{Receiver, Sender};
use futures::StreamExt;
use serde::Deserialize;
use std::{fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
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
    // Time in millisecond to wait before to fetch/send new data from/in the pipe.
    #[serde(alias = "sleep")]
    pub wait: u64,
    #[serde(skip)]
    pub receiver: Option<Receiver<StepContext>>,
    #[serde(skip)]
    pub sender: Option<Sender<StepContext>>,
}

impl Default for Reader {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Reader {
            connector_type: ConnectorType::default(),
            document_type: DocumentType::default(),
            name: uuid.simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            receiver: None,
            sender: None,
            wait: 10,
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
    /// See [`Step::set_receiver`] for more details.
    fn set_receiver(&mut self, receiver: Receiver<StepContext>) {
        self.receiver = Some(receiver);
    }
    /// See [`Step::receiver`] for more details.
    fn receiver(&self) -> Option<&Receiver<StepContext>> {
        self.receiver.as_ref()
    }
    /// See [`Step::set_sender`] for more details.
    fn set_sender(&mut self, sender: Sender<StepContext>) {
        self.sender = Some(sender);
    }
    /// See [`Step::sender`] for more details.
    fn sender(&self) -> Option<&Sender<StepContext>> {
        self.sender.as_ref()
    }
    /// See [`Step::sleep`] for more details.
    fn sleep(&self) -> u64 {
        self.wait
    }
    #[instrument]
    async fn exec(&self) -> io::Result<()> {
        let mut connector = self.connector_type.clone().boxed_inner();
        let document = self.document_type.ref_inner();
        connector.set_metadata(connector.metadata().merge(document.metadata()));

        // Used to check if one data has been received.
        let mut has_data_been_received = false;

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(step_context_received) = receiver_stream.next().await {
            if !has_data_been_received {
                has_data_been_received = true;
            }

            if !step_context_received
                .data_result()
                .is_type(self.data_type.as_ref())
            {
                trace!("This step handle only this data type");
                super::send(self as &dyn Step, &step_context_received.clone()).await?;
                continue;
            }

            connector.set_parameters(step_context_received.to_value()?);

            exec_connector(
                self,
                &mut connector,
                document,
                &Some(step_context_received),
            )
            .await?
        }

        // If data has not been received and the channel has been close, run last time the step.
        // It arrive when the previous step don't push data through the pipe.
        if !has_data_been_received {
            exec_connector(self, &mut connector, document, &None).await?
        }

        Ok(())
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

async fn exec_connector<'step>(
    step: &'step Reader,
    connector: &'step mut Box<dyn Connector>,
    document: &'step dyn Document,
    context: &'step Option<StepContext>,
) -> io::Result<()> {
    // todo: remove paginator mutability
    let paginator = connector.paginator().await?;
    let mut stream = paginator.stream().await?;

    match paginator.is_parallelizable() {
        true => {
            // Concurrency stream
            // The loop cross the paginator never stop. The paginator mustn't return indefinitely a connector.
            stream.for_each_concurrent(None, |connector_result| async move {
                    let mut connector = match connector_result {
                        Ok(connector) => connector,
                        Err(e) => {
                            warn!(error = e.to_string().as_str(), "Pagination through the paginator failed. The concurrency loop in the paginator continue");
                            return;
                        }
                    };
                    match send_data_into_pipe(step, &mut connector, document, context).await
                    {
                        Ok(Some(_)) => trace!("All data has been pushed into the pipe. The concurrency loop in the paginator continue"),
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
                        warn!(error = e.to_string().as_str(), "Pagination through the paginator failed. The iterative loop in the paginator is stoped");
                        break;
                    }
                };
                match send_data_into_pipe(step, connector, document, context).await? {
                    Some(_) => trace!("All data has been pushed into the pipe. The iterative loop in the paginator continue"),
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
    document: &'step dyn Document,
    context: &'step Option<StepContext>,
) -> io::Result<Option<()>> {
    let mut dataset = match connector.fetch(document).await? {
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

        super::send(step as &dyn Step, &context).await?;
    }

    Ok(Some(()))
}
