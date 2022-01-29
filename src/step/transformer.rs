use super::super::helper::referentials_reader_into_value;
use super::DataResult;
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::{Action, UpdaterType};
use crate::StepContext;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    #[serde(rename = "updater")]
    #[serde(alias = "u")]
    pub updater_type: UpdaterType,
    #[serde(alias = "refs")]
    pub referentials: Option<HashMap<String, Reader>>,
    #[serde(alias = "alias")]
    pub name: String,
    pub description: Option<String>,
    pub data_type: String,
    #[serde(alias = "threads")]
    pub thread_number: usize,
    // Use Vec in order to keep the FIFO order.
    pub actions: Vec<Action>,
    #[serde(alias = "input")]
    pub input_name: String,
    #[serde(alias = "output")]
    pub output_name: String,
    // Time in millisecond to wait before to fetch/send new data from/in the pipe. 
    #[serde(alias = "sleep")]
    pub wait: u64,
    #[serde(skip)]
    pub receiver: Option<Receiver<StepContext>>,
    #[serde(skip)]
    pub sender: Option<Sender<StepContext>>,
}

impl Default for Transformer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Transformer {
            updater_type: UpdaterType::default(),
            referentials: None,
            name: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            thread_number: 1,
            actions: Vec::default(),
            input_name: "input".to_string(),
            output_name: "output".to_string(),
            receiver: None,
            sender: None,
            wait: 10,
        }
    }
}

impl fmt::Display for Transformer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Transformer {{'{}','{}' }}",
            self.name,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

/// This Step transform a dataset.
#[async_trait]
impl Step for Transformer {
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
        let referentials = match self.referentials.clone() {
            Some(referentials) => Some(referentials_reader_into_value(referentials).await?),
            None => None,
        };

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(ref mut step_context_received) = receiver_stream.next().await {
            let data_result = step_context_received.data_result();
            if !data_result.is_type(self.data_type.as_ref()) {
                trace!("This step handle only this data type");
                continue;
            }

            let record = data_result.to_value();

            let new_data_result = match self.updater_type.updater().update(
                record.clone(),
                step_context_received.steps_result(),
                referentials.clone(),
                self.actions.clone(),
                self.input_name.clone(),
                self.output_name.clone(),
            ) {
                Ok(new_record) => {
                    if Value::Null == new_record {
                        trace!(
                            record = format!("{}", new_record).as_str(),
                            "Record skip because the value si null"
                        );
                        continue;
                    }

                    DataResult::Ok(new_record)
                }
                Err(e) => DataResult::Err((record, e)),
            };

            step_context_received.insert_step_result(self.name(), new_data_result)?;
            super::send(self as &dyn Step, &step_context_received.clone()).await?;
        }

        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}
