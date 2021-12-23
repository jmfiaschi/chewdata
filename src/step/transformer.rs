use super::DataResult;
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::{Action, UpdaterType};
use crate::StepContext;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
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
    pub alias: String,
    pub description: Option<String>,
    pub data_type: String,
    #[serde(alias = "threads")]
    pub thread_number: usize,
    // Use Vec in order to keep the order FIFO.
    pub actions: Vec<Action>,
    #[serde(alias = "input")]
    input_name: String,
    #[serde(alias = "output")]
    output_name: String,
}

impl Default for Transformer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Transformer {
            updater_type: UpdaterType::default(),
            referentials: None,
            alias: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            thread_number: 1,
            actions: Vec::default(),
            input_name: "input".to_string(),
            output_name: "output".to_string(),
        }
    }
}

impl fmt::Display for Transformer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Transformer {{'{}','{}' }}",
            self.alias,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

/// This Step transform a dataset.
#[async_trait]
impl Step for Transformer {
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

        let receiver = match receiver_option {
            Some(receiver) => receiver,
            None => {
                info!("This step is skipped. Need a step before or a receiver");
                return Ok(());
            }
        };

        let referentials = match self.referentials.clone() {
            Some(referentials) => Some(super::referentials_reader_into_value(referentials).await?),
            None => None,
        };

        for mut step_context_received in receiver {
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

            step_context_received.insert_step_result(self.alias(), new_data_result)?;
            self.send(step_context_received.clone(), &sender)?;
        }

        drop(sender);

        info!("End");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
    fn alias(&self) -> String {
        self.alias.clone()
    }
}
