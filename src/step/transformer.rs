use super::DataResult;
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::{Action, UpdaterType};
use async_trait::async_trait;
use multiqueue::{MPMCReceiver, MPMCSender};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io};
use std::{thread, time};
use tracing::Instrument;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    #[serde(rename = "updater")]
    #[serde(alias = "u")]
    pub updater_type: UpdaterType,
    #[serde(alias = "refs")]
    pub referentials: Option<HashMap<String, Reader>>,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    #[serde(alias = "wait")]
    pub wait_in_millisecond: usize,
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
            alias: Some(uuid.to_simple().to_string()),
            description: None,
            data_type: DataResult::OK.to_string(),
            wait_in_millisecond: 10,
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
            self.alias
                .to_owned()
                .unwrap_or_else(|| "No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}
/// Return a referentials hashmap indexed by the alias of the referential.
async fn referentials_reader_to_dataset(
    referentials: HashMap<String, Reader>,
) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_dataset = HashMap::new();

    // For each reader, try to build the referential.
    for (alias, referential) in referentials {
        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        let mut referential_dataset: Vec<Value> = Vec::new();

        referential
            .exec(None, Some(pipe_inbound))
            .instrument(tracing::info_span!("exec"))
            .await?;

        for data_result in pipe_outbound {
            referential_dataset.push(data_result.to_json_value());
        }
        referentials_dataset.insert(alias, referential_dataset);
    }

    Ok(referentials_dataset)
}
/// This Step transform a dataset.
#[async_trait]
impl Step for Transformer {
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

        let mapping = match self.referentials.clone() {
            Some(referentials) => Some(referentials_reader_to_dataset(referentials).await?),
            None => None,
        };

        for data_result in pipe_outbound {
            if !data_result.is_type(self.data_type.as_ref()) {
                trace!(
                    data_type = self.data_type.to_string().as_str(),
                    data = format!("{:?}", data_result).as_str(),
                    step = format!("{}", self.clone()).as_str(),
                    "This step handle only this data type"
                );
                continue;
            }

            let record = data_result.to_json_value();

            let new_data_results = match self.updater_type.updater().update(
                record.clone(),
                mapping.clone(),
                self.actions.clone(),
                self.input_name.clone(),
                self.output_name.clone(),
            ) {
                Ok(new_record) => {
                    trace!(
                        step = format!("{}", self).as_str(),
                        record = format!("{}", new_record).as_str(),
                        "Record transformation success"
                    );

                    if Value::Null == new_record {
                        trace!(
                            step = format!("{}", self).as_str(),
                            record = format!("{}", new_record).as_str(),
                            "Record skip because the value si null"
                        );
                        continue;
                    }

                    let new_data_result = DataResult::Ok(new_record);
                    trace!(
                        step = format!("{}", self).as_str(),
                        data_result = format!("{:?}", new_data_result).as_str(),
                        "New data result"
                    );
                    new_data_result
                }
                Err(e) => {
                    let new_data_result = DataResult::Err((record, e));
                    warn!(
                        step = format!("{}", self).as_str(),
                        data_result = format!("{:?}", new_data_result).as_str(),
                        "Record transformation error. New data result with error"
                    );
                    new_data_result
                }
            };

            trace!(
                data = format!("{:?}", new_data_results).as_str(),
                step = format!("{}", self.clone()).as_str(),
                "Data send to the queue"
            );

            let mut current_retry = 0;
            while pipe_inbound.try_send(new_data_results.clone()).is_err() {
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

        drop(pipe_inbound);

        trace!(step = format!("{}", self).as_str(), "Exec ended");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
}
