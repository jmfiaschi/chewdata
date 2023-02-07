use crate::step::Step;
use crate::DataResult;
use crate::StepContext;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::{fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Generator {
    #[serde(alias = "alias")]
    pub name: String,
    #[serde(alias = "desc")]
    pub description: Option<String>,
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    #[serde(alias = "size")]
    pub dataset_size: usize,
    // Time in millisecond to wait before to fetch/send new data from/in the pipe.
    #[serde(alias = "sleep")]
    pub wait: u64,
    #[serde(skip)]
    pub receiver: Option<Receiver<StepContext>>,
    #[serde(skip)]
    pub sender: Option<Sender<StepContext>>,
}

impl Default for Generator {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Generator {
            name: uuid.simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            dataset_size: 1,
            receiver: None,
            sender: None,
            wait: 10,
        }
    }
}

impl fmt::Display for Generator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Generator {{'{}','{}'}}",
            self.name,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

#[async_trait]
impl Step for Generator {
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
    #[instrument(name = "generator::exec")]
    async fn exec(&self) -> io::Result<()> {
        let mut has_data_been_received = false;
        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        let dataset_size = self.dataset_size;

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

            for _ in 0..dataset_size {
                let mut context = step_context_received.clone();
                context.insert_step_result(self.name(), context.data_result())?;
                super::send(self as &dyn Step, &context).await?;
            }
        }

        if !has_data_been_received {
            for _ in 0..dataset_size {
                let context = StepContext::new(self.name(), DataResult::Ok(Value::Null))?;
                super::send(self as &dyn Step, &context).await?;
            }
        }

        Ok(())
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::io::{Error, ErrorKind};
    use std::thread;

    #[async_std::test]
    async fn exec_with_different_data_result_type() {
        let mut step = Generator::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let error = Error::new(ErrorKind::InvalidData, "My error");
        let step_context =
            StepContext::new("before".to_string(), DataResult::Err((data, error))).unwrap();
        let expected_step_context = step_context.clone();

        thread::spawn(move || {
            sender_input.try_send(step_context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.exec().await.unwrap();

        assert_eq!(expected_step_context, receiver_output.recv().await.unwrap());
    }
    #[async_std::test]
    async fn exec_with_same_data_result_type() {
        let mut step = Generator::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let step_context =
            StepContext::new("before".to_string(), DataResult::Ok(data.clone())).unwrap();
        let mut expected_step_context = step_context.clone();
        expected_step_context
            .insert_step_result("my_step".to_string(), DataResult::Ok(data))
            .unwrap();

        thread::spawn(move || {
            sender_input.try_send(step_context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.exec().await.unwrap();

        assert_eq!(expected_step_context, receiver_output.recv().await.unwrap());
    }
}
