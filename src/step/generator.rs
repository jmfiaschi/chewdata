//! Generate an empty [`crate::DataResult`] that you can enrich with the [`crate::step::Transformer`].
//!
//! It possible to duplicate input data and enrich them after.
//!
//! ### Actions
//!
//! 1 - Get a [`crate::Context`] from the input queue.  
//! 2 - Extract the [`crate::DataResult`] from the [`crate::Context`].  
//! 3 - Clone the current [`crate::Context`] or it create a new one if empty.  
//! 4 - Push the [`crate::Context`] into the output queue.  
//! 5 - Go to the step 3 n times.  
//! 6 - Go to step 1 until the input queue is not empty.  
//!
//! ### Configuration
//!
//! | key          | alias | Description                                                                     | Default Value | Possible Values                  |
//! | ------------ | ----- | ------------------------------------------------------------------------------- | ------------- | -------------------------------- |
//! | type         | -     | Required in order to use generator step                                         | `generator`   | `generator` / `g`                |
//! | name         | alias | Name step                                                                       | `null`        | Auto generate alphanumeric value |
//! | data_type    | data  | Type of data used for the transformation. skip other data type                  | `ok`          | `ok` / `err`                     |
//! | dataset_size | batch | Stack size limit before to push data into the resource though the connector     | `1000`        | unsigned number                  |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "generator",
//!         "name": "my_generator",
//!         "data_type": "ok",
//!         "dataset_size": 1000,
//!     },
//!     {
//!         "type": "transformer",
//!         "actions": [
//!             {
//!                 "field":"firstname",
//!                 "pattern": "{{ fake_first_name() }}"
//!             },
//!             {
//!                 "field":"lastname",
//!                 "pattern": "{{ fake_last_name() }}"
//!             },
//!             {
//!                 "field":"city",
//!                 "pattern": "{{ fake_city() }}"
//!             },
//!             {
//!                 "field":"password",
//!                 "pattern": "{{ fake_password(min = 5, max = 10) }}"
//!             },
//!             {
//!                 "field":"color",
//!                 "pattern": "{{ fake_color_hex() }}"
//!             }
//!         ]
//!     },
//!     {
//!         "type": "writer"
//!     }
//! ]
//! ```
//!
//! No input.
//!
//! output:
//!
//! ```json
//! [
//!     {"firstname": "my firstname", "lastname": "my lastname", "city": "my city", "password": "my password", "color": "my color"},
//!     ...
//! ]
//! ```
use crate::step::Step;
use crate::Context;
use crate::DataResult;
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
    #[serde(alias = "data")]
    pub data_type: String,
    #[serde(alias = "batch")]
    #[serde(alias = "size")]
    pub dataset_size: usize,
    #[serde(skip)]
    pub receiver: Option<Receiver<Context>>,
    #[serde(skip)]
    pub sender: Option<Sender<Context>>,
}

impl Default for Generator {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Generator {
            name: uuid.simple().to_string(),
            data_type: DataResult::OK.to_string(),
            dataset_size: 1,
            receiver: None,
            sender: None,
        }
    }
}

impl fmt::Display for Generator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Generator {{'{}'}}", self.name,)
    }
}

#[async_trait]
impl Step for Generator {
    /// See [`Step::set_receiver`] for more details.
    fn set_receiver(&mut self, receiver: Receiver<Context>) {
        self.receiver = Some(receiver);
    }
    /// See [`Step::receiver`] for more details.
    fn receiver(&self) -> Option<&Receiver<Context>> {
        self.receiver.as_ref()
    }
    /// See [`Step::set_sender`] for more details.
    fn set_sender(&mut self, sender: Sender<Context>) {
        self.sender = Some(sender);
    }
    /// See [`Step::sender`] for more details.
    fn sender(&self) -> Option<&Sender<Context>> {
        self.sender.as_ref()
    }
    #[instrument(name = "generator::exec")]
    async fn exec(&self) -> io::Result<()> {
        let mut has_data_been_received = false;
        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        let dataset_size = self.dataset_size;

        while let Some(context_received) = receiver_stream.next().await {
            if !has_data_been_received {
                has_data_been_received = true;
            }

            if !context_received.input().is_type(self.data_type.as_ref()) {
                trace!("This step handle only this data type");
                super::send(self as &dyn Step, &context_received).await?;
                continue;
            }

            for _ in 0..dataset_size {
                let mut context = context_received.clone();
                context.insert_step_result(self.name(), context.input())?;
                super::send(self as &dyn Step, &context).await?;
            }
        }

        if !has_data_been_received {
            for _ in 0..dataset_size {
                let context = Context::new(self.name(), DataResult::Ok(Value::Null))?;
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
        let context = Context::new("before".to_string(), DataResult::Err((data, error))).unwrap();
        let expected_context = context.clone();

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
    #[async_std::test]
    async fn exec_with_same_data_result_type() {
        let mut step = Generator::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone())).unwrap();
        let mut expected_context = context.clone();
        expected_context
            .insert_step_result("my_step".to_string(), DataResult::Ok(data))
            .unwrap();

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
}
