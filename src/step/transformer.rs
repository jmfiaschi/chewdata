//! Read the data from the input queue and transform it.
//!
//! ### Actions
//!
//! 1 - Get a [`crate::Context`] from the input queue.  
//! 2 - Extract the [`crate::DataResult`] from the [`crate::Context`].  
//! 3 - Transform the data with a list of [`crate::updater::Action`].  
//! 4 - Create a new [`crate::Context`] and attach the [`crate::DataResult`] to it.  
//! 5 - Push the new [`crate::Context`] into the output queue.  
//! 6 - Go to step 1 until the input queue is not empty.  
//!
//! ### Configuration
//!
//! | key           | alias   | Description                                                                                                       | Default Value | Possible Values                                 |
//! | ------------- | ------- | ----------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------- |
//! | type          | -       | Required in order to use transformer step                                                                         | `transformer` | `transformer` / `transform` / `t`               |
//! | updater       | u       | Updater type used as a template engine for transformation                                                         | `tera`        | `tera`                                          |
//! | referentials  | refs    | List of [`crate::step::Reader`] indexed by their name. A referential can be use to map object during the transformation | `null`        | `{"alias_a": READER,"alias_b": READER, etc...}` |
//! | name          | alias   | Name step                                                                                                         | `null`        | Auto generate alphanumeric value                |
//! | description   | desc    | Describ your step and give more visibility                                                                        | `null`        | String                                          |
//! | data_type     | data    | Type of data used for the transformation. skip other data type                                                    | `ok`          | `ok` / `err`                                    |
//! | thread_number | threads | Parallelize the step in multiple threads                                                                          | `1`           | unsigned number                                 |
//! | actions       | -       | List of [`crate::updater::Action`]                                                                                | `null`        | See [`crate::updater::Action`]                           |
//! | input_name    | input   | Input name variable can be used in the pattern action                                                             | `input`       | String                                          |
//! | output_name   | output  | Output name variable can be used in the pattern action                                                            | `output`      | String                                          |
//! | wait          | sleep   | Time in millisecond to wait before to fetch data result from the previous queue                                   | `10`          | unsigned number                                 |
//!
//! #### Action
//!
//! | key     | Description                                                                                                                                                           | Default Value | Possible Values                                                                                                                       |
//! | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
//! | field   | Json pointer that define the field path created into the output object                                                                                                | `/`           | alphanumeric or [json pointer](https://datatracker.ietf.org/doc/html/rfc6901)                                                         |
//! | pattern | Pattern in [django template language](https://docs.djangoproject.com/en/3.1/topics/templates/) format used to build the output field. This project use Tera's methods | `null`        |
//! | type    | Type of action                                                                                                                                                        | `merge`       | `merge` current result with the `output` result / `replace` the `output` result with the current result / `remove` the `output` field |
//!
//! ### Examples
//!
//! ```json
//! [
//!     ...
//!     {
//!         "type": "transformer",
//!         "updater": {
//!             "type": "tera"
//!         },
//!         "referentials": {
//!             "ref_a": {
//!                 "connector": {
//!                     "type": "io"
//!                 }
//!             }
//!         },
//!         "name": "transform_a",
//!         "description": "My description of the step",
//!         "connector": {
//!             "type": "io"
//!         },
//!         "document": {
//!             "type": "json"
//!         },
//!         "data_type": "ok",
//!         "thread_number": 1,
//!         "actions": [
//!             {
//!                 "pattern": "{{ my_input | json_encode() }}"
//!             },
//!             {
//!                 "field": "my_new_field",
//!                 "pattern": "{{ my_input.number * my_output.number * ref_a.number * steps.my_previous_step.number }}",
//!                 "type": "merge"
//!             },
//!             {
//!                 "field": "text",
//!                 "type": "remove"
//!             },
//!             {
//!                 "field": "array",
//!                 "pattern": "[\"a\",\"b\"]",
//!                 "type": "replace"
//!             }
//!         ],
//!         "input_name": "my_input",
//!         "output_name": "my_output",
//!         "wait: 10
//!     }
//!     ...
//! ]
//! ```
use super::super::helper::referentials_reader_into_value;
use super::DataResult;
use crate::step::reader::Reader;
use crate::step::Step;
use crate::updater::{Action, UpdaterType};
use crate::Context;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
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
    pub receiver: Option<Receiver<Context>>,
    #[serde(skip)]
    pub sender: Option<Sender<Context>>,
}

impl Default for Transformer {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Transformer {
            updater_type: UpdaterType::default(),
            referentials: None,
            name: uuid.simple().to_string(),
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
    /// See [`Step::sleep`] for more details.
    fn sleep(&self) -> u64 {
        self.wait
    }
    #[instrument(name = "transformer::exec")]
    async fn exec(&self) -> io::Result<()> {
        let referentials = match self.referentials.clone() {
            Some(referentials) => Some(referentials_reader_into_value(referentials).await?),
            None => None,
        };

        let mut receiver_stream = super::receive(self as &dyn Step).await?;
        while let Some(ref mut context_received) = receiver_stream.next().await {
            let data_result = context_received.data_result();
            if !data_result.is_type(self.data_type.as_ref()) {
                trace!("This step handle only this data type");
                super::send(self as &dyn Step, &context_received.clone()).await?;
                continue;
            }

            let record = data_result.to_value();

            let new_data_result = match self.updater_type.updater().update(
                record.clone(),
                context_received.history(),
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
                Err(e) => {
                    warn!(
                        record = format!("{}", record).as_str(),
                        error = format!("{}", e).as_str(),
                        "The transformer's updater raise an error"
                    );
                    DataResult::Err((record, e))
                }
            };

            context_received.insert_step_result(self.name(), new_data_result)?;
            super::send(self as &dyn Step, &context_received.clone()).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::io::{Error, ErrorKind};
    use std::thread;

    #[async_std::test]
    async fn exec_with_different_data_result_type() {
        let mut step = Transformer::default();
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
        let mut step = Transformer::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone())).unwrap();

        let mut expected_context = context.clone();
        let data2: Value = serde_json::from_str(r#"{"field_1":"value_2"}"#).unwrap();
        expected_context
            .insert_step_result("my_step".to_string(), DataResult::Ok(data2))
            .unwrap();

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.actions =
            serde_json::from_str(r#"[{"field":"field_1","pattern": "value_2"}]"#).unwrap();
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
}
