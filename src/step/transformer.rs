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
//! ### Configuration
//!
//! | key           | alias   | Description                                                                                                       | Default Value | Possible Values                                       |
//! | ------------- | ------- | ----------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------------- |
//! | type          | -       | Required in order to use transformer step                                                                         | `transformer` | `transformer` / `transform` / `t`                     |
//! | updater       | u       | Updater type used as a template engine for transformation                                                         | `tera`        | `tera`                                                |
//! | referentials  | refs    | List of [`crate::step::Reader`] indexed by their name. A referential can be use to map object during the transformation | `null`        | `{"alias_a": READER,"alias_b": READER, etc...}` |
//! | name          | alias   | Name step                                                                                                         | `null`        | Auto generate alphanumeric value                      |
//! | data_type     | data    | Type of data used for the transformation. skip other data type                                                    | `ok`          | `ok` / `err`                                          |
//! | concurrency_limit | -       | Limit of steps to run in concurrence.                                                                          | `1`           | unsigned number                                       |
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
//!         "connector": {
//!             "type": "io"
//!         },
//!         "document": {
//!             "type": "json"
//!         },
//!         "data_type": "ok",
//!         "concurrency_limit": 1,
//!         "actions": [
//!             { # Force to set 'output' with the data in 'input'.
//!                 "pattern": "{{ input | json_encode() }}"
//!             },
//!             {}, # Do the same as before.
//!             { # Create a new field 'my_new_field' in the output and set the value with the 'pattern' expression.
//!                 "field": "my_new_field",
//!                 "pattern": "{{ input.number * output.number * ref_a.number * steps.my_previous_step.number }}",
//!                 "type": "merge"
//!             },
//!             { # Remove the field 'text'.
//!                 "field": "text",
//!                 "type": "remove"
//!             },
//!             { # Replace the 'array' field value.
//!                 "field": "array",
//!                 "pattern": "[\"a\",\"b\"]",
//!                 "type": "replace"
//!             }
//!         ]
//!     }
//!     ...
//! ]
//! ```
use super::reader::Reader;
use super::referential::Referential;
use super::DataResult;
use crate::step::Step;
use crate::updater::{Action, UpdaterType};
use crate::Context;
use async_channel::{Receiver, Sender};
use async_std::task;
use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use uuid::Uuid;
use crate::helper::string::DisplayOnlyForDebugging;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Transformer {
    #[serde(rename = "updater")]
    #[serde(alias = "u")]
    pub updater_type: UpdaterType,
    #[serde(alias = "refs")]
    pub referentials: HashMap<String, Reader>,
    #[serde(alias = "alias")]
    pub name: String,
    pub data_type: String,
    pub concurrency_limit: usize,
    // Use Vec in order to keep the FIFO order.
    pub actions: Vec<Action>,
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
            referentials: HashMap::default(),
            name: uuid.simple().to_string(),
            data_type: DataResult::OK.to_string(),
            concurrency_limit: 1,
            actions: Vec::default(),
            receiver: None,
            sender: None,
        }
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
    #[instrument(name = "transformer::exec",
        skip(self),
        fields(name=self.name, 
        data_type=self.data_type,
        concurrency_limit=self.concurrency_limit,
    ))]
    async fn exec(&self) -> io::Result<()> {
        info!("Starts transforming data...");

        let receiver_stream = self.receive().await;

        trace!("Warm up static referential before using it in the concurrent execution.");
        Referential::new(&self.referentials).to_value(&Context::new(String::default(), DataResult::Ok(Value::default()))).await?;

        // Transform in concurrence with parallelism.
        let results: Vec<_> = receiver_stream.map(|context_received| {
            let step = self.clone();
            task::spawn(async move {
                transform(&step, &mut context_received.clone()).await
            })
        }).buffer_unordered(self.concurrency_limit).collect().await;

        results
            .into_iter()
            .filter(|result| result.is_err())
            .map(|result| warn!("{:?}", result))
            .for_each(drop);

        info!("Stops transforming data and sending context in the channel");

        Ok(())
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

#[instrument(name = "transformer::transform", skip(step, context_received))]
async fn transform(step: &Transformer, context_received: &mut Context) -> io::Result<()> {
    let data_result = context_received.input();
    if !data_result.is_type(step.data_type.as_ref()) {
        trace!("Handles only this data type");
        step.send(context_received).await;
        return Ok(());
    }
    
    let record = data_result.to_value();

    match step.updater_type.updater().update(
        &record,
        &context_received.to_value()?,
        &Referential::new(&step.referentials).to_value(context_received).await?,
        &step.actions,
    ).await {
        Ok(new_record) => match &new_record {
            Value::Array(array) => {
                info!(
                    from = record.display_only_for_debugging(),
                    to = new_record.display_only_for_debugging(),
                    "data transformed with success"
                );

                for array_value in array {
                    context_received.insert_step_result(step.name(), DataResult::Ok(array_value.clone()));
                    step.send(context_received).await;
                }
            }
            Value::Null => {
                info!(
                    record = new_record.display_only_for_debugging(),
                    "Record skip because the value is null"
                );
            }
            _ => {
                info!(
                    from = record.display_only_for_debugging(),
                    to = new_record.display_only_for_debugging(),
                    "data transformed with success"
                );

                context_received.insert_step_result(step.name(), DataResult::Ok(new_record.clone()));
                step.send(context_received).await;
            }
        },
        Err(e) => {
            warn!(
                from = record.display_only_for_debugging(),
                error = format!("{}", e).as_str(),
                context = context_received.display_only_for_debugging(),
                "The transformer's updater raise an error"
            );

            context_received.insert_step_result(step.name(), DataResult::Err((record.clone(), e)));

            step.send(context_received).await;
        }
    };

    Ok(())
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
        let context = Context::new("before".to_string(), DataResult::Err((data, error)));
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
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone()));

        let mut expected_context = context.clone();
        let data2: Value = serde_json::from_str(r#"{"field_1":"value_2"}"#).unwrap();
        expected_context.insert_step_result("my_step".to_string(), DataResult::Ok(data2));

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
    #[async_std::test]
    async fn exec_with_array() {
        let mut step = Transformer::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone()));

        let mut expected_context_1 = context.clone();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        expected_context_1.insert_step_result("my_step".to_string(), DataResult::Ok(data));

        let mut expected_context_2 = context.clone();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_2"}"#).unwrap();
        expected_context_2.insert_step_result("my_step".to_string(), DataResult::Ok(data));

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.actions = serde_json::from_str(
            r#"[{"pattern": "[{\"field_1\":\"value_1\"},{\"field_1\":\"value_2\"}]"}]"#,
        )
        .unwrap();
        step.exec().await.unwrap();

        assert_eq!(expected_context_1, receiver_output.recv().await.unwrap());
        assert_eq!(expected_context_2, receiver_output.recv().await.unwrap());
    }
}