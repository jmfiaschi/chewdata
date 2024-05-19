//! Check the consistancy of the data.
//!
//! If a data is not valid, an error message is stored in the field `_error` before to share the data to another step and the data is tagged with an error.
//! Use the `data_type` field of a `step` to target which kind of data a step can handle.
//!
//! ### Actions
//!
//! 1 - Get a [`crate::Context`] from the input queue.  
//! 2 - Extract the [`crate::DataResult`] from the [`crate::Context`].  
//! 3 - Validate the data with a list of rules.  
//! 4 - Create a new [`crate::Context`] and attach the [`crate::DataResult`] to it.  
//! 5 - Push the new [`crate::Context`] into the output queue.  
//! 6 - Go to step 1 until the input queue is not empty.  
//!
//! ### Configuration
//!
//! | key             | alias   | Description                                                                                                       | Default Value | Possible Values                                 |
//! | --------------- | ------- | ----------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------- |
//! | type            | -       | Required in order to use transformer step                                                                         | `transformer` | `transformer` / `transform` / `t`               |
//! | updater         | u       | Updater type used as a template engine for transformation                                                         | `tera`        | `tera`                                          |
//! | referentials    | refs    | List of [`crate::step::Reader`] indexed by their name. A referential can be use to map object during the validation | `null`        | `{"alias_a": READER,"alias_b": READER, etc...}` |
//! | name            | alias   | Name step                                                                                                         | `null`        | Auto generate alphanumeric value                |
//! | data_type       | data    | Type of data used for the transformation. skip other data type                                                    | `ok`          | `ok` / `err`                                    |
//! | concurrency_limit   | -   | Limit of steps to run in concurrence.                                                                              | `1`           | unsigned number                                 |
//! | rules           | -       | List of [`self::Rule`] indexed by their names                                                                     | `null`        | `{"rule_0": Rule,"rule_1": Rule}`               |
//! | error_separator | -       | Separator use to delimite two errors                                                                              | `\r\n`        | String                                          |
//!
//! ### Rule
//!
//! | key     | Description                                                                                                                                                                                     | Default Value | Possible Values                                   |
//! | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------- |
//! | pattern | Pattern in [django template language](https://docs.djangoproject.com/en/3.1/topics/templates/) format used to test a field. If the result of the pattern is not a boolean, an error will raised | `null`        | `{% if true %} true {% else %} false {% endif %}` |
//! | message | Message to display if the render pattern is false. If the message is empty, a default value is rendered                                                                                         | `string`      | `My error message`                                |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "validator",
//!         "updater": {
//!             "type": "tera"
//!         },
//!         "referentials": {
//!             "mapping_ref": {
//!                 "connector": {
//!                     "type": "mem",
//!                     "data": "[{\"mapping_code\":\"value_to_map\",\"mapping_value\":\"value mapped\"},{\"mapping_code\":\"value_to_map_2\",\"mapping_value\":\"value mapped 2\"}]"
//!                 }
//!             }
//!         },
//!         "name": "my_validator",
//!         "data_type": "ok",
//!         "concurrency_limit": 1,
//!         "rules": {
//!             "number_rule": {
//!                 "pattern": "{% if input.number == 10  %} true {% else %} false {% endif %}",
//!                 "message": "The number field value must be equal to 10"
//!             },
//!             "text_rule": {
//!                 "pattern": "{% if input.text is matching('.*hello world.*') %} true {% else %} false {% endif %}",
//!                 "message": "The text field value doesn't contain 'Hello World'"
//!             },
//!             "code_rule": {
//!                 "pattern": "{% if mapping_ref | filter(attribute='mapping_code', value=input.code) | length > 0 %} true {% else %} false {% endif %}",
//!                 "message": "The code field value doesn't match with the referential dataset"
//!             }
//!         },
//!         "error_separator": " & "
//!     }
//! ]
//! ```
//!
//! input:
//!
//! ```json
//! [
//!     {"number": 100, "text": "my text", "code": "my_code"},
//!     ...
//! ]
//! ```
//!
//! output:
//!
//! ```json
//! [
//!     {"number": 100, "text": "my text", "code": "my_code", "_error":"The number field value must be equal to 10 & The text field value doesn't contain 'Hello World' & The code field value doesn't match with the referential dataset"},
//!     ...
//! ]
//! ```
use super::DataResult;
use super::reader::Reader;
use super::referential::Referential;
use crate::helper::json_pointer::JsonPointer;
use crate::helper::mustache::Mustache;
use crate::step::Step;
use crate::updater::{ActionType, UpdaterType, INPUT_FIELD_KEY};
use crate::Context;
use crate::updater::Action;
use async_channel::{Receiver, Sender};
use async_std::task;
use async_trait::async_trait;
use futures::StreamExt;
use json_value_merge::Merge;
use json_value_search::Search;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::{
    collections::BTreeMap,
    io,
};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Validator {
    #[serde(rename = "updater")]
    #[serde(alias = "u")]
    pub updater_type: UpdaterType,
    #[serde(alias = "refs")]
    pub referentials: HashMap<String, Reader>,
    #[serde(alias = "alias")]
    pub name: String,
    pub data_type: String,
    pub concurrency_limit: usize,
    pub rules: BTreeMap<String, Rule>,
    #[serde(alias = "separator")]
    pub error_separator: String,
    #[serde(skip)]
    pub receiver: Option<Receiver<Context>>,
    #[serde(skip)]
    pub sender: Option<Sender<Context>>,
}

impl Default for Validator {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Validator {
            updater_type: UpdaterType::default(),
            referentials: HashMap::default(),
            name: uuid.simple().to_string(),
            data_type: DataResult::OK.to_string(),
            concurrency_limit: 1,
            rules: BTreeMap::default(),
            error_separator: "\r\n".to_string(),
            receiver: None,
            sender: None,
        }
    }
}

#[async_trait]
impl Step for Validator {
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
    /// This step validate the values of a dataset.
    ///
    /// # Example: simple validations
    /// ```rust
    /// use std::io;
    /// use serde_json::Value;
    /// use json_value_search::Search;
    /// use chewdata::DataResult;
    /// use chewdata::Context;
    /// use chewdata::step::Step;
    /// use chewdata::step::validator::{Validator, Rule};
    /// use std::thread;
    /// use std::collections::{BTreeMap, HashMap};
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let (sender_input, receiver_input) = async_channel::unbounded();
    ///     let (sender_output, receiver_output) = async_channel::unbounded();
    ///
    ///     let mut rules = BTreeMap::default();
    ///     rules.insert("rule_number_1".to_string(), Rule {
    ///         pattern: "{% if input.number_1 is matching('\\d+') %} true {% else %} false {% endif %}".to_string(),
    ///         message: Some("Err N.1".to_string())
    ///     });
    ///
    ///     let validator = Validator {
    ///         rules: rules,
    ///         error_separator: " & ".to_string(),
    ///         receiver: Some(receiver_input),
    ///         sender: Some(sender_output),
    ///         ..Default::default()
    ///     };
    ///
    ///     thread::spawn(move || {
    ///         let data = serde_json::from_str(r#"{"number_1":"my_string","number_2":100,"text":"120"}"#).unwrap();
    ///         let context = Context::new("step_data_loading".to_string(), DataResult::Ok(data));
    ///         sender_input.try_send(context).unwrap();
    ///     });
    ///
    ///     validator.exec().await?;
    ///
    ///     for context in receiver_output.try_recv() {
    ///         let error_result = context.input().to_value().search("/_error").unwrap().unwrap();
    ///         let error_result_expected = Value::String("Err N.1".to_string());
    ///         assert_eq!(error_result_expected, error_result);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "validator::exec",
        skip(self),
        fields(name=self.name, 
        data_type=self.data_type,
        concurrency_limit=self.concurrency_limit,
        error_separator=self.error_separator,
    ))]
    async fn exec(&self) -> io::Result<()> {
        info!("Start validating data...");
    

        let receiver_stream = self.receive().await;

        trace!("Warm up static referential before using it in the concurrent execution.");
        Referential::new(&self.referentials).to_value(&Context::new(String::default(), DataResult::Ok(Value::default()))).await?;

        let actions: Vec<Action> = self
            .rules
            .iter()
            .map(|(rule_name, rule)| Action {
                field: rule_name.clone(),
                pattern: Some(rule.pattern.clone()),
                action_type: ActionType::Replace,
            })
            .collect();

        // Validate in concurrence with parallelism.
        let results: Vec<_> = receiver_stream.map(|context_received| {
            let step = self.clone();
            let actions = actions.clone();
            task::spawn(async move {
            validate(&step, &mut context_received.clone(), &actions.clone()).await
        })}).buffer_unordered(self.concurrency_limit).collect().await;

        results
            .into_iter()
            .filter(|result| result.is_err())
            .map(|result| warn!("{:?}", result))
            .for_each(drop);

        info!(
            "Stops validating and sending context in the channel"
        );

        Ok(())
    }
    fn number(&self) -> usize {
        self.concurrency_limit
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

#[instrument(name = "validator::validate", skip(step, context_received))]
async fn validate(step: &Validator, context_received: &mut Context, actions: &Vec<Action>) -> io::Result<()> {
    let data_result = context_received.input();

    if !data_result.is_type(step.data_type.as_ref()) {
        trace!("Handles only this data type");
        step.send(context_received).await;
        return Ok(());
    }

    let record = data_result.to_value();

    let validator_result = step
        .updater_type
        .updater()
        .update(
            &record,
            &context_received.steps(),
            &Referential::new(&step.referentials).to_value(context_received).await?,
            actions,
        )
        .await
        .and_then(|value| match value {
            Value::Object(_) => Ok(value),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("The validation's result must be a boolean and not '{:?}'", value),
            )),
        })
        .and_then(|value| {
            let mut errors = String::default();

            for (rule_name, rule) in &step.rules {
                let value_result =
                    value.clone().search(rule_name.to_json_pointer().as_str());

                let mut error = match value_result {
                    Ok(Some(Value::Bool(true))) => continue,
                    Ok(Some(Value::Bool(false))) => {
                        rule.message.clone().unwrap_or(format!("The rule '{}' failed", rule_name))
                    }
                    Ok(Some(_)) => format!(
                        "The rule '{}' has invalid result pattern '{:?}', it must be a boolean",
                        rule_name,
                        value_result.unwrap().unwrap()
                    ),
                    Ok(None) => format!(
                        "The rule '{}' is not found in the validation result '{:?}'",
                        rule_name,
                        value_result.unwrap()
                    ),
                    Err(e) => e.to_string(),
                };

                if !errors.is_empty() {
                    errors.push_str(step.error_separator.as_str());
                }

                let mut params = Value::default();
                params.merge_in(&format!("/{}", INPUT_FIELD_KEY), &record)?;
                params.merge_in("/rule/name", &Value::String(rule_name.clone()))?;

                error.replace_mustache(params);

                errors.push_str(error.as_str());
            }

            if !errors.is_empty() {
                Err(Error::new(ErrorKind::InvalidInput, errors))
            } else {
                Ok(record.clone())
            }
        });

    let new_data_result = match validator_result {
        Ok(record) => DataResult::Ok(record),
        Err(e) => DataResult::Err((record.clone(), e)),
    };

    context_received.insert_step_result(step.name(), new_data_result);
    step.send(context_received).await;

    Ok(())
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Rule {
    pub pattern: String,
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[async_std::test]
    async fn exec_with_different_data_result_type() {
        let mut step = Validator::default();
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
        let mut step = Validator::default();
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let data: Value = serde_json::from_str(r#"{"field_1":"value_1"}"#).unwrap();
        let context = Context::new("before".to_string(), DataResult::Ok(data.clone()));

        let mut expected_context = context.clone();
        expected_context.insert_step_result("my_step".to_string(), DataResult::Ok(data));

        thread::spawn(move || {
            sender_input.try_send(context).unwrap();
        });

        step.receiver = Some(receiver_input);
        step.sender = Some(sender_output);
        step.name = "my_step".to_string();
        step.rules = serde_json::from_str(r#"{"rule_1": {"pattern": "true"}}"#).unwrap();
        step.exec().await.unwrap();

        assert_eq!(expected_context, receiver_output.recv().await.unwrap());
    }
    #[async_std::test]
    async fn exec() {
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let mut rules = BTreeMap::default();
        rules.insert(
            "rule_number_1".to_string(),
            Rule {
                pattern:
                    "{% if input.number_1 is matching('\\d+') %} true {% else %} false {% endif %}"
                        .to_string(),
                message: Some("Err N.1".to_string()),
            },
        );
        rules.insert(
            "rule_number_2".to_string(),
            Rule {
                pattern: "{% if input.number_2 < 100 %} true {% else %} false {% endif %}"
                    .to_string(),
                message: Some("Err N.2".to_string()),
            },
        );
        rules.insert(
            "rule_text".to_string(),
            Rule {
                pattern:
                    "{% if input.text is matching('[^\\d]+') %} true {% else %} false {% endif %}"
                        .to_string(),
                message: Some("Err T.1".to_string()),
            },
        );
        let validator = Validator {
            rules: rules,
            error_separator: " & ".to_string(),
            receiver: Some(receiver_input),
            sender: Some(sender_output),
            ..Default::default()
        };
        thread::spawn(move || {
            let data =
                serde_json::from_str(r#"{"number_1":"my_string","number_2":100,"text":"120"}"#)
                    .unwrap();
            let context =
                Context::new("step_data_loading".to_string(), DataResult::Ok(data));
            sender_input.try_send(context).unwrap();
        });
        validator.exec().await.unwrap();
        while let Ok(context) = receiver_output.try_recv() {
            let error_result = context
                .input()
                .to_value()
                .search("/_error")
                .unwrap()
                .unwrap();
            let error_result_expected = Value::String("Err N.1 & Err N.2 & Err T.1".to_string());
            assert_eq!(error_result_expected, error_result);
        }
    }
    #[async_std::test]
    async fn exec_with_validation_error() {
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();
        let mut rules = BTreeMap::default();
        rules.insert(
            "rule_exception".to_string(),
            Rule {
                pattern:
                    "{% if input.number_1 is matching('\\d+') %} true {% else %} false {% endif %}"
                        .to_string(),
                message: Some("Err N.1".to_string()),
            },
        );
        let validator = Validator {
            rules: rules,
            error_separator: " & ".to_string(),
            receiver: Some(receiver_input),
            sender: Some(sender_output),
            ..Default::default()
        };
        thread::spawn(move || {
            let data = serde_json::from_str(r#"{"number":100}"#).unwrap();
            let context =
                Context::new("step_data_loading".to_string(), DataResult::Ok(data));
            sender_input.try_send(context).unwrap();
        });
        validator.exec().await.unwrap();
        while let Ok(context) = receiver_output.try_recv() {
            let error_result = context
                .input()
                .to_value()
                .search("/_error")
                .unwrap()
                .unwrap();
            let error_result_expected = Value::String("Failed to render the field 'rule_exception'. Tester `matching` was called on an undefined variable.".to_string());
            assert_eq!(error_result_expected, error_result);
        }
    }
}