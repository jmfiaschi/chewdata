use super::super::helper::referentials_reader_into_value;
use super::DataResult;
use crate::helper::json_pointer::JsonPointer;
use crate::step::Step;
use crate::updater::{ActionType, UpdaterType};
use crate::StepContext;
use crate::{step::reader::Reader, updater::Action};
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use json_value_search::Search;
use serde::Deserialize;
use serde_json::Value;
use std::io::{Error, ErrorKind};
use std::{collections::{BTreeMap,HashMap}, fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Validator {
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
    pub rules: BTreeMap<String, Rule>,
    #[serde(alias = "input")]
    pub input_name: String,
    #[serde(alias = "output")]
    pub output_name: String,
    pub error_separator: String,
}

impl Default for Validator {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Validator {
            updater_type: UpdaterType::default(),
            referentials: None,
            name: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            thread_number: 1,
            rules: BTreeMap::default(),
            input_name: "input".to_string(),
            output_name: "output".to_string(),
            error_separator: "\r\n".to_string(),
        }
    }
}

impl fmt::Display for Validator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Validator {{'{}','{}' }}",
            self.name,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

#[async_trait]
impl Step for Validator {
    /// This step validate the values of a dataset.
    ///
    /// # Example: simple validations
    /// ```rust
    /// use std::io;
    /// use serde_json::Value;
    /// use json_value_search::Search;
    /// use chewdata::DataResult;
    /// use chewdata::StepContext;
    /// use chewdata::step::Step;
    /// use chewdata::step::validator::{Validator, Rule};
    /// use crossbeam::channel::unbounded;
    /// use std::thread;
    /// use std::collections::{BTreeMap, HashMap};
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let (sender_input, receiver_input) = unbounded();
    ///     let (sender_output, receiver_output) = unbounded();
    ///
    ///     let mut rules = BTreeMap::default();
    ///     rules.insert("rule_number_1".to_string(), Rule {
    ///         pattern: "{% if input.number_1 is matching('\\d+') %} true {% else %} false {% endif %}".to_string(),
    ///         message: Some("Err N.1".to_string())
    ///     });
    ///     rules.insert("rule_number_2".to_string(), Rule {
    ///         pattern: "{% if input.number_2 < 100 %} true {% else %} false {% endif %}".to_string(),
    ///         message: Some("Err N.2".to_string())
    ///     });
    ///     rules.insert("rule_text".to_string(), Rule {
    ///         pattern: "{% if input.text is matching('[^\\d]+') %} true {% else %} false {% endif %}".to_string(),
    ///         message: Some("Err T.1".to_string())
    ///     });
    ///
    ///     let validator = Validator {
    ///         rules: rules,
    ///         error_separator: " & ".to_string(),
    ///         ..Default::default()
    ///     };
    ///
    ///     thread::spawn(move || {
    ///         let data = serde_json::from_str(r#"{"number_1":"my_string","number_2":100,"text":"120"}"#).unwrap();
    ///         let step_context = StepContext::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
    ///         sender_input.send(step_context).unwrap();
    ///     });
    ///     
    ///     validator.exec(Some(receiver_input), Some(sender_output)).await?;
    ///
    ///     for step_context in receiver_output {
    ///         let error_result = step_context.data_result().to_value().search("/_error").unwrap().unwrap();
    ///         let error_result_expected = Value::String("Err N.1 & Err N.2 & Err T.1".to_string());
    ///         assert_eq!(error_result_expected, error_result);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Display one error for pattern render error
    /// ```rust
    /// use std::io;
    /// use serde_json::Value;
    /// use json_value_search::Search;
    /// use chewdata::DataResult;
    /// use chewdata::StepContext;
    /// use chewdata::step::Step;
    /// use chewdata::step::validator::{Validator, Rule};
    /// use crossbeam::channel::unbounded;
    /// use std::thread;
    /// use std::collections::{BTreeMap, HashMap};
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let (sender_input, receiver_input) = unbounded();
    ///     let (sender_output, receiver_output) = unbounded();
    ///
    ///     let mut rules = BTreeMap::default();
    ///     rules.insert("rule_exception".to_string(), Rule {
    ///         pattern: "{% if input.number_1 is matching('\\d+') %} true {% else %} false {% endif %}".to_string(),
    ///         message: Some("Err N.1".to_string())
    ///     });
    ///
    ///     let validator = Validator {
    ///         rules: rules,
    ///         error_separator: " & ".to_string(),
    ///         ..Default::default()
    ///     };
    ///
    ///     thread::spawn(move || {
    ///         let data = serde_json::from_str(r#"{"number":100}"#).unwrap();
    ///         let step_context = StepContext::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
    ///         sender_input.send(step_context).unwrap();
    ///     });
    ///     
    ///     validator.exec(Some(receiver_input), Some(sender_output)).await?;
    ///
    ///     for step_context in receiver_output {
    ///         let error_result = step_context.data_result().to_value().search("/_error").unwrap().unwrap();
    ///         let error_result_expected = Value::String("Failed to render the field 'rule_exception'. Tester `matching` was called on an undefined variable.".to_string());
    ///         assert_eq!(error_result_expected, error_result);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
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
            Some(referentials) => Some(referentials_reader_into_value(referentials).await?),
            None => None,
        };

        let actions: Vec<Action> = self
            .rules
            .clone()
            .into_iter()
            .map(|(rule_name, rule)| Action {
                field: rule_name,
                pattern: Some(rule.pattern),
                action_type: ActionType::Replace,
            })
            .collect();

        for mut step_context_received in receiver {
            let data_result = step_context_received.data_result();

            if !data_result.is_type(self.data_type.as_ref()) {
                trace!("This step handle only this data type");
                continue;
            }

            let record = data_result.to_value();

            let validator_result = self
                .updater_type
                .updater()
                .update(
                    record.clone(),
                    step_context_received.steps_result(),
                    referentials.clone(),
                    actions.clone(),
                    self.input_name.clone(),
                    self.output_name.clone(),
                )
                .and_then(|value| match value {
                    Value::Object(_) => Ok(value),
                    _ => Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!("The validation's result must be an object of boolean and not '{:?}'", value),
                    )),
                })
                .and_then(|value| {
                    let mut errors = String::default();

                    for (rule_name, rule) in self.rules.clone() {
                        let value_result =
                            value.clone().search(rule_name.to_json_pointer().as_str());

                        let error = match value_result {
                            Ok(Some(Value::Bool(true))) => continue,
                            Ok(Some(Value::Bool(false))) => {
                                rule.message.unwrap_or(format!("The rule '{}' failed", rule_name))
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
                            errors.push_str(self.error_separator.as_str());
                        }
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

            step_context_received.insert_step_result(self.name(), new_data_result)?;
            self.send(step_context_received.clone(), &sender)?;
        }

        drop(sender);

        info!("End");
        Ok(())
    }
    fn thread_number(&self) -> usize {
        self.thread_number
    }
    fn name(&self) -> String {
        self.name.clone()
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Rule {
    pub pattern: String,
    pub message: Option<String>,
}
