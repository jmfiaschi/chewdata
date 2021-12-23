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
use std::{collections::HashMap, fmt, io};
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Validator {
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
    pub rules: HashMap<String, Rule>,
    #[serde(alias = "input")]
    input_name: String,
    #[serde(alias = "output")]
    output_name: String,
    error_separator: String,
}

impl Default for Validator {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        Validator {
            updater_type: UpdaterType::default(),
            referentials: None,
            alias: uuid.to_simple().to_string(),
            description: None,
            data_type: DataResult::OK.to_string(),
            thread_number: 1,
            rules: HashMap::default(),
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
            self.alias,
            self.description
                .to_owned()
                .unwrap_or_else(|| "No description".to_string())
        )
    }
}

/// This Step validate the contain of a dataset.
#[async_trait]
impl Step for Validator {
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
                            Ok(Some(Value::Bool(true))) => String::default(),
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

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Rule {
    pub pattern: String,
    pub message: Option<String>,
}
