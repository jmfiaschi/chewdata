extern crate tera;

use super::Updater;
use super::{Action, ActionType};
use crate::helper::json_pointer::JsonPointer;
use crate::updater::tera_helpers::{filters, function};
use json_value_merge::Merge;
use json_value_remove::Remove;
use json_value_resolve::Resolve;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::{fmt, io};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct Tera {}

impl fmt::Display for Tera {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tera {{}}")
    }
}

impl Updater for Tera {
    fn update(
        &self,
        object: Value,
        mapping: Option<HashMap<String, Vec<Value>>>,
        actions: Vec<Action>,
        input_name: String,
        output_name: String,
    ) -> io::Result<Value> {
        debug!(slog_scope::logger(), "Update"; "input" => format!("{}", object), "updater" => format!("{}", self));
        let mut engine = Tera::engine();
        let mut context = tera::Context::new();
        context.insert(input_name, &object);

        if let Some(mapping) = mapping {
            for (field_path, object) in mapping {
                context.insert(&field_path.clone(), &object.clone());
            }
        }

        let mut json_value = Value::default();
        for action in actions {
            debug!(slog_scope::logger(), "Field fetch into the pattern collection"; "field" => &action.field);
            context.insert(output_name.clone(), &json_value.clone());

            let mut field_new_value = Value::default();

            match &action.pattern {
                Some(pattern) => {
                    let render_result: String = match engine.render_str(pattern.as_str(), &context)
                    {
                        Ok(render_result) => Ok(render_result),
                        Err(e) => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Failed to render the field '{}'. {}",
                                action.field,
                                match e.source() {
                                    Some(e) => {
                                        match e.source() {
                                                Some(e) => e.to_string(),
                                                None => "Pattern not found. Error during the evaluation of the pattern, check if it exist.".to_string(),
                                            }
                                    }
                                    None => "".to_string(),
                                }
                            ),
                        )),
                    }?;
                    debug!(slog_scope::logger(), "Field value before resolved it"; "value" => render_result.to_string());
                    field_new_value = Value::resolve(render_result);
                    debug!(slog_scope::logger(), "Field value after resolved it"; "value" => format!("{}", field_new_value));
                }
                None => (),
            };

            let json_pointer = action.field.clone().to_json_pointer();

            debug!(slog_scope::logger(), "{} the new field", action.action_type;
                "output" => format!("{}", json_value),
                "jpointer" => json_pointer.to_string(),
                "data to add" => format!("{}", field_new_value)
            );

            match action.action_type {
                ActionType::Merge => {
                    json_value.merge_in(&json_pointer, field_new_value);
                }
                ActionType::Replace => {
                    json_value.merge_in(&json_pointer, Value::Null);
                    json_value.merge_in(&json_pointer, field_new_value);
                }
                ActionType::Remove => {
                    json_value.remove(&json_pointer)?;
                }
            }
        }

        debug!(slog_scope::logger(), "Update ended"; "output" => format!("{}", json_value));
        Ok(json_value)
    }
}

impl Tera {
    fn engine() -> tera::Tera {
        let mut engine = tera::Tera::default();

        engine.autoescape_on(vec![]);
        // register new filter
        engine.register_filter("merge", filters::object::merge);
        engine.register_function("uuid_v4", function::uuid_v4);
        engine.register_function("set_env", function::set_env);
        engine.register_function("base64_encode", function::base64_encode);
        engine.register_function("base64_decode", function::base64_decode);

        engine
    }
}
