extern crate tera;

use super::Update;
use super::{Action, ActionType};
use crate::updater::tera_helpers::{filters, function};
use crate::FieldPath;
use json_value_merge::Merge;
use json_value_resolve::Resolve;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::{fmt, io};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct Tera {
    // Use Vec in order to keep the order FIFO.
    actions: Vec<Action>,
    entry_name: String,
    output_name: String,
}

impl Default for Tera {
    fn default() -> Self {
        Tera {
            actions: Vec::default(),
            entry_name: "input".to_string(),
            output_name: "output".to_string(),
        }
    }
}

impl fmt::Display for Tera {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tera {{'{}','{}'}}", self.entry_name, self.output_name)
    }
}

impl Update for Tera {
    fn update(
        &self,
        object: Value,
        mapping: Option<HashMap<String, Vec<Value>>>,
    ) -> io::Result<Value> {
        trace!(slog_scope::logger(), "Update"; "input" => format!("{}", object), "updater" => format!("{}", self));
        let mut engine = Tera::engine();
        let mut context = tera::Context::new();
        context.insert(self.entry_name.clone(), &object);

        if let Some(mapping) = mapping {
            for (field_path, object) in mapping {
                context.insert(&field_path.clone(), &object.clone());
            }
        }

        let mut json_value = Value::default();
        for action in &self.actions {
            trace!(slog_scope::logger(), "Field fetch into the pattern collection"; "field" => &action.field);
            context.insert(self.output_name.clone(), &json_value.clone());

            let mut field_new_value = Value::default();
            let json_pointer = FieldPath::new(action.field.clone()).to_json_pointer();

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
                                e.source().unwrap().source().unwrap().to_string()
                            ),
                        )),
                    }?;
                    trace!(slog_scope::logger(), "Field value before resolved it"; "value" => render_result.to_string());
                    field_new_value = Value::resolve(render_result);
                    trace!(slog_scope::logger(), "Field value after resolved it"; "value" => format!("{}", field_new_value));
                }
                None => (),
            };

            trace!(slog_scope::logger(), "{} the new field", action.action_type;
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
            }
        }

        trace!(slog_scope::logger(), "Update ended"; "output" => format!("{}", json_value));
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

        engine
    }
}
