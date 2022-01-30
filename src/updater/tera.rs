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
    #[instrument]
    fn update(
        &self,
        object: Value,
        context: Value,
        mapping: Option<HashMap<String, Vec<Value>>>,
        actions: Vec<Action>,
        input_name: String,
        output_name: String,
    ) -> io::Result<Value> {
        let mut engine = Tera::engine();
        let mut tera_context = tera::Context::new();
        tera_context.insert(input_name, &object);
        tera_context.insert("context", &context);

        if let Some(mapping) = mapping {
            for (field_path, object) in mapping {
                tera_context.insert(&field_path.clone(), &object.clone());
            }
        }

        let mut json_value = Value::default();
        for action in actions {
            trace!(
                field = action.field.as_str(),
                "Field fetch into the pattern collection"
            );
            tera_context.insert(output_name.clone(), &json_value.clone());

            let mut field_new_value = Value::default();

            match &action.pattern {
                Some(pattern) => {
                    let render_result: String = match engine
                        .render_str(pattern.as_str(), &tera_context)
                    {
                        Ok(render_result) => Ok(render_result),
                        Err(e) => Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "Failed to render the field '{}'. {}.",
                                action.field,
                                match e.source() {
                                    Some(e) => {
                                        match e.source() {
                                            Some(e) => e.to_string(),
                                            None => e.to_string(),
                                        }
                                    }
                                    None =>
                                        format!("Please fix the pattern `{}`", pattern),
                                }
                                .replace(" '__tera_one_off'", "")
                            ),
                        )),
                    }?;
                    trace!(
                        value = render_result.as_str(),
                        "Field value before resolved it"
                    );
                    field_new_value = Value::resolve(render_result);
                    trace!(
                        value = format!("{}", field_new_value).as_str(),
                        "Field value after resolved it"
                    );
                }
                None => (),
            };

            let json_pointer = action.field.clone().to_json_pointer();

            trace!(
                output = format!("{}", json_value).as_str(),
                jpointer = json_pointer.to_string().as_str(),
                data = format!("{}", field_new_value).as_str(),
                "{} the new field",
                action.action_type
            );

            match action.action_type {
                ActionType::Merge => {
                    json_value.merge_in(&json_pointer, field_new_value)?;
                }
                ActionType::Replace => {
                    json_value.merge_in(&json_pointer, Value::Null)?;
                    json_value.merge_in(&json_pointer, field_new_value)?;
                }
                ActionType::Remove => {
                    json_value.remove(&json_pointer)?;
                }
            }
        }

        trace!(output = format!("{}", json_value).as_str(), "Update ended");
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
        engine.register_function("base64_encode", function::base64_encode);
        engine.register_function("base64_decode", function::base64_decode);
        engine.register_filter("search", filters::object::search);

        engine
    }
}
