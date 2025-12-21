extern crate tera;

use super::Updater;
use super::{Action, ActionType};
use crate::helper::json_pointer::JsonPointer;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::updater::tera_helpers::{filters, function};
use async_lock::Mutex;
use async_trait::async_trait;
use json_value_merge::Merge;
use json_value_remove::Remove;
use json_value_resolve::Resolve;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error as StdError;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, OnceLock};
use std::{fmt, io};
use tera::Tera as TeraClient;

static ENGINE: OnceLock<Arc<Mutex<Option<TeraClient>>>> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Tera {}

impl fmt::Display for Tera {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tera {{}}")
    }
}

#[async_trait]
impl Updater for Tera {
    #[instrument(name = "tera::update", skip(self, object, context, mapping, actions))]
    async fn update(
        &self,
        object: &Value,
        context: &Value,
        mapping: &Value,
        actions: &[Action],
    ) -> io::Result<Value> {
        trace!("Update start...");

        let mut engine = self.engine().await;

        let mut context_value = Value::default();
        context_value.merge_in(format!("/{}", super::INPUT_FIELD_KEY).as_str(), object)?;
        context_value.merge_in(format!("/{}", super::CONTEXT_FIELD_KEY).as_str(), context)?;

        match mapping {
            Value::Object(_) => context_value.merge(mapping),
            Value::Null => (),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "The mapping value must be an object",
                ))
            }
        }

        let mut tera_context = match context_value {
            Value::Null => tera::Context::new(),
            _ => tera::Context::from_value(context_value).unwrap(),
        };

        let mut output = Value::default();
        for action in actions {
            trace!(
                output = output.display_only_for_debugging(),
                "Current output"
            );

            tera_context.insert(super::OUPUT_FIELD_KEY, &output);

            let mut field_new_value = Value::default();

            match &action.pattern {
                Some(pattern) => {
                    trace!(
                        field = action.field.as_str(),
                        pattern = pattern,
                        "Field/Pattern that will be apply on the output"
                    );

                    let render_result: String =
                        match engine.render_str(pattern.as_str(), &tera_context) {
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
                                        None => format!("Please fix the pattern `{}`", pattern),
                                    }
                                    .replace(" '__tera_one_off'", "")
                                ),
                            )),
                        }?;

                    trace!(
                        result = render_result.display_only_for_debugging(),
                        field = action.field.as_str(),
                        pattern = pattern,
                        context = tera_context.display_only_for_debugging(),
                        output = output.display_only_for_debugging(),
                        "Field value before resolved it"
                    );

                    field_new_value = Value::resolve(render_result);

                    trace!(
                        result = field_new_value.display_only_for_debugging(),
                        field = action.field.as_str(),
                        pattern = pattern,
                        context = tera_context.display_only_for_debugging(),
                        output = output.display_only_for_debugging(),
                        "Field value after resolved it"
                    );
                }
                None => (),
            };

            let json_pointer = action.field.to_json_pointer();

            trace!(
                output = output.display_only_for_debugging(),
                jpointer = json_pointer.to_string().as_str(),
                result = field_new_value.display_only_for_debugging(),
                context = tera_context.display_only_for_debugging(),
                output = output.display_only_for_debugging(),
                "{:?} the new field",
                action.action_type
            );

            match action.action_type {
                ActionType::Merge => {
                    output.merge_in(&json_pointer, &field_new_value)?;
                }
                ActionType::Replace => {
                    output.merge_in(&json_pointer, &Value::Null)?;
                    output.merge_in(&json_pointer, &field_new_value)?;
                }
                ActionType::Remove => {
                    output.remove(&json_pointer)?;
                }
            }
        }

        trace!(
            output = output.display_only_for_debugging(),
            "Output updated with success"
        );

        Ok(output)
    }
}

impl Tera {
    pub async fn engine(&self) -> tera::Tera {
        let arc = ENGINE.get_or_init(|| Arc::new(Mutex::new(None)));

        if let Some(engine) = arc.lock().await.clone() {
            return engine;
        }

        let mut guard = arc.lock_arc().await;

        let mut engine = tera::Tera::default();
        engine.autoescape_on(vec![]);
        // register new filter
        engine.register_filter("merge", filters::object::merge);
        engine.register_filter("replace_key", filters::object::replace_key);
        engine.register_filter("replace_value", filters::object::replace_value);
        engine.register_function("uuid_v4", function::string::uuid_v4);
        engine.register_function("base64_encode", function::string::base64_encode);
        engine.register_filter("base64_encode", filters::string::base64_encode);
        engine.register_function("base64_decode", function::string::base64_decode);
        engine.register_filter("base64_decode", filters::string::base64_decode);
        engine.register_filter("search", filters::object::search);
        engine.register_filter("env", filters::string::set_env);
        engine.register_function("env", function::string::env);
        engine.register_function("get_env", function::string::env);
        engine.register_function("find", function::string::find);
        engine.register_filter("find", filters::string::find);
        engine.register_filter("extract", filters::object::extract);
        engine.register_filter("values", filters::object::values);
        engine.register_filter("keys", filters::object::keys);
        engine.register_filter("update", filters::object::update);
        engine.register_filter("map", filters::object::map);

        // faker
        engine.register_function("fake_words", function::faker::words);
        engine.register_function("fake_sentences", function::faker::sentences);
        engine.register_function("fake_paragraphs", function::faker::paragraphs);
        engine.register_function("fake_first_name", function::faker::first_name);
        engine.register_function("fake_last_name", function::faker::last_name);
        engine.register_function("fake_title", function::faker::title);
        engine.register_function("fake_job_seniority", function::faker::job_seniority);
        engine.register_function("fake_job_field", function::faker::job_field);
        engine.register_function("fake_job_position", function::faker::job_position);
        engine.register_function("fake_city", function::faker::city);
        engine.register_function("fake_country_name", function::faker::country_name);
        engine.register_function("fake_country_code", function::faker::country_code);
        engine.register_function("fake_street_name", function::faker::street_name);
        engine.register_function("fake_state_name", function::faker::state_name);
        engine.register_function("fake_state_code", function::faker::state_code);
        engine.register_function("fake_zipcode", function::faker::zipcode);
        engine.register_function("fake_postcode", function::faker::postcode);
        engine.register_function("fake_timezone", function::faker::timezone);
        engine.register_function("fake_latitude", function::faker::latitude);
        engine.register_function("fake_longitude", function::faker::longitude);
        engine.register_function("fake_profession", function::faker::profession);
        engine.register_function("fake_industry", function::faker::industry);
        engine.register_function("fake_email", function::faker::email);
        engine.register_function("fake_ipv4", function::faker::ipv4);
        engine.register_function("fake_ipv6", function::faker::ipv6);
        engine.register_function("fake_mac_address", function::faker::mac_address);
        engine.register_function("fake_color_hex", function::faker::color_hex);
        engine.register_function("fake_user_agent", function::faker::user_agent);
        engine.register_function("fake_digit", function::faker::digit);
        engine.register_function("fake_phone_number", function::faker::phone_number);
        engine.register_function("fake_currency_name", function::faker::currency_name);
        engine.register_function("fake_currency_code", function::faker::currency_code);
        engine.register_function("fake_currency_symbol", function::faker::currency_symbol);
        engine.register_function("fake_credit_card", function::faker::credit_card);
        engine.register_function("fake_barcode", function::faker::barcode);
        engine.register_function("fake_password", function::faker::password);

        *guard = Some(engine.clone());

        engine
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_create_input() {
        let input = Value::Null;
        let context = Value::Null;
        let mapping = Value::Null;
        let actions = &vec![Action {
            field: "input_field".to_string(),
            pattern: Some("input_value".to_string()),
            action_type: ActionType::Merge,
        }];

        let tera = Tera::default();
        let result = tera.update(&input, &context, &mapping, &actions).await;

        assert!(result.is_ok());
        assert_eq!(json!({"input_field": "input_value"}), result.unwrap());
    }
    #[apply(test!)]
    async fn test_update_input() {
        let input = json!(10);
        let context = Value::Null;
        let mapping = Value::Null;
        let actions = &vec![Action {
            field: "input_field".to_string(),
            pattern: Some("{{ input * 10 }}".to_string()),
            action_type: ActionType::Merge,
        }];

        let tera = Tera::default();
        let result = tera.update(&input, &context, &mapping, &actions).await;

        assert!(result.is_ok());
        assert_eq!(json!({"input_field": 100}), result.unwrap());
    }
    #[apply(test!)]
    async fn test_update_with_mapping_failing() {
        let input = Value::Null;
        let context = Value::Null;
        let mapping = Value::String("".to_string());
        let actions = &vec![Action {
            field: "field".to_string(),
            pattern: Some("value".to_string()),
            action_type: ActionType::Merge,
        }];

        let tera = Tera::default();
        let result = tera.update(&input, &context, &mapping, &actions).await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "The mapping value must be an object"
        );
    }
}
