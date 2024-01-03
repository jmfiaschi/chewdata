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
#[serde(default, deny_unknown_fields)]
pub struct Tera {}

impl fmt::Display for Tera {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tera {{}}")
    }
}

impl Updater for Tera {
    #[instrument(name = "tera::update")]
    fn update(
        &self,
        object: &Value,
        context: &Value,
        mapping: &Option<HashMap<String, Vec<Value>>>,
        actions: &[Action],
    ) -> io::Result<Value> {
        let mut engine = Tera::engine();
        let mut tera_context = tera::Context::new();

        tera_context.insert(super::INPUT_FIELD_KEY, &object);
        tera_context.insert(super::CONTEXT_FIELD_KEY, &context);

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
            tera_context.insert(super::OUPUT_FIELD_KEY, &json_value.clone());

            let mut field_new_value = Value::default();

            match &action.pattern {
                Some(pattern) => {
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

            let json_pointer = action.field.to_json_pointer();

            trace!(
                output = format!("{}", json_value).as_str(),
                jpointer = json_pointer.to_string().as_str(),
                data = format!("{}", field_new_value).as_str(),
                "{:?} the new field",
                action.action_type
            );

            match action.action_type {
                ActionType::Merge => {
                    json_value.merge_in(&json_pointer, &field_new_value)?;
                }
                ActionType::Replace => {
                    json_value.merge_in(&json_pointer, &Value::Null)?;
                    json_value.merge_in(&json_pointer, &field_new_value)?;
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

        engine
    }
}
