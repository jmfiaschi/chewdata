use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

use crate::updater;

/// See [`updater::tera_helpers::function::string::base64_encode`] for more details.
pub fn base64_encode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::string::base64_encode(&new_args)
}

/// See [`updater::tera_helpers::function::string::base64_decode`] for more details.
pub fn base64_decode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::string::base64_decode(&new_args)
}

/// See [`updater::tera_helpers::function::string::set_env`] for more details.
pub fn set_env(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::string::set_env(&new_args)
}

/// See [`updater::tera_helpers::function::string::find`] for more details.
pub fn find(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::string::find(&new_args)
}
