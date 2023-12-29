use crate::updater;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// See [`updater::tera_helpers::function::object::merge`] for more details.
pub fn merge(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("from".to_string(), value.clone());
    updater::tera_helpers::function::object::merge(&new_args)
}

/// See [`updater::tera_helpers::function::object::search`] for more details.
pub fn search(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("from".to_string(), value.clone());
    updater::tera_helpers::function::object::search(&new_args)
}

/// See [`updater::tera_helpers::function::object::replace_key`] for more details.
pub fn replace_key(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("target".to_string(), value.clone());
    updater::tera_helpers::function::object::replace_key(&new_args)
}

/// See [`updater::tera_helpers::function::object::replace_value`] for more details.
pub fn replace_value(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("target".to_string(), value.clone());
    updater::tera_helpers::function::object::replace_value(&new_args)
}
