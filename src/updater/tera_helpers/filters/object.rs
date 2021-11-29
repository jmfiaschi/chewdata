use json_value_merge::Merge;
use json_value_search::Search;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Merge two Value together.
///
/// # Example: Merge single array of scalar.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let mut array: Vec<Value> = Vec::default();
/// array.push(Value::String("a".to_string()));
/// array.push(Value::String("b".to_string()));
///
/// let obj = Value::Array(array);
/// let args = HashMap::new();
///
/// let result = merge(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(Value::String("b".to_string()), result.unwrap());
/// ```
/// # Example: Merge single array of objects.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let mut array: Vec<Value> = Vec::default();
/// array.push(serde_json::from_str(r#"{"field1":"value1"}"#).unwrap());
/// array.push(serde_json::from_str(r#"{"field2":"value2"}"#).unwrap());
///
/// let obj = Value::Array(array);
/// let args = HashMap::new();
///
/// let result = merge(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"field1":"value1","field2":"value2"}"#).unwrap(),
///     result.unwrap()
/// );
/// ```
/// # Example: Merge one object with another.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let mut obj = Value::default();
/// obj.merge_in("/field", Value::String("value".to_string()));
///
/// let mut with = Value::default();
/// with.merge_in("/other_field", Value::String("other value".to_string()));
///
/// let mut args = HashMap::new();
/// args.insert("with".to_string(), with.clone());
///
/// let result = merge(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
///         .unwrap(),
///     result.unwrap()
/// );
/// ```
/// # Example: Merge one object with another in specific path.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let mut obj = Value::default();
/// obj.merge_in("/field", Value::String("value".to_string()));
///
/// let with = Value::String("other value".to_string());
///
/// let mut args = HashMap::new();
/// args.insert("with".to_string(), with.clone());
/// args.insert("in".to_string(), Value::String("/other_field".to_string()));
///
/// let result = merge(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
///         .unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn merge(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_value = Value::default();
    let with = match args.get("with") {
        Some(val) => Some(try_get_value!("merge", "with", Value, val)),
        None => None,
    };

    let into = match args.get("in") {
        Some(path) => Some(try_get_value!("merge", "in", String, path)),
        None => None,
    };

    let new_value = match (with, into, value) {
        (None, None, Value::Array(values)) => {
            for value in values {
                new_value.merge(value.clone());
            }
            new_value
        }
        (Some(merge_with), None, value) => {
            new_value.merge(value.clone());
            new_value.merge(merge_with);
            new_value
        }
        (Some(merge_with), Some(path), value) => {
            new_value.merge(value.clone());
            new_value.merge_in(path.as_str(), merge_with)?;
            new_value
        }
        (None, Some(_), _value) => {
            return Err(Error::msg(
                "Function `merge` was called without the 'with' argument.",
            ))
        }
        (None, None, _) => {
            return Err(Error::msg(
                "Function `merge` was called without the 'with' argument. Only an array can be merged without argument.",
            ))
        }
    };

    Ok(new_value)
}

/// Search elements in object Value.
///
/// # Example:
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_search::Search;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::filters::object::search;
///
/// let mut obj = Value::default();
/// obj.merge_in("/field_1/field_2", Value::String("value".to_string()));
///
/// let mut args = HashMap::new();
/// args.insert("path".to_string(), Value::String("/field_1".to_string()));
///
/// let result = search(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(serde_json::from_str::<Value>(r#"{"field_2":"value"}"#).unwrap(), result.unwrap());
/// 
/// let mut args = HashMap::new();
/// args.insert("path".to_string(), Value::String("/field_1/field_2".to_string()));
///
/// let result = search(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!("value".to_string(), result.unwrap());
/// 
/// let mut args = HashMap::new();
/// args.insert("path".to_string(), Value::String("/field_1/not_found".to_string()));
///
/// let result = search(&obj, &args);
/// assert!(result.is_ok());
/// assert_eq!(Value::Null, result.unwrap());
/// ```
pub fn search(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let path = match args.get("path") {
        Some(val) => val.as_str().ok_or("Function `search` can't get the `path` argument")?.clone(),
        None => return Err(Error::msg(
            "Function `search` didn't receive a `path` argument",
        ))
    };

    let new_value = match value.clone().search(path)? {
        Some(value) => value.clone(),
        None => Value::Null,
    };

    Ok(new_value)
}
