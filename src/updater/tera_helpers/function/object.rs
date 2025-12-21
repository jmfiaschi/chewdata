use json_value_resolve::Resolve;
use regex::Regex;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

use crate::helper::{
    json_pointer::JsonPointer,
    value::{Extract, MergeAndReplace},
};

/// Replace object key name by another.
///
/// # Arguments
///
/// * `target` - the object on which the transformation will be applied.
/// * `from` - The key to replace. Can be a regular expression.
/// * `to` - The new key.
/// * `level` - The depth level to apply the replacement.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::object::replace_key;
///
/// let target = serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();
/// let mut args = HashMap::new();
/// args.insert("target".to_string(), target);
/// args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
/// args.insert("to".to_string(), Value::String("@$1".to_string()));
///
/// let result = replace_key(&args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"@field_1":"value_1","field_2":"value_1"}"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn replace_key(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'target' argument
    let mut target = args
        .get("target")
        .ok_or_else(|| Error::msg("Function `replace_key` didn't receive a `target` argument"))
        .and_then(|val| Ok(try_get_value!("replace_key", "target", Value, val)))?;

    // Extracting and validating the 'from' argument
    let from = args
        .get("from")
        .ok_or_else(|| Error::msg("Function `replace_key` didn't receive a `from` argument"))
        .and_then(|val| Ok(try_get_value!("replace_key", "from", String, val)))?;

    // Extracting and validating the 'to' argument
    let to = args
        .get("to")
        .ok_or_else(|| Error::msg("Function `replace_key` didn't receive a `to` argument"))
        .and_then(|val| Ok(try_get_value!("replace_key", "to", String, val)))?;

    // Extracting and validating the 'to' argument
    let level = match args.get("level") {
        Some(level) => try_get_value!("replace_value", "level", usize, level),
        None => 0,
    };

    replace_key_recursively(&mut target, &from, &to, level, 0)?;

    Ok(target)
}

/// Replace object value by another.
///
/// # Arguments
///
/// * `target` - the object on which the transformation will be applied.
/// * `from` - The value to replace. Can be a regular expression.
/// * `to` - The new value.
/// * `level` - The depth level to apply the replacement.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::object::replace_value;
///
/// let target = serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();

/// let mut args = HashMap::new();
/// args.insert("target".to_string(), target);
/// args.insert("from".to_string(), Value::String("^(value_1)$".to_string()));
/// args.insert("to".to_string(), Value::String("@$1".to_string()));
///
/// let result = replace_value(&args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"field_1":"@value_1","field_2":"@value_1"}"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn replace_value(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'target' argument
    let mut target = args
        .get("target")
        .ok_or_else(|| Error::msg("Function `replace_value` didn't receive a `target` argument"))
        .and_then(|val| Ok(try_get_value!("replace_value", "target", Value, val)))?;

    // Extracting and validating the 'from' argument
    let from = args
        .get("from")
        .ok_or_else(|| Error::msg("Function `replace_value` didn't receive a `from` argument"))
        .and_then(|val| Ok(try_get_value!("replace_value", "from", String, val)))?;

    // Extracting and validating the 'to' argument
    let to = args
        .get("to")
        .ok_or_else(|| Error::msg("Function `replace_value` didn't receive a `to` argument"))
        .and_then(|val| Ok(try_get_value!("replace_value", "to", String, val)))?;

    let level = match args.get("level") {
        Some(level) => try_get_value!("replace_value", "level", usize, level),
        None => 0,
    };

    replace_value_recursively(&mut target, &from, &to, level, 0)?;

    Ok(target)
}

fn replace_key_recursively(
    target: &mut Value,
    from: &str,
    to: &str,
    level: usize,
    current_level: usize,
) -> Result<()> {
    if level > 0 && level <= current_level {
        return Ok(());
    }

    let re = Regex::new(from).map_err(Error::msg)?;
    match target {
        Value::Object(map) => {
            let new_array: Map<String, Value> = map
                .iter()
                .map(|(key, value)| {
                    let new_key = re.replace(key, to).to_string();

                    let mut cloned_value = value.clone();
                    replace_key_recursively(&mut cloned_value, from, to, level, current_level + 1)?;

                    Ok((new_key, cloned_value))
                })
                .collect::<Result<_>>()?;

            *map = new_array;
        }
        Value::Array(array) => {
            for value in array {
                replace_key_recursively(value, from, to, level, current_level + 1)?;
            }
        }
        _ => (),
    };

    Ok(())
}

fn replace_value_recursively(
    target: &mut Value,
    from: &str,
    to: &str,
    level: usize,
    current_level: usize,
) -> Result<()> {
    if level > 0 && level <= current_level {
        return Ok(());
    }

    let re = Regex::new(from).map_err(Error::msg)?;

    match target {
        Value::Object(map) => {
            for (_, value) in map {
                replace_value_recursively(value, from, to, level, current_level + 1)?;
            }
        }
        Value::Array(array) => {
            for value in array {
                replace_value_recursively(value, from, to, level, current_level + 1)?;
            }
        }
        Value::Bool(bool_val) => {
            let result = re.replace(&bool_val.to_string(), to).to_string();
            *target = Value::resolve(result);
        }
        Value::Null => {
            let result = re.replace("null", to).to_string();
            *target = Value::resolve(result);
        }
        Value::Number(number) => {
            let result = re.replace(&number.to_string(), to).to_string();
            *target = Value::resolve(result);
        }
        Value::String(string) => {
            let result = re.replace(string, to).to_string();
            *target = Value::resolve(result);
        }
    }

    Ok(())
}

/// Extract from a list of object, the attributes with the values. Keep the object structure.
///
/// # Arguments
///
/// * `from` - The list of objects or an object.
/// * `attributes` - The list of attribute to extract. Accept regular expression in the attribute names.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::object::extract;
///
/// let from = serde_json::from_str::<Value>(r#"[{"field1_1":{"field1_2":"value1_1"}},{"field2_1":{"field2_2":"value2_1"}}]"#).unwrap();
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), from);
/// args.insert("attributes".to_string(), Value::Array(vec![Value::String("field1_1.field1_2".to_string())]));
///
/// let result = extract(&args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"[{"field1_1":{"field1_2":"value1_1"}}]"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn extract(args: &HashMap<String, Value>) -> Result<Value> {
    let extract_attributes = |value: &Value, attributes: &Vec<String>| -> Result<Value> {
        let mut new_value = Value::default();
        for attribute in attributes {
            let attribute_json_pointer = attribute.to_json_pointer();
            let value_extracted = value.extract(&attribute_json_pointer)?;

            if let Value::Null = value_extracted {
                continue;
            }

            new_value.merge_replace(&value_extracted);
        }
        Ok(new_value)
    };

    // Extracting and validating the 'attributes' argument
    let attributes = args
        .get("attributes")
        .ok_or_else(|| Error::msg("Function `extract` didn't receive an `attributes` argument"))
        .and_then(|val| Ok(try_get_value!("extract", "attributes", Vec<String>, val)))?;

    args.get("from")
        .ok_or_else(|| Error::msg("Function `extract` didn't receive an `from` argument"))
        .and_then(|val| match val {
            Value::Array(vec) => {
                let mut result = Vec::default();
                for value in vec {
                    let new_value = extract_attributes(value, &attributes)?;

                    if new_value != Value::default() {
                        result.append(&mut vec![new_value]);
                    }
                }
                Ok(Value::Array(result))
            }
            Value::Object(_) => extract_attributes(val, &attributes),
            _ => Ok(Value::Null),
        })
}

// Returns all values of an array.
pub fn values(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let value = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `values` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("values", "value", Value, val)))?
        .clone();

    match value {
        Value::Array(arr) => Ok(to_value(arr.clone()).unwrap()),
        Value::Object(obj) => {
            let values: Vec<Value> = obj.values().cloned().collect();
            Ok(to_value(values).unwrap())
        }
        _ => Ok(value.clone()),
    }
}

// Returns all keys of an array.
pub fn keys(args: &HashMap<String, Value>) -> Result<Value> {
    let value = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `keys` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("keys", "value", Value, val)))?
        .clone();

    match value {
        Value::Array(arr) => {
            let keys: Vec<Value> = (0..arr.len()).map(|i| Value::Number(i.into())).collect();
            Ok(to_value(keys).unwrap())
        }
        Value::Object(obj) => {
            let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.to_string())).collect();
            Ok(to_value(keys).unwrap())
        }
        _ => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_key() {
        let target =
            serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();

        let mut args = HashMap::new();
        args.insert("target".to_string(), target);
        args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = replace_key(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"@field_1":"value_1","field_2":"value_1"}"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_replace_key_with_array() {
        let target =
            serde_json::from_str::<Value>(r#"[{"field_1":"value_1","field_2":"value_1"}]"#)
                .unwrap();

        let mut args = HashMap::new();
        args.insert("target".to_string(), target);
        args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = replace_key(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"@field_1":"value_1","field_2":"value_1"}]"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_replace_value() {
        let target =
            serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();

        let mut args = HashMap::new();
        args.insert("target".to_string(), target);
        args.insert("from".to_string(), Value::String("^(value_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = replace_value(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field_1":"@value_1","field_2":"@value_1"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_replace_value_with_array() {
        let target =
            serde_json::from_str::<Value>(r#"[{"field_1":"value_1","field_2":"value_1"}]"#)
                .unwrap();
        let mut args = HashMap::new();
        args.insert("target".to_string(), target);
        args.insert("from".to_string(), Value::String("^(value_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = replace_value(&args);

        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"field_1":"@value_1","field_2":"@value_1"}]"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_extract_on_array() {
        let from = serde_json::from_str::<Value>(
            r#"[{"field1_1":{"field1_2":"value1_1"}},{"field2_1":{"field2_2":"value2_1"}}]"#,
        )
        .unwrap();

        // Extract one attribute.
        let mut args = HashMap::new();
        args.insert("from".to_string(), from.clone());
        args.insert(
            "attributes".to_string(),
            Value::Array(vec![Value::String("field1_1.field1_2".to_string())]),
        );

        let result = extract(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"field1_1":{"field1_2":"value1_1"}}]"#).unwrap(),
            result.unwrap()
        );

        // Extract two attributes.
        let mut args = HashMap::new();
        args.insert("from".to_string(), from.clone());
        args.insert(
            "attributes".to_string(),
            Value::Array(vec![
                Value::String("field1_1.field1_2".to_string()),
                Value::String("field2_1.field2_2".to_string()),
            ]),
        );

        let result = extract(&args);
        assert!(result.is_ok());
        assert_eq!(from, result.unwrap());
    }
    #[test]
    fn test_extract_on_object() {
        let from = serde_json::from_str::<Value>(
            r#"{"field1_1":{"field1_2":"value1_1"},"field2_1":{"field2_2":"value2_1"}}"#,
        )
        .unwrap();

        // Extract one attribute.
        let mut args = HashMap::new();
        args.insert("from".to_string(), from.clone());
        args.insert(
            "attributes".to_string(),
            Value::Array(vec![Value::String("field1_1.field1_2".to_string())]),
        );

        let result = extract(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field1_1":{"field1_2":"value1_1"}}"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_values_from_array() {
        let arr = to_value(vec![
            serde_json::json!({"a": 1}),
            serde_json::json!({"b": 2}),
            serde_json::json!({"c": 3}),
        ])
        .unwrap();

        let mut args = HashMap::new();
        args.insert("from".to_string(), arr.clone());

        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(values_result, arr);
        assert_eq!(keys_result, to_value(vec![0, 1, 2]).unwrap());
    }
    #[test]
    fn test_keys_from_array() {
        let arr = to_value(vec![
            serde_json::json!({"a": 1}),
            serde_json::json!({"b": 2}),
            serde_json::json!({"c": 3}),
        ])
        .unwrap();

        let mut args = HashMap::new();
        args.insert("from".to_string(), arr.clone());

        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(values_result, arr);
        assert_eq!(keys_result, to_value(vec![0, 1, 2]).unwrap());
    }
    #[test]
    fn test_keys_from_object() {
        let obj = serde_json::from_str::<Value>(r#"{"a":1,"b":2,"c":3}"#).unwrap();
        let mut args = HashMap::new();
        args.insert("from".to_string(), obj.clone());
        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(
            values_result,
            to_value(vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3)
            ])
            .unwrap()
        );
        assert_eq!(
            keys_result,
            to_value(vec![
                serde_json::json!("a"),
                serde_json::json!("b"),
                serde_json::json!("c")
            ])
            .unwrap()
        );
    }
    #[test]
    fn test_keys_from_other_type() {
        let val = serde_json::from_str::<Value>(r#""a string""#).unwrap();
        let mut args = HashMap::new();
        args.insert("from".to_string(), val.clone());
        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(values_result, val);
        assert_eq!(keys_result, Value::Null);
    }
    #[test]
    fn test_values_from_other_type() {
        let val = serde_json::from_str::<Value>(r#""a string""#).unwrap();
        let mut args = HashMap::new();
        args.insert("from".to_string(), val.clone());
        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(values_result, val);
        assert_eq!(keys_result, Value::Null);
    }
    #[test]
    fn test_values_from_object() {
        let obj = serde_json::from_str::<Value>(r#"{"a":1,"b":2,"c":3}"#).unwrap();
        let mut args = HashMap::new();
        args.insert("from".to_string(), obj.clone());
        let values_result = values(&args).unwrap();
        let keys_result = keys(&args).unwrap();
        assert_eq!(
            values_result,
            to_value(vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3)
            ])
            .unwrap()
        );
        assert_eq!(
            keys_result,
            to_value(vec![
                serde_json::json!("a"),
                serde_json::json!("b"),
                serde_json::json!("c")
            ])
            .unwrap()
        );
    }
}
