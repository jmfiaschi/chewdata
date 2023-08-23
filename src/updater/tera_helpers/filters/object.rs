use json_value_merge::Merge;
use json_value_resolve::Resolve;
use json_value_search::Search;
use regex::Regex;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Merge two Value together.
///
/// # Examples
///
/// ```no_run
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
/// # Examples
///
/// ```no_run
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
        Some(val) => val
            .as_str()
            .ok_or("Function `search` can't get the `path` argument")?,
        None => {
            return Err(Error::msg(
                "Function `search` didn't receive a `path` argument",
            ))
        }
    };

    let new_value = match value.clone().search(path)? {
        Some(value) => value,
        None => Value::Null,
    };

    Ok(new_value)
}

pub fn replace_key(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let to_opt = match args.get("to") {
        Some(key) => Some(try_get_value!("replace", "to", String, key)),
        None => None,
    };

    let from_opt = match args.get("from") {
        Some(pattern) => Some(try_get_value!("replace", "from", String, pattern)),
        None => None,
    };

    let new_value = match (&from_opt, &to_opt) {
        (Some(from), Some(to)) => {
            let mut new_map = Map::default();
            match value {
                Value::Object(map) => {
                    let re = Regex::new(from.as_str()).map_err(Error::msg)?;

                    for (key, value_inner) in map {
                        new_map.insert(
                            re.replace(key.as_str(), to).to_string().clone(),
                            value_inner.clone(),
                        );
                    }
                    Value::Object(new_map)
                }
                Value::Array(array) => Value::Array(
                    array
                        .iter()
                        .map(|array_value| self::replace_key(array_value, args))
                        .collect::<Result<Vec<Value>>>()?
                ),
                _ => {
                    return Err(Error::msg(
                        "Function `replace_key` works only on `object` and `array`. Number, Null, Bool, String are not handled by this method.",
                    ))
                }
            }
        }
        (None, _) | (_, None) => {
            return Err(Error::msg(
                "Function `replace_key` was called without the 'from' or 'to' arguments.",
            ))
        }
    };

    Ok(new_value)
}

pub fn replace_value(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let to_opt = match args.get("to") {
        Some(key) => Some(try_get_value!("replace", "to", String, key)),
        None => None,
    };

    let from_opt = match args.get("from") {
        Some(pattern) => Some(try_get_value!("replace", "from", String, pattern)),
        None => None,
    };

    let new_value = match (&from_opt, &to_opt) {
        (Some(from), Some(to)) => {
            let mut new_map = Map::default();
            match value {
                Value::Object(map) => {
                    let re = Regex::new(from.as_str()).map_err(Error::msg)?;

                    for (key, value_inner) in map {
                        new_map.insert(
                            key.clone(),
                            match value_inner {
                                Value::Array(array) => Value::Array(
                                    array
                                        .iter()
                                        .map(|array_value| self::replace_value(array_value, args))
                                        .collect::<Result<Vec<Value>>>()?,
                                ),
                                Value::Object(_) => value_inner.clone(),
                                Value::String(string) => Value::String(
                                    re.replace(string.as_str(), to).to_string().clone(),
                                ),
                                Value::Bool(bool) => Value::resolve(
                                    re.replace(format!("{}", bool).as_str(), to)
                                        .to_string()
                                        .clone(),
                                ),
                                Value::Null => value_inner.clone(),
                                Value::Number(number) => Value::resolve(
                                    re.replace(format!("{}", number).as_str(), to)
                                        .to_string()
                                        .clone(),
                                ),
                            },
                        );
                    }
                    Value::Object(new_map)
                }
                Value::Array(array) => Value::Array(
                    array
                        .iter()
                        .map(|array_value| self::replace_value(array_value, args))
                        .collect::<Result<Vec<Value>>>()?
                ),
                _ => {
                    return Err(Error::msg(
                        "Function `replace_value` worked only on `object` and `array`.",
                    ))
                }
            }
        }
        (None, _) | (_, None) => {
            return Err(Error::msg(
                "Function `replace_value` was called without the `from` or `to` arguments.",
            ))
        }
    };

    Ok(new_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_array_of_scalar() {
        let mut array: Vec<Value> = Vec::default();
        array.push(Value::String("a".to_string()));
        array.push(Value::String("b".to_string()));
        let obj = Value::Array(array);
        let args = HashMap::new();
        let result = super::merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(Value::String("b".to_string()), result.unwrap());
    }
    #[test]
    fn merge_array_of_object() {
        let mut array: Vec<Value> = Vec::default();
        array.push(serde_json::from_str(r#"{"field1":"value1"}"#).unwrap());
        array.push(serde_json::from_str(r#"{"field2":"value2"}"#).unwrap());
        let obj = Value::Array(array);
        let args = HashMap::new();
        let result = super::merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field1":"value1","field2":"value2"}"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn merge_objects() {
        let mut obj = Value::default();
        obj.merge_in("/field", Value::String("value".to_string()))
            .unwrap();
        let mut with = Value::default();
        with.merge_in("/other_field", Value::String("other value".to_string()))
            .unwrap();
        let mut args = HashMap::new();
        args.insert("with".to_string(), with.clone());
        let result = super::merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn merge_objects_with_path() {
        let mut obj = Value::default();
        obj.merge_in("/field", Value::String("value".to_string()))
            .unwrap();
        let with = Value::String("other value".to_string());
        let mut args = HashMap::new();
        args.insert("with".to_string(), with.clone());
        args.insert("in".to_string(), Value::String("/other_field".to_string()));
        let result = super::merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn search() {
        let mut obj = Value::default();
        obj.merge_in("/field_1/field_2", Value::String("value".to_string()))
            .unwrap();
        let mut args = HashMap::new();
        args.insert("path".to_string(), Value::String("/field_1".to_string()));
        let result = super::search(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field_2":"value"}"#).unwrap(),
            result.unwrap()
        );
        let mut args = HashMap::new();
        args.insert(
            "path".to_string(),
            Value::String("/field_1/field_2".to_string()),
        );
        let result = super::search(&obj, &args);
        assert!(result.is_ok());
        assert_eq!("value".to_string(), result.unwrap());
        let mut args = HashMap::new();
        args.insert(
            "path".to_string(),
            Value::String("/field_1/not_found".to_string()),
        );
        let result = super::search(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(Value::Null, result.unwrap());
    }
    #[test]
    fn replace_key() {
        let obj =
            serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();
        let mut args = HashMap::new();

        args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = super::replace_key(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"@field_1":"value_1","field_2":"value_1"}"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn replace_key_with_array() {
        let obj = serde_json::from_str::<Value>(r#"[{"field_1":"value_1","field_2":"value_1"}]"#)
            .unwrap();
        let mut args = HashMap::new();

        args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = super::replace_key(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"@field_1":"value_1","field_2":"value_1"}]"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn replace_value() {
        let obj =
            serde_json::from_str::<Value>(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();
        let mut args = HashMap::new();

        args.insert("from".to_string(), Value::String("^(value_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = super::replace_value(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field_1":"@value_1","field_2":"@value_1"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn replace_value_with_array() {
        let obj = serde_json::from_str::<Value>(r#"[{"field_1":"value_1","field_2":"value_1"}]"#)
            .unwrap();
        let mut args = HashMap::new();

        args.insert("from".to_string(), Value::String("^(value_1)$".to_string()));
        args.insert("to".to_string(), Value::String("@$1".to_string()));

        let result = super::replace_value(&obj, &args);
        println!("{:?}", result);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"field_1":"@value_1","field_2":"@value_1"}]"#)
                .unwrap(),
            result.unwrap()
        );
    }
}
