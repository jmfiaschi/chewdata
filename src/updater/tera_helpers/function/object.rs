use json_value_merge::Merge;
use json_value_resolve::Resolve;
use json_value_search::Search;
use regex::Regex;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Merge two Value together.
///
/// # Arguments
///
/// * `from` - The initial value.
/// * `with` - An object to merge with the `from`.
/// * `in` - A json pointer path to merge the `with` value object in a specific position. Example: `/field1/field2`
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::function::object::merge;
///
/// let mut from_array: Vec<Value> = Vec::default();
/// from_array.push(Value::String("a".to_string()));
/// let from = Value::Array(from_array);
///
/// let mut with_array: Vec<Value> = Vec::default();
/// with_array.push(Value::String("b".to_string()));
/// let with = Value::Array(with_array);
///
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), from);
/// args.insert("with".to_string(), with);
///
/// let result = merge(&args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"["a","b"]"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn merge(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let mut from = args
        .get("from")
        .ok_or_else(|| Error::msg("Function `merge` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("merge", "from", Value, val)))?
        .clone();

    // Extracting and validating the 'value' argument
    let with = args
        .get("with")
        .ok_or_else(|| Error::msg("Function `merge` didn't receive a `with` argument"))
        .and_then(|val| Ok(try_get_value!("merge", "with", Value, val)))?;

    let into = match args.get("in") {
        Some(path) => Some(try_get_value!("merge", "in", String, path)),
        None => None,
    };

    match into {
        Some(path) => {
            from.merge_in(path.as_str(), &with)?;
        }
        None => {
            from.merge(&with);
        }
    };

    Ok(from)
}

/// Search an element of an object.
///
/// # Arguments
///
/// * `from` - The initial value.
/// * `path` - The json pointer path where to find the element.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_search::Search;
/// use json_value_merge::Merge;
/// use chewdata::updater::tera_helpers::function::object::search;
///
/// let mut from = Value::default();
/// from.merge_in("/field_1/field_2", &Value::String("value".to_string()));
///
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), from.clone());
/// args.insert("path".to_string(), Value::String("/field_1".to_string()));
///
/// let result = search(&args);
/// assert!(result.is_ok());
/// assert_eq!(serde_json::from_str::<Value>(r#"{"field_2":"value"}"#).unwrap(), result.unwrap());
///
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), from.clone());
/// args.insert("path".to_string(), Value::String("/field_1/field_2".to_string()));
///
/// let result = search(&args);
/// assert!(result.is_ok());
/// assert_eq!("value".to_string(), result.unwrap());
///
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), from);
/// args.insert("path".to_string(), Value::String("/field_1/not_found".to_string()));
///
/// let result = search(&args);
/// assert!(result.is_ok());
/// assert_eq!(Value::Null, result.unwrap());
/// ```
pub fn search(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let from = args
        .get("from")
        .ok_or_else(|| Error::msg("Function `search` didn't receive a `from` argument"))
        .and_then(|val| Ok(try_get_value!("search", "from", Value, val)))?;

    // Extracting and validating the 'path' argument
    let path = args
        .get("path")
        .ok_or_else(|| Error::msg("Function `search` didn't receive a `path` argument"))
        .and_then(|val| Ok(try_get_value!("search", "path", String, val)))?;

    let new_value = match from.clone().search(path.as_str())? {
        Some(value) => value,
        None => Value::Null,
    };

    Ok(new_value)
}

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
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_array_of_scalar() {
        let mut from_array: Vec<Value> = Vec::default();
        from_array.push(Value::String("a".to_string()));
        let from = Value::Array(from_array);

        let mut with_array: Vec<Value> = Vec::default();
        with_array.push(Value::String("b".to_string()));
        let with = Value::Array(with_array);

        let mut args = HashMap::new();
        args.insert("from".to_string(), from);
        args.insert("with".to_string(), with);

        let result = merge(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"["a","b"]"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_merge_array_of_object() {
        let mut from_array: Vec<Value> = Vec::default();
        from_array.push(serde_json::from_str(r#"{"field1":"value1"}"#).unwrap());
        let from = Value::Array(from_array);

        let mut with_array: Vec<Value> = Vec::default();
        with_array.push(serde_json::from_str(r#"{"field2":"value2"}"#).unwrap());
        let with = Value::Array(with_array);

        let mut args = HashMap::new();
        args.insert("from".to_string(), from);
        args.insert("with".to_string(), with);

        let result = merge(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"[{"field1":"value1"},{"field2":"value2"}]"#).unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_merge_objects() {
        let mut from = Value::default();
        from.merge_in("/field", &Value::String("value".to_string()))
            .unwrap();
        let mut with = Value::default();
        with.merge_in("/other_field", &Value::String("other value".to_string()))
            .unwrap();
        let mut args = HashMap::new();
        args.insert("from".to_string(), from);
        args.insert("with".to_string(), with);

        let result = merge(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_merge_objects_with_path() {
        let mut from = Value::default();
        from.merge_in("/field", &Value::String("value".to_string()))
            .unwrap();
        let with = Value::String("other value".to_string());

        let mut args = HashMap::new();
        args.insert("from".to_string(), from);
        args.insert("with".to_string(), with);
        args.insert("in".to_string(), Value::String("/other_field".to_string()));

        let result = merge(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
    #[test]
    fn test_search() {
        let mut from = Value::default();
        from.merge_in("/field_1/field_2", &Value::String("value".to_string()))
            .unwrap();

        let mut args = HashMap::new();
        args.insert("from".to_string(), from.clone());
        args.insert("path".to_string(), Value::String("/field_1".to_string()));

        let result = search(&args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field_2":"value"}"#).unwrap(),
            result.unwrap()
        );

        let mut args = HashMap::new();
        args.insert("from".to_string(), from.clone());
        args.insert(
            "path".to_string(),
            Value::String("/field_1/field_2".to_string()),
        );
        let result = search(&args);
        assert!(result.is_ok());
        assert_eq!("value".to_string(), result.unwrap());

        let mut args = HashMap::new();
        args.insert("from".to_string(), from);
        args.insert(
            "path".to_string(),
            Value::String("/field_1/not_found".to_string()),
        );
        let result = search(&args);
        assert!(result.is_ok());
        assert_eq!(Value::Null, result.unwrap());
    }
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
}
