use crate::helper::json_pointer::JsonPointer;
use crate::helper::value::{Extract, MergeAndReplace};
use crate::updater::tera::engine;
use json_value_merge::Merge;
use json_value_resolve::Resolve;
use json_value_search::Search;
use regex::Regex;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Merge two objects together.
///
/// # Arguments
///
/// * `with` - Object to merge with the value in input.
/// * `attribute` - (optional) Where to merge the object defined by the 'with' argument.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use serde_json::json;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let from = json!(["a"]);
/// let with = json!(["b"]);
/// let mut args = HashMap::new();
/// args.insert("with".to_string(), with);
///
/// let result = merge(&from, &args).unwrap();
/// assert_eq!(result, json!(["a", "b"]));
/// ```
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use serde_json::json;
/// use chewdata::updater::tera_helpers::filters::object::merge;
///
/// let from = json!({"a":"b"});
/// let with = json!({"c":"d"});
/// let mut args = HashMap::new();
/// args.insert("with".to_string(), with);
/// args.insert("attribute".to_string(), json!("e"));
///
/// let result = merge(&from, &args).unwrap();
/// assert_eq!(result, json!({"a":"b","e":{"c":"d"}}));
/// ```
pub fn merge(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let with = args
        .get("with")
        .ok_or_else(|| Error::msg("Function `merge` didn't receive a `with` argument"))
        .and_then(|val| Ok(try_get_value!("merge", "with", Value, val)))?;

    let attribute = args.get("attribute").and_then(|v| v.as_str());

    let mut new_value = value.clone();

    match attribute {
        Some(attribute) => {
            let json_pointer = attribute.to_string().to_json_pointer();
            new_value.merge_in(&json_pointer, &with)?;
        }
        None => {
            new_value.merge(&with);
        }
    };

    Ok(new_value)
}

/// Search an element of an object.
///
/// # Arguments
///
/// * `attribute` - Attribute to search and return.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use json_value_search::Search;
/// use json_value_merge::Merge;
/// use serde_json::json;
/// use chewdata::updater::tera_helpers::filters::object::search;
///
/// let from = json!({"field_1":{"field_2": "value"}});
/// let mut args = HashMap::new();
/// args.insert("attribute".to_string(), json!("/field_1"));
///
/// let result = search(&from, &args);
/// assert!(result.is_ok());
/// assert_eq!(serde_json::from_str::<Value>(r#"{"field_2":"value"}"#).unwrap(), result.unwrap());
/// ```
pub fn search(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'path' argument
    let path = args
        .get("attribute")
        .ok_or_else(|| Error::msg("Function `search` didn't receive an `attribute` argument"))
        .and_then(|val| Ok(try_get_value!("search", "attribute", String, val)))?;

    let new_value = match value.clone().search(path.as_str())? {
        Some(value) => value,
        None => Value::Null,
    };

    Ok(new_value)
}

/// Replace object key name by another.
///
/// # Arguments
///
/// * `from` - The key to replace. Can be a regular expression.
/// * `to` - The new key.
/// * `level` - The depth level to apply the replacement.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::object::replace_key;
/// use serde_json::json;
///
/// let value: Value = json!({"field_1":"value_1","field_2":"value_1"});
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), Value::String("^(field_1)$".to_string()));
/// args.insert("to".to_string(), Value::String("@$1".to_string()));
///
/// let result = replace_key(&value, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"@field_1":"value_1","field_2":"value_1"}"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn replace_key(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
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

    let mut new_value = value.clone();

    replace_key_recursively(&mut new_value, &from, &to, level, 0)?;

    Ok(new_value)
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

/// Replace object value by another.
///
/// # Arguments
///
/// * `from` - The value to replace. Can be a regular expression.
/// * `to` - The new value.
/// * `level` - The depth level to apply the replacement.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::object::replace_value;
/// use serde_json::json;
///
/// let value = json!({"field_1":"value_1","field_2":"value_1"});
///
/// let mut args = HashMap::new();
/// args.insert("from".to_string(), json!("^(value_1)$"));
/// args.insert("to".to_string(), json!("@$1"));
///
/// let result = replace_value(&value, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     serde_json::from_str::<Value>(r#"{"field_1":"@value_1","field_2":"@value_1"}"#).unwrap(),
///     result.unwrap()
/// );
/// ```
pub fn replace_value(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
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

    let mut new_value = value.clone();

    replace_value_recursively(&mut new_value, &from, &to, level, 0)?;

    Ok(new_value)
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
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::object::extract;
/// use serde_json::json;
///
/// let from = json!([{"field1_1":{"field1_2":"value1_1"}},{"field2_1":{"field2_2":"value2_1"}}]);
/// let mut args = HashMap::new();
/// args.insert("attributes".to_string(), json!(["field1_1.field1_2"]));
///
/// let result = extract(&from, &args);
/// assert!(result.is_ok());
/// assert_eq!(
///     json!([{"field1_1":{"field1_2":"value1_1"}}]),
///     result.unwrap()
/// );
/// ```
pub fn extract(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
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

    match value {
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
        Value::Object(_) => extract_attributes(value, &attributes),
        _ => Ok(Value::Null),
    }
}

/// Update values of an object or array by applying a Tera filter to a specified attribute.
/// # Arguments
/// * `value` - A reference to a serde_json::Value which is either an object or an array.
/// * `args` - A HashMap containing the following keys:
///     - "fn": The name of the Tera filter function to apply.
///     - "attribute": The attribute (in dot notation) to update.
/// # Returns
/// A Result containing the updated serde_json::Value or an error if the operation fails.
///
/// # Example
/// ```
/// use serde_json::json;
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::filters::object::update;
///
/// let mut args = HashMap::new();
/// args.insert("fn".to_string(), json!("filter"));
/// args.insert("filter_attribute".to_string(), json!("code"));
/// args.insert("filter_value".to_string(), json!("admin"));
/// args.insert("attribute".to_string(), json!("roles"));
///
/// let value = json!({"name": "  Alice  ", "age": 30, "roles": [{"name": " Admin ","code": "admin"}, {"name": " Other ","code": "other"}]});
/// let updated_value = update(&value, &args).unwrap();
/// assert_eq!(updated_value, json!({"name": "  Alice  ", "age": 30, "roles": [{"name": " Admin ","code": "admin"}]}));
/// ```
pub fn update(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let engine = engine();

    let fn_name: &str = args
        .get("fn")
        .and_then(|v| v.as_str())
        .ok_or("map requires 'fn'")?;

    if fn_name == "update" {
        return Err(Error::msg(
            "`fn=update` cannot be used with this filter to avoid recurcive calls",
        ));
    }

    let attribute = args
        .get("attribute")
        .and_then(|v| v.as_str())
        .ok_or("update requires 'attribute'")?
        .to_string();

    let json_pointer = attribute.to_json_pointer();
    let fields: Vec<&str> = json_pointer.split('/').skip(1).collect();

    let mut new_args = HashMap::new();
    for (k, v) in args {
        if k.starts_with(&format!("{fn_name}_")) {
            new_args.insert(k[fn_name.len() + 1..].to_string(), v.clone());
        }
    }

    let guard = engine.lock().map_err(|e| Error::msg(e))?;
    let filter = guard.get_filter(fn_name)?;
    let new_value = &mut value.clone();

    if !search_and_update(new_value, &fields, filter, &new_args)? {
        return Err(Error::msg(format!("Attribute not found '{}'", &attribute)));
    }

    Ok(new_value.clone())
}

fn search_and_update(
    value: &mut Value,
    fields: &[&str],
    filter: &dyn Filter,
    args: &HashMap<String, Value>,
) -> Result<bool> {
    if let Some((field, rest)) = fields.split_first() {
        // Numeric index
        if let Ok(index) = field.parse::<usize>() {
            return match value {
                Value::Array(arr) => arr
                    .get_mut(index)
                    .map(|v| search_and_update(v, rest, filter, args))
                    .unwrap_or(Ok(false)),
                _ => Ok(false),
            };
        }

        search_by_pattern(value, field, rest, filter, args)
    } else {
        *value = filter.filter(value, args)?;
        Ok(true)
    }
}

fn search_by_pattern(
    value: &mut Value,
    pattern: &str,
    fields: &[&str],
    filter: &dyn Filter,
    args: &HashMap<String, Value>,
) -> Result<bool> {
    match value {
        Value::Array(arr) => {
            let mut updated = false;

            for v in arr {
                if pattern == "*" {
                    updated |= search_and_update(v, fields, filter, args)?;
                } else {
                    updated |= search_by_pattern(v, pattern, fields, filter, args)?;
                }
            }

            Ok(updated)
        }

        Value::Object(map) => {
            let re = Regex::new(pattern).unwrap();

            for (key, v) in map {
                if re.is_match(key) {
                    return search_and_update(v, fields, filter, args);
                }
            }

            Ok(false)
        }

        _ => Ok(false),
    }
}

/// Map function to extract a specific attribute from an object.
/// # Arguments
/// * `value` - A reference to a serde_json::Value which is an object.
/// * `args` - A HashMap containing the following key:
///     - "attribute": The attribute (in dot notation) to extract.
/// # Returns
/// A Result containing the extracted serde_json::Value or an error if the operation fails.
pub fn map(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let attribute = args
        .get("attribute")
        .and_then(|v| v.as_str())
        .ok_or("map requires 'attribute'")?
        .to_string()
        .to_json_pointer();

    let found_value = match value.clone().search(&attribute)? {
        Some(attr_value) => attr_value,
        None => {
            return Err(Error::msg(format!(
                "Attribute '{}' not found in {}",
                &attribute, value
            )))
        }
    };

    Ok(found_value)
}

// Returns all values of an array.
pub fn values(value: &Value, _args: &HashMap<String, Value>) -> Result<Value> {
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
pub fn keys(value: &Value, _args: &HashMap<String, Value>) -> Result<Value> {
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
    use macro_rules_attribute::apply;
    use serde_json::json;
    use smol_macros::test;
    use std::collections::HashMap;

    // ---------- Helpers ----------
    fn args(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn merge_array_of_scalar() {
        let from = json!(["a"]);
        let with = json!(["b"]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!(["a", "b"]));
    }

    #[test]
    fn merge_array_of_object() {
        let from = json!([{"field1": "value1"}]);
        let with = json!([{"field2": "value2"}]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!([{"field1": "value1"}, {"field2": "value2"}]));
    }

    #[test]
    fn merge_objects_flat() {
        let mut from = Value::default();
        from.merge_in("/field", &json!("value")).unwrap();
        let mut with = Value::default();
        with.merge_in("/other_field", &json!("other value"))
            .unwrap();
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "field": "value",
                "other_field": "other value"
            })
        );
    }

    #[test]
    fn merge_objects_with_attribute() {
        let from = json!({"field":"value"});
        let with = json!("other value");
        let args = args(&[("with", with), ("attribute", json!("other_field"))]);
        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "field": "value",
                "other_field": "other value"
            })
        );
    }

    #[test]
    fn merge_empty_arrays() {
        let from = json!([]);
        let with = json!([]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!([]));
    }

    #[test]
    fn merge_scalar_overwrites() {
        let from = json!("old");
        let with = json!("new");
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!("new"));
    }

    #[test]
    fn merge_nested_objects() {
        let mut from = Value::default();
        from.merge_in("/a/b", &json!("x")).unwrap();
        let mut with = Value::default();
        with.merge_in("/a/c", &json!("y")).unwrap();
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "a": {
                    "b": "x",
                    "c": "y"
                }
            })
        );
    }

    #[test]
    fn search_various_paths() {
        // Setup a nested object
        let value = json!({"field_1":{"field_2": "value"}});

        // Table of test cases: (search path, expected result)
        let cases = vec![
            ("/field_1", json!({ "field_2": "value" })),
            ("/field_1/field_2", json!("value")),
            ("/field_1/not_found", Value::Null),
        ];

        for (attribute, expected) in cases {
            let args = args(&[("attribute", attribute.into())]);
            let result = search(&value, &args).unwrap();
            assert_eq!(
                result, expected,
                "Failed to search attribute: {}",
                attribute
            );
        }
    }

    #[test]
    fn replace_key_with_object() {
        let value = json!({"field_1":"value_1","field_2":"value_1"});
        let args = args(&[("from", json!("^(field_1)$")), ("to", json!("@$1"))]);

        let result = replace_key(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!({"@field_1":"value_1","field_2":"value_1"}),
            result.unwrap()
        );
    }
    #[test]
    fn replace_key_with_array() {
        let value = json!([{"field_1":"value_1","field_2":"value_1"}]);
        let args = args(&[("from", json!("^(field_1)$")), ("to", json!("@$1"))]);

        let result = replace_key(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!([{"@field_1":"value_1","field_2":"value_1"}]),
            result.unwrap()
        );
    }

    #[test]
    fn replace_value_with_object() {
        let value = json!({"field_1":"value_1","field_2":"value_1"});
        let args = args(&[("from", json!("^(value_1)$")), ("to", json!("@$1"))]);

        let result = replace_value(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!({"field_1":"@value_1","field_2":"@value_1"}),
            result.unwrap()
        );
    }
    #[test]
    fn replace_value_with_array() {
        let value = json!([{"field_1":"value_1","field_2":"value_1"}]);
        let args = args(&[("from", json!("^(value_1)$")), ("to", json!("@$1"))]);

        let result = replace_value(&value, &args);

        assert!(result.is_ok());
        assert_eq!(
            json!([{"field_1":"@value_1","field_2":"@value_1"}]),
            result.unwrap()
        );
    }

    #[test]
    fn extract_on_array() {
        let from =
            json!([{"field1_1":{"field1_2":"value1_1"}},{"field2_1":{"field2_2":"value2_1"}}]);

        let args_1 = args(&[("attributes", json!(["field1_1.field1_2"]))]);

        let result = extract(&from, &args_1);
        assert!(result.is_ok());
        assert_eq!(
            json!([{"field1_1":{"field1_2":"value1_1"}}]),
            result.unwrap()
        );

        // Extract two attributes.
        let args_2 = args(&[(
            "attributes",
            json!(["field1_1.field1_2", "field2_1.field2_2"]),
        )]);

        let result = extract(&from, &args_2);
        assert!(result.is_ok());
        assert_eq!(from, result.unwrap());
    }
    #[test]
    fn extract_on_object() {
        let from = json!({"field1_1":{"field1_2":"value1_1"},"field2_1":{"field2_2":"value2_1"}});

        // Extract one attribute.
        let args = args(&[("attributes", json!(["field1_1.field1_2"]))]);

        let result = extract(&from, &args);
        assert!(result.is_ok());
        assert_eq!(json!({"field1_1":{"field1_2":"value1_1"}}), result.unwrap());
    }

    // ---------- Update Object Tests ----------
    #[apply(test!)]
    async fn update_object_trims_field() {
        let user = json!({ "name": "  alice ", "age": 30 });
        let args = args(&[("fn", json!("trim")), ("attribute", json!("name"))]);
        let result = update(&user, &args).unwrap();
        assert_eq!(result, json!({ "name": "alice", "age": 30 }));
    }

    #[apply(test!)]
    async fn update_object_regex_trim() {
        let user = json!({
            "name": "  alice ",
            "age": 30,
            "roles": [
                {"name_1": " Admin ", "code": "admin"},
                {"name_2": " Other ", "code": "other"}
            ]
        });
        let args = args(&[("fn", json!("trim")), ("attribute", json!("roles.*.name+"))]);
        let result = update(&user, &args).unwrap();
        let expected = json!({
            "name": "  alice ",
            "age": 30,
            "roles": [
                {"name_1": "Admin", "code": "admin"},
                {"name_2": "Other", "code": "other"}
            ]
        });

        assert_eq!(result, expected);
    }

    // ---------- Update Array Tests ----------
    #[apply(test!)]
    async fn update_array_positions() {
        let input = json!([
            { "name": "  alice ", "age": 30 },
            { "name": "  bob ", "age": 25 }
        ]);

        let cases = vec![
            ("*.name", json!(["alice", "bob"])),
            ("1.name", json!(["  alice ", "bob"])),
        ];

        for (attr, expected_names) in cases {
            let args = args(&[("fn", json!("trim")), ("attribute", json!(attr))]);

            let result = update(&input, &args).unwrap();
            let names: Vec<_> = result
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["name"].clone())
                .collect();

            assert_eq!(names, expected_names.as_array().unwrap().clone());
        }
    }

    #[apply(test!)]
    async fn update_array_with_nested_roles() {
        let users = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] }
        ]);

        let args = args(&[("fn", json!("trim")), ("attribute", json!("roles.*.name"))]);

        let result = update(&users, &args).unwrap();
        let expected = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": "Admin", "code": "admin"}, {"name": "Other", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": "Admin", "code": "admin"}, {"name": "Other", "code": "other"}] }
        ]);

        assert_eq!(result, expected);
    }

    #[apply(test!)]
    async fn update_array_with_map_function() {
        let users = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] }
        ]);

        let args = args(&[
            ("fn", json!("map")),
            ("attribute", json!("roles")),
            ("map_attribute", json!("name")),
        ]);

        let result = update(&users, &args).unwrap();
        let expected = json!([
            { "name": "  alice ", "age": 30, "roles": [" Admin ", " Other "] },
            { "name": "  bob ", "age": 25, "roles": [" Admin ", " Other "] }
        ]);

        assert_eq!(result, expected);
    }

    // ---------- Error Tests ----------
    #[apply(test!)]
    async fn update_with_recursive_call_fails() {
        let user = json!({ "name": "alice", "age": 30 });
        let args = args(&[("fn", json!("update")), ("attribute", json!("roles"))]);

        let err = update(&user, &args).unwrap_err();
        assert!(
            err.to_string().contains("update"),
            "Unexpected error: {}",
            err
        );
    }

    #[apply(test!)]
    async fn update_missing_attribute_returns_error() {
        let user = json!({ "name": "alice" });
        let args = args(&[("fn", json!("trim"))]);

        let err = update(&user, &args).unwrap_err();
        assert!(
            err.to_string().contains("attribute"),
            "Unexpected error: {}",
            err
        );
    }

    // ---------- Map Tests ----------
    #[test]
    fn map_object_field() {
        let user = json!({ "name": "alice", "age": 30 });
        let args = args(&[("attribute", json!("name"))]);
        let result = map(&user, &args).unwrap();
        assert_eq!(result, json!("alice"));
    }

    #[test]
    fn map_array_field() {
        let users = json!([
            {"name": "alice", "age": 30},
            {"name": "bob", "age": 25}
        ]);

        let args = args(&[("attribute", json!("name"))]);
        let result = map(&users, &args).unwrap();
        assert_eq!(result, json!(["alice", "bob"]));
    }

    #[test]
    fn keys_values_from_array() {
        let value = json!([
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]);

        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, value);
        assert_eq!(keys_result, to_value(vec![0, 1, 2]).unwrap());
    }
    #[test]
    fn keys_values_from_other_type() {
        let value = json!("a string");
        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, value);
        assert_eq!(keys_result, Value::Null);
    }
    #[test]
    fn keys_values_from_object() {
        let value = json!({"a":1,"b":2,"c":3});
        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, json!([1, 2, 3]));
        assert_eq!(keys_result, json!(["a", "b", "c"]));
    }
}

/// See [`updater::tera_helpers::function::object::values`] for more details.
pub fn values(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::object::values(&new_args)
}

/// See [`updater::tera_helpers::function::object::keys`] for more details.
pub fn keys(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    updater::tera_helpers::function::object::keys(&new_args)
}

/// Update values of an object or array by applying a Tera filter to a specified attribute.
/// # Arguments
/// * `value` - A reference to a serde_json::Value which is either an object or an array.
/// * `args` - A HashMap containing the following keys:
///     - "fn": The name of the Tera filter function to apply.
///     - "attribute": The attribute (in dot notation) to update.
/// # Returns
/// A Result containing the updated serde_json::Value or an error if the operation fails.
///
/// # Example
/// ```no_run
/// use serde_json::json;
/// use std::collections::HashMap;
/// use chewdata::updater::tera::Tera;
/// use chewdata::updater::tera_helpers::filters::object::update;
///
/// let tera = Tera::default();
/// futures::executor::block_on(async { tera.engine().await });
///
/// let mut args = HashMap::new();
/// args.insert("fn".to_string(), json!("filter"));
/// args.insert("filter_attribute".to_string(), json!("code"));
/// args.insert("filter_value".to_string(), json!("admin"));
/// args.insert("attribute".to_string(), json!("roles"));
///
/// let value = json!({"name": "  Alice  ", "age": 30, "roles": [{"name": " Admin ","code": "admin"}, {"name": " Other ","code": "other"}]});
/// let updated_value = update(&value, &args).unwrap();
/// assert_eq!(updated_value, json!({"name": "  Alice  ", "age": 30, "roles": [{"name": " Admin ","code": "admin"}]}));
/// ```
pub fn update(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let fn_name: &str = args
        .get("fn")
        .and_then(|v| v.as_str())
        .ok_or("map requires 'fn'")?;

    if fn_name == "update" {
        return Err(Error::msg("update function cannot be used in update"));
    }

    let attribute = args
        .get("attribute")
        .and_then(|v| v.as_str())
        .ok_or("update requires 'attribute'")?
        .to_string();

    let json_pointer = attribute.to_json_pointer();
    let fields: Vec<&str> = json_pointer.split('/').skip(1).collect();

    let mut new_args = HashMap::new();
    for (k, v) in args {
        if k.starts_with(&format!("{fn_name}_")) {
            new_args.insert(k[fn_name.len() + 1..].to_string(), v.clone());
        }
    }

    let engine = futures::executor::block_on(async { Tera::default().engine().await });
    let filter = engine.get_filter(fn_name)?;
    let new_value = &mut value.clone();

    if !search_and_update(new_value, &fields, filter, &new_args)? {
        return Err(Error::msg(format!("Attribute not found '{}'", &attribute)));
    }

    Ok(new_value.clone())
}

fn search_and_update(
    value: &mut Value,
    fields: &[&str],
    filter: &dyn Filter,
    args: &HashMap<String, Value>,
) -> Result<bool> {
    if let Some((field, rest)) = fields.split_first() {
        // Numeric index
        if let Ok(index) = field.parse::<usize>() {
            return match value {
                Value::Array(arr) => arr
                    .get_mut(index)
                    .map(|v| search_and_update(v, rest, filter, args))
                    .unwrap_or(Ok(false)),
                _ => Ok(false),
            };
        }

        search_by_pattern(value, field, rest, filter, args)
    } else {
        *value = filter.filter(value, args)?;
        Ok(true)
    }
}

fn search_by_pattern(
    value: &mut Value,
    pattern: &str,
    fields: &[&str],
    filter: &dyn Filter,
    args: &HashMap<String, Value>,
) -> Result<bool> {
    match value {
        Value::Array(arr) => {
            let mut updated = false;

            for v in arr {
                if pattern == "*" {
                    updated |= search_and_update(v, fields, filter, args)?;
                } else {
                    updated |= search_by_pattern(v, pattern, fields, filter, args)?;
                }
            }

            Ok(updated)
        }

        Value::Object(map) => {
            let re = Regex::new(pattern).unwrap();

            for (key, v) in map {
                if re.is_match(key) {
                    return search_and_update(v, fields, filter, args);
                }
            }

            Ok(false)
        }

        _ => Ok(false),
    }
}

/// Map function to extract a specific attribute from an object.
/// # Arguments
/// * `value` - A reference to a serde_json::Value which is an object.
/// * `args` - A HashMap containing the following key:
///     - "attribute": The attribute (in dot notation) to extract.
/// # Returns
/// A Result containing the extracted serde_json::Value or an error if the operation fails.
pub fn map(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let attribute = args
        .get("attribute")
        .and_then(|v| v.as_str())
        .ok_or("map requires 'attribute'")?
        .to_string()
        .to_json_pointer();

    let found_value = match value.clone().search(&attribute)? {
        Some(attr_value) => attr_value,
        None => {
            return Err(Error::msg(format!(
                "Attribute '{}' not found in {}",
                &attribute,
                value.to_string()
            )))
        }
    };

    Ok(found_value)
}

// Returns all values of an array.
pub fn values(value: &Value, _args: &HashMap<String, Value>) -> Result<Value> {
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
pub fn keys(value: &Value, _args: &HashMap<String, Value>) -> Result<Value> {
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
    use crate::updater::tera::Tera;
    use serde_json::json;
    use std::collections::HashMap;

    // ---------- Helpers ----------
    fn setup_tera() -> Tera {
        let tera = Tera::default();
        futures::executor::block_on(async { tera.engine().await });
        tera
    }

    fn args(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn merge_array_of_scalar() {
        let from = json!(["a"]);
        let with = json!(["b"]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!(["a", "b"]));
    }

    #[test]
    fn merge_array_of_object() {
        let from = json!([{"field1": "value1"}]);
        let with = json!([{"field2": "value2"}]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!([{"field1": "value1"}, {"field2": "value2"}]));
    }

    #[test]
    fn merge_objects_flat() {
        let mut from = Value::default();
        from.merge_in("/field", &json!("value")).unwrap();
        let mut with = Value::default();
        with.merge_in("/other_field", &json!("other value"))
            .unwrap();
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "field": "value",
                "other_field": "other value"
            })
        );
    }

    #[test]
    fn merge_objects_with_attribute() {
        let from = json!({"field":"value"});
        let with = json!("other value");
        let args = args(&[("with", with), ("attribute", json!("other_field"))]);
        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "field": "value",
                "other_field": "other value"
            })
        );
    }

    #[test]
    fn merge_empty_arrays() {
        let from = json!([]);
        let with = json!([]);
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!([]));
    }

    #[test]
    fn merge_scalar_overwrites() {
        let from = json!("old");
        let with = json!("new");
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();
        assert_eq!(result, json!("new"));
    }

    #[test]
    fn merge_nested_objects() {
        let mut from = Value::default();
        from.merge_in("/a/b", &json!("x")).unwrap();
        let mut with = Value::default();
        with.merge_in("/a/c", &json!("y")).unwrap();
        let args = args(&[("with", with)]);

        let result = merge(&from, &args).unwrap();

        assert_eq!(
            result,
            json!({
                "a": {
                    "b": "x",
                    "c": "y"
                }
            })
        );
    }

    #[test]
    fn search_various_paths() {
        // Setup a nested object
        let value = json!({"field_1":{"field_2": "value"}});

        // Table of test cases: (search path, expected result)
        let cases = vec![
            ("/field_1", json!({ "field_2": "value" })),
            ("/field_1/field_2", json!("value")),
            ("/field_1/not_found", Value::Null),
        ];

        for (attribute, expected) in cases {
            let args = args(&[("attribute", attribute.into())]);
            let result = search(&value, &args).unwrap();
            assert_eq!(
                result, expected,
                "Failed to search attribute: {}",
                attribute
            );
        }
    }

    #[test]
    fn replace_key_with_object() {
        let value = json!({"field_1":"value_1","field_2":"value_1"});
        let args = args(&[("from", json!("^(field_1)$")), ("to", json!("@$1"))]);

        let result = replace_key(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!({"@field_1":"value_1","field_2":"value_1"}),
            result.unwrap()
        );
    }
    #[test]
    fn replace_key_with_array() {
        let value = json!([{"field_1":"value_1","field_2":"value_1"}]);
        let args = args(&[("from", json!("^(field_1)$")), ("to", json!("@$1"))]);

        let result = replace_key(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!([{"@field_1":"value_1","field_2":"value_1"}]),
            result.unwrap()
        );
    }

    #[test]
    fn replace_value_with_object() {
        let value = json!({"field_1":"value_1","field_2":"value_1"});
        let args = args(&[("from", json!("^(value_1)$")), ("to", json!("@$1"))]);

        let result = replace_value(&value, &args);
        assert!(result.is_ok());
        assert_eq!(
            json!({"field_1":"@value_1","field_2":"@value_1"}),
            result.unwrap()
        );
    }
    #[test]
    fn replace_value_with_array() {
        let value = json!([{"field_1":"value_1","field_2":"value_1"}]);
        let args = args(&[("from", json!("^(value_1)$")), ("to", json!("@$1"))]);

        let result = replace_value(&value, &args);

        assert!(result.is_ok());
        assert_eq!(
            json!([{"field_1":"@value_1","field_2":"@value_1"}]),
            result.unwrap()
        );
    }

    #[test]
    fn extract_on_array() {
        let from =
            json!([{"field1_1":{"field1_2":"value1_1"}},{"field2_1":{"field2_2":"value2_1"}}]);

        let args_1 = args(&[("attributes", json!(["field1_1.field1_2"]))]);

        let result = extract(&from, &args_1);
        assert!(result.is_ok());
        assert_eq!(
            json!([{"field1_1":{"field1_2":"value1_1"}}]),
            result.unwrap()
        );

        // Extract two attributes.
        let args_2 = args(&[(
            "attributes",
            json!(["field1_1.field1_2", "field2_1.field2_2"]),
        )]);

        let result = extract(&from, &args_2);
        assert!(result.is_ok());
        assert_eq!(from, result.unwrap());
    }
    #[test]
    fn extract_on_object() {
        let from = json!({"field1_1":{"field1_2":"value1_1"},"field2_1":{"field2_2":"value2_1"}});

        // Extract one attribute.
        let args = args(&[("attributes", json!(["field1_1.field1_2"]))]);

        let result = extract(&from, &args);
        assert!(result.is_ok());
        assert_eq!(json!({"field1_1":{"field1_2":"value1_1"}}), result.unwrap());
    }

    // ---------- Update Object Tests ----------
    #[test]
    fn update_object_trims_field() {
        setup_tera();
        let user = json!({ "name": "  alice ", "age": 30 });

        let args = args(&[("fn", json!("trim")), ("attribute", json!("name"))]);

        let result = update(&user, &args).unwrap();
        assert_eq!(result, json!({ "name": "alice", "age": 30 }));
    }

    #[test]
    fn update_object_regex_trim() {
        setup_tera();
        let user = json!({
            "name": "  alice ",
            "age": 30,
            "roles": [
                {"name_1": " Admin ", "code": "admin"},
                {"name_2": " Other ", "code": "other"}
            ]
        });

        let args = args(&[("fn", json!("trim")), ("attribute", json!("roles.*.name+"))]);

        let result = update(&user, &args).unwrap();
        let expected = json!({
            "name": "  alice ",
            "age": 30,
            "roles": [
                {"name_1": "Admin", "code": "admin"},
                {"name_2": "Other", "code": "other"}
            ]
        });

        assert_eq!(result, expected);
    }

    // ---------- Update Array Tests ----------
    #[test]
    fn update_array_positions() {
        setup_tera();
        let input = json!([
            { "name": "  alice ", "age": 30 },
            { "name": "  bob ", "age": 25 }
        ]);

        let cases = vec![
            ("*.name", json!(["alice", "bob"])),
            ("1.name", json!(["  alice ", "bob"])),
        ];

        for (attr, expected_names) in cases {
            let args = args(&[("fn", json!("trim")), ("attribute", json!(attr))]);

            let result = update(&input, &args).unwrap();
            let names: Vec<_> = result
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["name"].clone())
                .collect();

            assert_eq!(names, expected_names.as_array().unwrap().clone());
        }
    }

    #[test]
    fn update_array_with_nested_roles() {
        setup_tera();
        let users = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] }
        ]);

        let args = args(&[("fn", json!("trim")), ("attribute", json!("roles.*.name"))]);

        let result = update(&users, &args).unwrap();
        let expected = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": "Admin", "code": "admin"}, {"name": "Other", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": "Admin", "code": "admin"}, {"name": "Other", "code": "other"}] }
        ]);

        assert_eq!(result, expected);
    }

    #[test]
    fn update_array_with_map_function() {
        setup_tera();
        let users = json!([
            { "name": "  alice ", "age": 30, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] },
            { "name": "  bob ", "age": 25, "roles": [{"name": " Admin ", "code": "admin"}, {"name": " Other ", "code": "other"}] }
        ]);

        let args = args(&[
            ("fn", json!("map")),
            ("attribute", json!("roles")),
            ("map_attribute", json!("name")),
        ]);

        let result = update(&users, &args).unwrap();
        let expected = json!([
            { "name": "  alice ", "age": 30, "roles": [" Admin ", " Other "] },
            { "name": "  bob ", "age": 25, "roles": [" Admin ", " Other "] }
        ]);

        assert_eq!(result, expected);
    }

    // ---------- Error Tests ----------
    #[test]
    fn update_with_recursive_call_fails() {
        let user = json!({ "name": "alice", "age": 30 });
        let args = args(&[("fn", json!("update")), ("attribute", json!("roles"))]);

        let err = update(&user, &args).unwrap_err();
        assert!(
            err.to_string().contains("update"),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn update_missing_attribute_returns_error() {
        let user = json!({ "name": "alice" });
        let args = args(&[("fn", json!("trim"))]);

        let err = update(&user, &args).unwrap_err();
        assert!(
            err.to_string().contains("attribute"),
            "Unexpected error: {}",
            err
        );
    }

    // ---------- Map Tests ----------
    #[test]
    fn map_object_field() {
        let user = json!({ "name": "alice", "age": 30 });
        let args = args(&[("attribute", json!("name"))]);
        let result = map(&user, &args).unwrap();
        assert_eq!(result, json!("alice"));
    }

    #[test]
    fn map_array_field() {
        let users = json!([
            {"name": "alice", "age": 30},
            {"name": "bob", "age": 25}
        ]);

        let args = args(&[("attribute", json!("name"))]);
        let result = map(&users, &args).unwrap();
        assert_eq!(result, json!(["alice", "bob"]));
    }

    #[test]
    fn keys_values_from_array() {
        let value = json!([
            {"a": 1},
            {"b": 2},
            {"c": 3},
        ]);

        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, value);
        assert_eq!(keys_result, to_value(vec![0, 1, 2]).unwrap());
    }
    #[test]
    fn keys_values_from_other_type() {
        let value = json!("a string");
        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, value);
        assert_eq!(keys_result, Value::Null);
    }
    #[test]
    fn keys_values_from_object() {
        let value = json!({"a":1,"b":2,"c":3});
        let values_result = values(&value, &HashMap::new()).unwrap();
        let keys_result = keys(&value, &HashMap::new()).unwrap();
        assert_eq!(values_result, json!([1, 2, 3]));
        assert_eq!(keys_result, json!(["a", "b", "c"]));
    }
}
