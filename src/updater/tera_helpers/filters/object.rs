use crate::helper::json_pointer::JsonPointer;
use crate::updater::{self, tera::Tera};
use json_value_search::Search;
use regex::Regex;
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

/// See [`updater::tera_helpers::function::object::extract`] for more details.
pub fn extract(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("from".to_string(), value.clone());
    updater::tera_helpers::function::object::extract(&new_args)
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
/// # Example
/// ```
/// use serde_json::json;
/// use std::collections::HashMap;
/// use chewdata::updater::tera::Tera;
/// use chewdata::updater::tera_helpers::filters::object::update;
///
/// let tera = Tera::default();
/// futures::executor::block_on(async { tera.engine().await });
/// let mut args = HashMap::new();
/// args.insert("fn".to_string(), json!("filter"));
/// args.insert("filter_attribute".to_string(), json!("code"));
/// args.insert("filter_value".to_string(), json!("admin"));
/// args.insert("attribute".to_string(), json!("roles"));
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
        .ok_or("map requires 'attribute'")?
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
}
