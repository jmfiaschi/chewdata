use json_value_merge::Merge;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Merge two Value together.
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
            new_value.merge_in(path.as_str(), merge_with);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_should_merge_simple_array_of_scalars() {
        let mut array: Vec<Value> = Vec::default();
        array.push(Value::String("a".to_string()));
        array.push(Value::String("b".to_string()));

        let obj = Value::Array(array);
        let args = HashMap::new();

        let result = merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(Value::String("b".to_string()), result.unwrap());
    }

    #[test]
    fn if_should_merge_simple_array_of_objects() {
        let mut array: Vec<Value> = Vec::default();
        array.push(serde_json::from_str(r#"{"field1":"value1"}"#).unwrap());
        array.push(serde_json::from_str(r#"{"field2":"value2"}"#).unwrap());

        let obj = Value::Array(array);
        let args = HashMap::new();

        let result = merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field1":"value1","field2":"value2"}"#).unwrap(),
            result.unwrap()
        );
    }

    #[test]
    fn it_should_merge_one_object_with_another() {
        let mut obj = Value::default();
        obj.merge_in("/field", Value::String("value".to_string()));

        let mut with = Value::default();
        with.merge_in("/other_field", Value::String("other value".to_string()));

        let mut args = HashMap::new();
        args.insert("with".to_string(), with.clone());

        let result = merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }

    #[test]
    fn it_should_merge_one_object_with_another_in_specific_path() {
        let mut obj = Value::default();
        obj.merge_in("/field", Value::String("value".to_string()));

        let with = Value::String("other value".to_string());

        let mut args = HashMap::new();
        args.insert("with".to_string(), with.clone());
        args.insert("in".to_string(), Value::String("/other_field".to_string()));

        let result = merge(&obj, &args);
        assert!(result.is_ok());
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"field":"value","other_field":"other value"}"#)
                .unwrap(),
            result.unwrap()
        );
    }
}
