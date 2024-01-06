use serde_json::{Map, Value};
use std::io;

/// Trait used to flat a Json Values
pub trait Flatten {
    /// Method use to get all the attributes from the object.
    fn flatten(&self) -> io::Result<Map<String, Value>>;
}

impl Flatten for serde_json::Value {
    fn flatten(&self) -> io::Result<Map<String, Value>> {
        match self {
            Value::Object(map) => Ok(flatten(map)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Attributes can be use only on an object",
            )),
        }
    }
}

fn flatten(json: &Map<String, Value>) -> Map<String, Value> {
    let mut obj = Map::new();
    insert_object(&mut obj, None, json);
    obj
}

fn insert_object(
    base_json: &mut Map<String, Value>,
    base_key: Option<&str>,
    object: &Map<String, Value>,
) {
    for (key, value) in object {
        let new_key = base_key.map_or_else(|| key.clone(), |base_key| format!("{base_key}.{key}"));

        if let Some(array) = value.as_array() {
            insert_array(base_json, Some(&new_key), array);
        } else if let Some(object) = value.as_object() {
            insert_object(base_json, Some(&new_key), object);
        } else {
            insert_value(base_json, &new_key, value.clone());
        }
    }
}

fn insert_array(base_json: &mut Map<String, Value>, base_key: Option<&str>, array: &[Value]) {
    for (key, value) in array.iter().enumerate() {
        let new_key = base_key.map_or_else(
            || key.clone().to_string(),
            |base_key| format!("{base_key}.{key}"),
        );
        if let Some(object) = value.as_object() {
            insert_object(base_json, Some(&new_key), object);
        } else if let Some(sub_array) = value.as_array() {
            insert_array(base_json, Some(&new_key), sub_array);
        } else {
            insert_value(base_json, &new_key, value.clone());
        }
    }
}

fn insert_value(base_json: &mut Map<String, Value>, key: &str, to_insert: Value) {
    debug_assert!(!to_insert.is_object());
    debug_assert!(!to_insert.is_array());

    // does the field aleardy exists?
    if let Some(value) = base_json.get_mut(key) {
        // is it already an array
        if let Some(array) = value.as_array_mut() {
            array.push(to_insert);
        // or is there a collision
        } else {
            let value = std::mem::take(value);
            base_json[key] = serde_json::json!([value, to_insert]);
        }
        // if it does not exist we can push the value untouched
    } else {
        base_json.insert(key.to_string(), serde_json::json!(to_insert));
    }
}

/// Trait used to have the depth of a Json Value.
pub trait Depth {
    /// Method use to get the depth of a Json Value.
    fn depth(&self) -> usize;
}

impl Depth for serde_json::Value {
    fn depth(&self) -> usize {
        depth(self)
    }
}

fn depth(object: &Value) -> usize {
    match object {
        Value::Array(vec) => {
            let mut depths = Vec::default();
            for value in vec {
                depths.push(depth(value))
            }
            depths.into_iter().max().unwrap_or_default() + 1
        }
        Value::Object(map) => {
            let mut depths = Vec::default();
            for value in map.values() {
                depths.push(depth(value))
            }
            depths.into_iter().max().unwrap_or_default() + 1
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten() {
        let object: Value = serde_json::from_str(
            r#"{"field1":[{"field2":"value2","field3":{"field4":"value4"}},"value1"]}"#,
        )
        .unwrap();

        assert_eq!(
            vec!["field1.0.field2", "field1.0.field3.field4", "field1.1"],
            object
                .flatten()
                .unwrap()
                .keys()
                .map(|key| key.clone())
                .collect::<Vec<String>>()
        )
    }
    #[test]
    fn test_depth() {
        let object: Value = serde_json::from_str(
            r#"{"field1":[{"field2":"value2","field3":{"field4":"value4"}},"value1"]}"#,
        )
        .unwrap();

        assert_eq!(4, object.depth())
    }
}
