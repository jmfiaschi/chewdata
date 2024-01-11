use json_value_merge::Merge;
use regex::Regex;
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

/// Trait used to extract attribute from a Json Values.
pub trait Extract {
    /// used to extract attribute from a Json Values.
    fn extract(&self, path: &str) -> io::Result<Value>;
}

impl Extract for serde_json::Value {
    fn extract(&self, path: &str) -> io::Result<Value> {
        let search_attribute_path_fields: Vec<&str> = path.split('/').skip(1).collect();

        let mut new_value = Value::default();
        attributes_extraction(
            self,
            &mut new_value,
            &search_attribute_path_fields,
            &mut Vec::default(),
        )?;
        Ok(new_value)
    }
}

fn attributes_extraction(
    from: &Value,
    to: &mut Value,
    search_attribute_path_fields: &Vec<&str>,
    new_attribute_path_fields: &mut [String],
) -> io::Result<()> {
    if search_attribute_path_fields.is_empty() {
        to.merge_in(&format!("/{}", new_attribute_path_fields.join("/")), from)?;
        return Ok(());
    }

    let mut search_attribute_path_fields = search_attribute_path_fields.clone();
    let current_field = search_attribute_path_fields.remove(0);

    match (&from, current_field) {
        (Value::Array(vec), "*") => {
            for (index, value) in vec.iter().enumerate() {
                let mut new_attribute_path_fields = new_attribute_path_fields.to_owned();
                new_attribute_path_fields.push(index.to_string());
                attributes_extraction(
                    value,
                    to,
                    &search_attribute_path_fields,
                    &mut new_attribute_path_fields,
                )?;
            }
        }
        (Value::Array(vec), _) => {
            if let Ok(index) = current_field.parse::<usize>() {
                if let Some(value) = vec.get(index) {
                    let mut new_attribute_path_fields = new_attribute_path_fields.to_owned();
                    new_attribute_path_fields.push(current_field.to_string());
                    attributes_extraction(
                        value,
                        to,
                        &search_attribute_path_fields,
                        &mut new_attribute_path_fields,
                    )?;
                }
            }
        }
        (Value::Object(map), _) => {
            let re = Regex::new(current_field)
                .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))?;

            for (key, value) in map {
                if re.is_match(key.as_str()) {
                    let mut new_attribute_path_fields = new_attribute_path_fields.to_owned();
                    new_attribute_path_fields.push(key.to_string());
                    attributes_extraction(
                        value,
                        to,
                        &search_attribute_path_fields,
                        &mut new_attribute_path_fields,
                    )?;
                }
            }
        }
        (_, _) => (),
    }

    Ok(())
}

// Trait that merge two objects together. Merging two array together will preserve the order and can replace values.
pub trait MergeAndReplace {
    /// Method use to merge two Json Values : ValueA <- ValueB. Merging two array together will preserve the order and can replace values.
    fn merge_replace(&mut self, new_value: &Value);
}

impl MergeAndReplace for serde_json::Value {
    fn merge_replace(&mut self, new_json_value: &Value) {
        merge_replace(self, new_json_value);
    }
}

fn merge_replace(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(ref mut a), Value::Object(ref b)) => {
            for (k, v) in b {
                merge_replace(a.entry(k).or_insert(Value::default()), v);
            }
        }
        (Value::Array(ref mut a), Value::Array(ref b)) => {
            for (idx, value_b) in b.iter().enumerate() {
                match a.get_mut(idx) {
                    Some(value_a) => value_a.merge_replace(value_b),
                    None => a.append(&mut vec![value_b.clone()]),
                }
            }
        }
        (a, b) => a.merge(b),
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
    #[test]
    fn test_extract() {
        let object: Value = serde_json::from_str(
            r#"{"field1":[{"field2":"value2","field3":{"field4":"value4"}},"value1"]}"#,
        )
        .unwrap();

        let expected: Value =
            serde_json::from_str(r#"{"field1":[{"field3":{"field4":"value4"}}]}"#).unwrap();
        assert_eq!(expected, object.extract("/field1/*/field3").unwrap());

        let expected: Value =
            serde_json::from_str(r#"{"field1":[{"field3":{"field4":"value4"}}]}"#).unwrap();
        assert_eq!(expected, object.extract("/field1/0/field3").unwrap());
        assert_eq!(
            Value::default(),
            object.extract("/field1/1/field3").unwrap()
        );

        let object: Value = serde_json::from_str(
            r#"{"field1":[{"field2":"value2","field3":{"field4":"value4"}},"value1",{"field5":"value5"}]}"#,
        )
        .unwrap();
        let expected: Value = serde_json::from_str(
            r#"{"field1":[{"field2":"value2","field3":{"field4":"value4"}},{"field5":"value5"}]}"#,
        )
        .unwrap();
        assert_eq!(expected, object.extract("/field1/*/field.+").unwrap());
    }
    #[test]
    fn test_merge_replace() {
        let mut json_value: Value =
            serde_json::from_str(r#"{"field":[{"field2":"value2"},{"field3":"value3"}]}"#).unwrap();
        let json_value_to_merge: Value =
            serde_json::from_str(r#"{"field":[{"field4":"value4"},{"field5":"value5"}]}"#).unwrap();
        json_value.merge_replace(&json_value_to_merge);
        assert_eq!(
            r#"{"field":[{"field2":"value2","field4":"value4"},{"field3":"value3","field5":"value5"}]}"#,
            json_value.to_string()
        );
    }
}
