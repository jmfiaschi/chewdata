use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;
use uuid::Uuid;

/// Generate a v4 uuid
pub fn uuid_v4(args: &HashMap<String, Value>) -> Result<Value> {
    let uuid = Uuid::new_v4();
    let value_format = match args.get("format") {
        Some(val) => try_get_value!("uuid_v4", "format", Value, val),
        None => Value::default(),
    };
    
    let uuid_string = match value_format.as_str() {
        Some("hyphenated") => uuid.to_hyphenated().to_string(),
        Some("urn") => uuid.to_urn().to_string(),
        _ => uuid.to_simple().to_string(),
    };

    Ok(Value::String(uuid_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn if_should_get_simple_uuid_by_default() {
        let args = HashMap::new();
        let first_result = uuid_v4(&args);
        let second_result = uuid_v4(&args);
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        let first_value = first_result.unwrap();
        let second_value = second_result.unwrap();
        assert_ne!(first_value, second_value);
    }
    #[test]
    fn if_should_get_urn_uuid() {
        let mut args = HashMap::new();
        args.insert("format".to_string(), Value::String("urn".to_string()));
        let first_result = uuid_v4(&args);
        let second_result = uuid_v4(&args);
        assert!(first_result.is_ok());
        let first_value = first_result.unwrap();
        assert!(first_value.to_string().contains("urn:uuid:"));
        assert!(second_result.is_ok());
        let second_value = second_result.unwrap();
        assert!(second_value.to_string().contains("urn:uuid:"));
        assert_ne!(first_value, second_value);
    }
    #[test]
    fn if_should_get_hyphenated_uuid() {
        let mut args = HashMap::new();
        args.insert("format".to_string(), Value::String("hyphenated".to_string()));

        let first_result = uuid_v4(&args);
        let second_result = uuid_v4(&args);
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        let first_value = first_result.unwrap();
        let second_value = second_result.unwrap();
        assert_ne!(first_value, second_value);
    }
}
