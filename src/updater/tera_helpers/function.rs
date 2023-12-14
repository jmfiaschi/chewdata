use base64::Engine;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;
use uuid::Uuid;

/// Return a generated v4 uuid.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::uuid_v4;
///
/// let args = HashMap::new();
/// let first_result = uuid_v4(&args);
/// let second_result = uuid_v4(&args);
/// assert!(first_result.is_ok());
/// assert!(second_result.is_ok());
/// ```
pub fn uuid_v4(args: &HashMap<String, Value>) -> Result<Value> {
    let uuid = Uuid::new_v4();
    let value_format = match args.get("format") {
        Some(val) => try_get_value!("uuid_v4", "format", Value, val),
        None => Value::default(),
    };

    let uuid_string = match value_format.as_str() {
        Some("hyphenated") => uuid.hyphenated().to_string(),
        Some("urn") => uuid.urn().to_string(),
        _ => uuid.simple().to_string(),
    };

    Ok(Value::String(uuid_string))
}

/// Encode string to base64
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_encode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("my_test".to_string()));
/// let value = base64_encode(&args).unwrap();
/// assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
/// ```
pub fn base64_encode(args: &HashMap<String, Value>) -> Result<Value> {
    let decode_string = match args.get("value") {
        Some(val) => match from_value::<String>(val.clone()) {
            Ok(v) => v,
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_encode` received value={} but `value` can only be a string",
                    val
                )));
            }
        },
        None => {
            return Err(Error::msg(
                "Function `base64_encode` didn't receive a `value` argument",
            ))
        }
    };
    let encode_string = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => {
                    base64::engine::general_purpose::STANDARD_NO_PAD.encode(decode_string)
                }
                "URL_SAFE" => base64::engine::general_purpose::URL_SAFE.encode(decode_string),
                "URL_SAFE_NO_PAD" => {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(decode_string)
                }
                _ => base64::engine::general_purpose::STANDARD.encode(decode_string),
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::engine::general_purpose::STANDARD.encode(decode_string),
    };

    Ok(Value::String(encode_string))
}

/// Decode base64 string.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_decode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("bXlfdGVzdA".to_string()));
/// let value = base64_decode(&args).unwrap();
/// assert_eq!("my_test", value.as_str().unwrap());
/// ```
pub fn base64_decode(args: &HashMap<String, Value>) -> Result<Value> {
    let encode_string = match args.get("value") {
        Some(val) => match from_value::<String>(val.clone()) {
            Ok(v) => v,
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received value={} but `value` can only be a string",
                    val
                )));
            }
        },
        None => {
            return Err(Error::msg(
                "Function `base64_decode` didn't receive a `value` argument",
            ))
        }
    };
    let decode_string = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => {
                    base64::engine::general_purpose::STANDARD_NO_PAD.decode(encode_string)
                }
                "URL_SAFE" => base64::engine::general_purpose::URL_SAFE.decode(encode_string),
                "URL_SAFE_NO_PAD" => {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encode_string)
                }
                _ => base64::engine::general_purpose::STANDARD.decode(encode_string),
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::engine::general_purpose::STANDARD.decode(encode_string),
    }
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
    .and_then(|res| {
        String::from_utf8(res).map_err(|e| std::io::Error::new(std::io::ErrorKind::Unsupported, e))
    })?;

    Ok(Value::String(decode_string))
}

/// Decode base64 string.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::get_env;
///
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), Value::String("my_key".to_string()));
///
/// std::env::set_var("chewdata:my_key", "my_var");
///
/// let value = get_env(&args).unwrap();
/// assert_eq!("my_var", value.as_str().unwrap());
/// ```
pub fn get_env(args: &HashMap<String, Value>) -> Result<Value> {
    let name: String = match args.get("name") {
        Some(val) => match from_value::<String>(val.clone()) {
            Ok(v) => v,
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `env` received name={} but `name` can only be a string",
                    val
                )));
            }
        },
        None => {
            return Err(Error::msg(
                "Function `env` didn't receive a `name` argument",
            ))
        }
    };

    // Avoid to override the system var env
    let var_env_key = format!("{}:{}", crate::PROJECT_NAME, name);

    match std::env::var(var_env_key).ok() {
        Some(res) => Ok(Value::String(res)),
        None => match args.get("default") {
            Some(default) => Ok(default.clone()),
            None => Err(Error::msg(format!(
                "Environment variable `{}` not found",
                &name
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v4_uuid() {
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
    fn uuid_v4_urn() {
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
    fn uuid_v4_hyphenated_uuid() {
        let mut args = HashMap::new();
        args.insert(
            "format".to_string(),
            Value::String("hyphenated".to_string()),
        );
        let first_result = uuid_v4(&args);
        let second_result = uuid_v4(&args);
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        let first_value = first_result.unwrap();
        let second_value = second_result.unwrap();
        assert_ne!(first_value, second_value);
    }
    #[test]
    fn base64_encode() {
        let mut args = HashMap::new();
        args.insert("value".to_string(), Value::String("my_test".to_string()));
        let value = super::base64_encode(&args).unwrap();
        assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
    }
    #[test]
    fn base64_encode_with_config() {
        let mut args = HashMap::new();
        args.insert("value".to_string(), Value::String("my_test".to_string()));
        args.insert(
            "config".to_string(),
            Value::String("STANDARD_NO_PAD".to_string()),
        );
        let value = super::base64_encode(&args).unwrap();
        assert_eq!("bXlfdGVzdA", value.as_str().unwrap());
    }
    #[test]
    fn base64_decode() {
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("bXlfdGVzdA==".to_string()),
        );
        let value = super::base64_decode(&args).unwrap();
        assert_eq!("my_test", value.as_str().unwrap());
    }
    #[test]
    fn base64_decode_with_config() {
        let mut args = HashMap::new();
        args.insert("value".to_string(), Value::String("bXlfdGVzdA".to_string()));
        args.insert(
            "config".to_string(),
            Value::String("STANDARD_NO_PAD".to_string()),
        );
        let value = super::base64_decode(&args).unwrap();
        assert_eq!("my_test", value.as_str().unwrap());
    }
    #[test]
    fn get_env() {
        let mut args = HashMap::new();
        args.insert("name".to_string(), Value::String("my_key".to_string()));

        std::env::set_var("chewdata:my_key", "my_var");

        let value = super::get_env(&args).unwrap();
        assert_eq!("my_var", value.as_str().unwrap());
    }
}
