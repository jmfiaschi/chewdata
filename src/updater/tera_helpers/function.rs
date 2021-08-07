use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;
use uuid::Uuid;

/// Return a generated v4 uuid.
///
/// # Example: Generate default uuid.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::uuid_v4;
///
/// let args = HashMap::new();
/// let first_result = uuid_v4(&args);
/// let second_result = uuid_v4(&args);
/// assert!(first_result.is_ok());
/// assert!(second_result.is_ok());
/// let first_value = first_result.unwrap();
/// let second_value = second_result.unwrap();
/// assert_ne!(first_value, second_value);
/// ```
/// # Example: Generate urn uuid.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::uuid_v4;
///
/// let mut args = HashMap::new();
/// args.insert("format".to_string(), Value::String("urn".to_string()));
/// let first_result = uuid_v4(&args);
/// let second_result = uuid_v4(&args);
/// assert!(first_result.is_ok());
/// let first_value = first_result.unwrap();
/// assert!(first_value.to_string().contains("urn:uuid:"));
/// assert!(second_result.is_ok());
/// let second_value = second_result.unwrap();
/// assert!(second_value.to_string().contains("urn:uuid:"));
/// assert_ne!(first_value, second_value);
/// ```
/// # Example: Generate hyphenated uuid.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::uuid_v4;
///
/// let mut args = HashMap::new();
/// args.insert("format".to_string(), Value::String("hyphenated".to_string()));
/// let first_result = uuid_v4(&args);
/// let second_result = uuid_v4(&args);
/// assert!(first_result.is_ok());
/// assert!(second_result.is_ok());
/// let first_value = first_result.unwrap();
/// let second_value = second_result.unwrap();
/// assert_ne!(first_value, second_value);
/// ```
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

/// Set env variable with a name and value.
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::set_env;
///
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), Value::String("ENV_NAME".to_string()));
/// args.insert("value".to_string(), Value::String("ENV_VALUE".to_string()));
/// set_env(&args);
/// assert_eq!(std::env::var("ENV_NAME").unwrap(),"ENV_VALUE");
/// ```
pub fn set_env(args: &HashMap<String, Value>) -> Result<Value> {
    let name = match args.get("name") {
        Some(val) => match from_value::<String>(val.clone()) {
            Ok(v) => v,
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `set_env` received name={} but `name` can only be a string",
                    val
                )));
            }
        },
        None => {
            return Err(Error::msg(
                "Function `set_env` didn't receive a `name` argument",
            ))
        }
    };

    let value = match args.get("value") {
        Some(val) => val,
        None => {
            return Err(Error::msg(
                "Function `set_env` didn't receive a `value` argument",
            ))
        }
    };

    match value {
        Value::String(string) => std::env::set_var(&name, string),
        Value::Number(number) => std::env::set_var(&name, number.to_string()),
        Value::Bool(bool) => std::env::set_var(&name, bool.to_string()),
        _ => (),
    };

    Ok(Value::Null)
}

/// Encode string to base64
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_encode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("my_test".to_string()));
/// let value = base64_encode(&args).unwrap();
/// assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
/// ```
/// # Example: Encode with config.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_encode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("my_test".to_string()));
/// args.insert("config".to_string(), Value::String("STANDARD_NO_PAD".to_string()));
/// let value = base64_encode(&args).unwrap();
/// assert_eq!("bXlfdGVzdA", value.as_str().unwrap());
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
    let config = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => base64::STANDARD_NO_PAD,
                "URL_SAFE" => base64::URL_SAFE,
                "URL_SAFE_NO_PAD" => base64::URL_SAFE_NO_PAD,
                "CRYPT" => base64::CRYPT,
                "BCRYPT" => base64::BCRYPT,
                "IMAP_MUTF7" => base64::IMAP_MUTF7,
                "BINHEX" => base64::BINHEX,
                _ => base64::STANDARD,
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::STANDARD,
    };
    let encode_string = base64::encode_config(decode_string, config);

    Ok(Value::String(encode_string))
}

/// Decode base64 string.
///
/// # Example
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_decode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("bXlfdGVzdA".to_string()));
/// let value = base64_decode(&args).unwrap();
/// assert_eq!("my_test", value.as_str().unwrap());
/// ```
/// # Example: Decode with config.
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::base64_decode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("bXlfdGVzdA".to_string()));
/// args.insert("config".to_string(), Value::String("STANDARD_NO_PAD".to_string()));
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
    let config = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => base64::STANDARD_NO_PAD,
                "URL_SAFE" => base64::URL_SAFE,
                "URL_SAFE_NO_PAD" => base64::URL_SAFE_NO_PAD,
                "CRYPT" => base64::CRYPT,
                "BCRYPT" => base64::BCRYPT,
                "IMAP_MUTF7" => base64::IMAP_MUTF7,
                "BINHEX" => base64::BINHEX,
                _ => base64::STANDARD,
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::STANDARD,
    };

    let decode_string = match base64::decode_config(encode_string, config) {
        Ok(bytes) => String::from_utf8_lossy(bytes.as_slice()).to_string(),
        Err(e) => {
            return Err(Error::msg(format!(
                "Function `base64_decode` can't decode the value. {}",
                e
            )))
        }
    };

    Ok(Value::String(decode_string))
}
