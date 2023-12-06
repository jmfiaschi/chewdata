use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

use crate::updater;

/// Encode string to base64
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::base64_encode;
///
/// let value = base64_encode(&Value::String("my_test".to_string()), &HashMap::new()).unwrap();
/// assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
/// ```
pub fn base64_encode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    let encode_string = updater::tera_helpers::function::base64_encode(&new_args)?;

    Ok(encode_string)
}

/// Decode base64 string.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::base64_decode;
///
/// let value = base64_decode(&Value::String("bXlfdGVzdA==".to_string()), &HashMap::new()).unwrap();
/// assert_eq!("my_test", value.as_str().unwrap());
/// ```
pub fn base64_decode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let mut new_args = args.clone();
    new_args.insert("value".to_string(), value.clone());
    let decode_string = updater::tera_helpers::function::base64_decode(&new_args)?;

    Ok(decode_string)
}

/// Set an environment variable.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::set_env;
///
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), Value::String("my_key".to_string()));
///
/// let value = set_env(&Value::String("my_var".to_string()), &args).unwrap();
/// assert_eq!("my_var", value.as_str().unwrap());
/// let value = std::env::var("chewdata:my_key").unwrap();
/// assert_eq!("my_var", value);
/// ```
pub fn set_env(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
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

    let var_env_value: String = match from_value::<String>(value.clone()) {
        Ok(v) => v,
        Err(_) => {
            return Err(Error::msg(format!(
                "Function `env` received value={} but `value` can only be a string",
                value
            )));
        }
    };

    // Avoid to override the system var env
    let var_env_key = format!("{}:{}", crate::PROJECT_NAME, name);

    std::env::set_var(var_env_key, var_env_value);

    Ok(value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_test() {
        let value = base64_encode(&Value::String("my_test".to_string()), &HashMap::new()).unwrap();
        assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
    }
    #[test]
    fn base64_decode_test() {
        let value =
            base64_decode(&Value::String("bXlfdGVzdA==".to_string()), &HashMap::new()).unwrap();
        assert_eq!("my_test", value.as_str().unwrap());
    }
    #[test]
    fn env_test() {
        let mut args = HashMap::new();
        args.insert("name".to_string(), Value::String("my_key".to_string()));

        let value = set_env(&Value::String("my_var".to_string()), &args).unwrap();
        assert_eq!("my_var", value.as_str().unwrap());
        let value = std::env::var("chewdata:my_key").unwrap();
        assert_eq!("my_var", value);
    }
}
