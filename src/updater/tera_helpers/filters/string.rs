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
}
