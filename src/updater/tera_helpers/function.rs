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
    let engine = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::STANDARD,
                    base64::engine::fast_portable::NO_PAD,
                ),
                "URL_SAFE" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::URL_SAFE,
                    base64::engine::fast_portable::PAD,
                ),
                "URL_SAFE_NO_PAD" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::URL_SAFE,
                    base64::engine::fast_portable::NO_PAD,
                ),
                "CRYPT" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::CRYPT,
                    base64::engine::fast_portable::PAD,
                ),
                "BCRYPT" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::BCRYPT,
                    base64::engine::fast_portable::PAD,
                ),
                "IMAP_MUTF7" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::IMAP_MUTF7,
                    base64::engine::fast_portable::PAD,
                ),
                "BIN_HEX" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::BIN_HEX,
                    base64::engine::fast_portable::PAD,
                ),
                _ => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::STANDARD,
                    base64::engine::fast_portable::PAD,
                ),
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::engine::fast_portable::FastPortable::from(
            &base64::alphabet::STANDARD,
            base64::engine::fast_portable::PAD,
        ),
    };
    let encode_string = base64::encode_engine(decode_string, &engine);

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
    let engine = match args.get("config") {
        Some(config) => match from_value::<String>(config.clone()) {
            Ok(config) => match config.to_uppercase().as_str() {
                "STANDARD_NO_PAD" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::STANDARD,
                    base64::engine::fast_portable::NO_PAD,
                ),
                "URL_SAFE" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::URL_SAFE,
                    base64::engine::fast_portable::PAD,
                ),
                "URL_SAFE_NO_PAD" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::URL_SAFE,
                    base64::engine::fast_portable::NO_PAD,
                ),
                "CRYPT" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::CRYPT,
                    base64::engine::fast_portable::PAD,
                ),
                "BCRYPT" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::BCRYPT,
                    base64::engine::fast_portable::PAD,
                ),
                "IMAP_MUTF7" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::IMAP_MUTF7,
                    base64::engine::fast_portable::PAD,
                ),
                "BIN_HEX" => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::BIN_HEX,
                    base64::engine::fast_portable::PAD,
                ),
                _ => base64::engine::fast_portable::FastPortable::from(
                    &base64::alphabet::STANDARD,
                    base64::engine::fast_portable::PAD,
                ),
            },
            Err(_) => {
                return Err(Error::msg(format!(
                    "Function `base64_decode` received config={} but `config` can only be a string",
                    config
                )));
            }
        },
        None => base64::engine::fast_portable::FastPortable::from(
            &base64::alphabet::STANDARD,
            base64::engine::fast_portable::PAD,
        ),
    };

    let decode_string = match base64::decode_engine(encode_string, &engine) {
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
        args.insert("value".to_string(), Value::String("bXlfdGVzdA==".to_string()));
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
}
