use base64::Engine;
use regex::Regex;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;
use uuid::Uuid;

/// Return a generated v4 uuid string.
///
/// # Arguments
///
/// * `format` - Possible format: `hyphenated` | `urn` | `simple` (default).
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::uuid_v4;
///
/// let args = HashMap::new();
/// let first_result = uuid_v4(&args);
/// let second_result = uuid_v4(&args);
/// assert!(first_result.is_ok());
/// assert!(second_result.is_ok());
/// ```
pub fn uuid_v4(args: &HashMap<String, Value>) -> Result<Value> {
    let uuid = Uuid::new_v4();

    let format = match args.get("format") {
        Some(val) => try_get_value!("uuid_v4", "format", String, val),
        None => String::default(),
    };

    let uuid_string = match format.as_str() {
        "hyphenated" => uuid.hyphenated().to_string(),
        "urn" => uuid.urn().to_string(),
        _ => uuid.simple().to_string(),
    };

    Ok(Value::String(uuid_string))
}

/// Returns encoded base64 string.
///
/// # Arguments
///
/// * `value` - A string slice to encode.
/// * `config` - Possible configuration: `standard_no_pad` | `url_safe` | `url_safe_no_pad` | `standard`
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::base64_encode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("my_test".to_string()));
/// let value = base64_encode(&args).unwrap();
/// assert_eq!("bXlfdGVzdA==", value.as_str().unwrap());
/// ```
pub fn base64_encode(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let decode_string: String = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `base64_encode` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("base64_encode", "value", String, val)))?;

    // Extracting and validating the 'config' argument
    let config = args
        .get("config")
        .map(|config| Ok(try_get_value!("base64_encode", "config", String, config)))
        .unwrap_or_else(|| Ok(String::from("standard")))?;

    // Encoding the string based on the specified config or using the standard config
    let encode_string = match config.to_uppercase().as_str() {
        "STANDARD_NO_PAD" => base64::engine::general_purpose::STANDARD_NO_PAD.encode(decode_string),
        "URL_SAFE" => base64::engine::general_purpose::URL_SAFE.encode(decode_string),
        "URL_SAFE_NO_PAD" => base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(decode_string),
        _ => base64::engine::general_purpose::STANDARD.encode(decode_string),
    };

    Ok(Value::String(encode_string))
}

/// Returns a decoded base64 string.
///
/// # Arguments
///
/// * `value` - A base64 string slice to decode.
/// * `config` - Possible configuration: `standard_no_pad` | `url_safe` | `url_safe_no_pad` | `standard`
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::base64_decode;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("bXlfdGVzdA==".to_string()));
/// let value = base64_decode(&args).unwrap();
/// assert_eq!("my_test", value.as_str().unwrap());
/// ```
pub fn base64_decode(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let encode_string: String = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `base64_decode` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("base64_decode", "value", String, val)))?;

    // Extracting and validating the 'config' argument
    let config = args
        .get("config")
        .map(|config| Ok(try_get_value!("base64_decode", "config", String, config)))
        .unwrap_or_else(|| Ok(String::from("strandard")))?;

    // Decoding the base64 string based on the specified config or using the standard config
    let decode_string = match config.to_uppercase().as_str() {
        "STANDARD_NO_PAD" => base64::engine::general_purpose::STANDARD_NO_PAD.decode(encode_string),
        "URL_SAFE" => base64::engine::general_purpose::URL_SAFE.decode(encode_string),
        "URL_SAFE_NO_PAD" => base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encode_string),
        _ => base64::engine::general_purpose::STANDARD.decode(encode_string),
    }
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
    .and_then(|res| {
        String::from_utf8(res).map_err(|e| std::io::Error::new(std::io::ErrorKind::Unsupported, e))
    })?;

    Ok(Value::String(decode_string))
}

/// Returns the environment variable.
///
/// # Arguments
///
/// * `name` - A string slice that contain the environment variable name.
/// * `default` - A string slice that contain the default environment variable value.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::env;
///
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), Value::String("my_key".to_string()));
///
/// std::env::set_var("chewdata:my_key", "my_var");
///
/// let value = env(&args).unwrap();
/// assert_eq!("my_var", value.as_str().unwrap());
/// ```
pub fn env(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'name' argument
    let name: String = args
        .get("name")
        .ok_or_else(|| Error::msg("Function `env` didn't receive a `name` argument"))
        .and_then(|val| Ok(try_get_value!("env", "name", String, val)))?;

    // Avoiding to override the system environment variable
    let var_env_key = format!("{}:{}", crate::PROJECT_NAME, name);

    // Try to get the environment variable value
    match std::env::var(var_env_key).ok() {
        Some(res) => Ok(Value::String(res)),
        None => {
            // If the environment variable is not found, check for a 'default' value in the arguments
            match args.get("default") {
                Some(default) => Ok(default.clone()),
                None => Err(Error::msg(format!(
                    "Environment variable `{}` not found",
                    &name
                ))),
            }
        }
    }
}

/// Set an environment variable.
///
/// Arguments:
///
/// * `name` - A string slice that contain the environment variable name.
/// * `value` - A string slice that contain the default environment variable value.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::set_env;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("my_var".to_string()));
/// args.insert("name".to_string(), Value::String("my_key".to_string()));
///
/// let value = set_env(&args).unwrap();
///
/// assert_eq!("my_var", value.as_str().unwrap());
/// let value = std::env::var("chewdata:my_key").unwrap();
/// assert_eq!("my_var", value);
/// ```
pub fn set_env(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let value_string: String = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `set_env` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("env", "value", String, val)))?;

    // Extracting and validating the 'name' argument
    let name: String = args
        .get("name")
        .ok_or_else(|| Error::msg("Function `set_env` didn't receive a `name` argument"))
        .and_then(|val| Ok(try_get_value!("env", "name", String, val)))?;

    // Avoiding to override the system environment variable
    let var_env_key = format!("{}:{}", crate::PROJECT_NAME, name);

    std::env::set_var(var_env_key, &value_string);

    Ok(Value::String(value_string))
}

/// Returns a list of string found. See [https://docs.rs/regex/latest/regex/struct.Regex.html#method.find_iter](find_iter).
///
/// Arguments:
///
/// * `pattern` - regex expression to identify what you want to find from a string.
/// * `value` - Value to analyse.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::function::string::find;
///
/// let mut args = HashMap::new();
/// args.insert("value".to_string(), Value::String("Hello, world!".to_string()));
/// args.insert("pattern".to_string(), Value::String(r"\w+".to_string()));
///
/// let result = find(&args).unwrap();
/// assert_eq!(
///     result,
///     Value::Array(vec![
///         Value::String("Hello".to_string()),
///         Value::String("world".to_string())
///     ])
/// );
/// ```
pub fn find(args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let value_string: String = args
        .get("value")
        .ok_or_else(|| Error::msg("Function `find` didn't receive a `value` argument"))
        .and_then(|val| Ok(try_get_value!("find", "value", String, val)))?;

    // Extracting and validating the 'pattern' argument
    let pattern = args
        .get("pattern")
        .ok_or_else(|| Error::msg("Function `find` didn't receive a `pattern` argument"))
        .and_then(|pattern| Ok(try_get_value!("find", "pattern", String, pattern)))?;

    // Creating a regex from the pattern
    let re = Regex::new(&pattern).map_err(Error::msg)?;

    // Collecting matching substrings into a Vec<Value>
    let vec = re
        .find_iter(&value_string)
        .map(|s| Value::String(s.as_str().to_string()))
        .collect();

    Ok(Value::Array(vec))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_v4() {
        // Test case 1: Default format (simple)
        let args = HashMap::new();
        let result = uuid_v4(&args).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 2: Hyphenated format
        let mut args = HashMap::new();
        args.insert(
            "format".to_string(),
            Value::String("hyphenated".to_string()),
        );
        let result = uuid_v4(&args).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 3: URN format
        let mut args = HashMap::new();
        args.insert("format".to_string(), Value::String("urn".to_string()));
        let result = uuid_v4(&args).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 4: Two generate uuid are not the same
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
    fn test_base64_encode() {
        // Test case 1: Default encoding (standard)
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("Hello, world!".to_string()),
        );

        let result = base64_encode(&args).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::STANDARD.encode("Hello, world!"))
        );

        // Test case 2: url_safe encoding
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("Hello, world!".to_string()),
        );
        args.insert("config".to_string(), Value::String("url_safe".to_string()));

        let result = base64_encode(&args).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::URL_SAFE.encode("Hello, world!"))
        );

        // Test case 3: Custom encoding (standard_no_pad)
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("Hello, world!".to_string()),
        );
        args.insert(
            "config".to_string(),
            Value::String("standard_no_pad".to_string()),
        );

        let result = base64_encode(&args).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::STANDARD_NO_PAD.encode("Hello, world!"))
        );
    }
    #[test]
    fn test_base64_decode() {
        // Test case 1: Default decoding (STANDARD)
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String(base64::engine::general_purpose::STANDARD.encode("Hello, world!")),
        );

        let result = base64_decode(&args).unwrap();
        assert_eq!(result, Value::String("Hello, world!".to_string()));

        // Test case 2: URL_SAFE decoding
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String(base64::engine::general_purpose::URL_SAFE.encode("Hello, world!")),
        );
        args.insert("config".to_string(), Value::String("URL_SAFE".to_string()));

        let result = base64_decode(&args).unwrap();
        assert_eq!(result, Value::String("Hello, world!".to_string()));

        // Test case 3: Custom decoding (STANDARD_NO_PAD)
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String(base64::engine::general_purpose::STANDARD_NO_PAD.encode("Hello, world!")),
        );
        args.insert(
            "config".to_string(),
            Value::String("STANDARD_NO_PAD".to_string()),
        );

        let result = base64_decode(&args).unwrap();
        assert_eq!(result, Value::String("Hello, world!".to_string()));
    }
    #[test]
    fn test_set_env() {
        let value = Value::String("new_value".to_string());

        // Test case 1: Valid name and value
        let mut args = HashMap::new();
        args.insert("value".to_string(), Value::String("new_value".to_string()));
        args.insert("name".to_string(), Value::String("MY_ENV_VAR".to_string()));

        let result = set_env(&args).unwrap();
        assert_eq!(result, value);

        // Test case 2: Missing name argument
        let mut args = HashMap::new();
        args.insert("value".to_string(), Value::String("new_value".to_string()));

        let result = set_env(&args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Function `set_env` didn't receive a `name` argument"
        );

        // Test case 3: Missing value argument
        let args = HashMap::new();

        let result = set_env(&args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Function `set_env` didn't receive a `value` argument"
        );
    }
    #[test]
    fn test_env() {
        // Test case 1: Environment variable exists
        let mut args = HashMap::new();
        args.insert("name".to_string(), Value::String("MY_ENV_VAR".to_string()));

        // Mock environment variable for testing
        std::env::set_var(format!("{}:MY_ENV_VAR", crate::PROJECT_NAME), "TestValue");

        let result = env(&args).unwrap();
        assert_eq!(result, Value::String("TestValue".to_string()));

        // Test case 2: Environment variable does not exist, but default value is provided
        let mut args = HashMap::new();
        args.insert(
            "name".to_string(),
            Value::String("NON_EXISTING_ENV_VAR".to_string()),
        );
        args.insert(
            "default".to_string(),
            Value::String("DefaultValue".to_string()),
        );

        let result = env(&args).unwrap();
        assert_eq!(result, Value::String("DefaultValue".to_string()));

        // Test case 3: Environment variable does not exist, and no default value is provided
        let mut args = HashMap::new();
        args.insert(
            "name".to_string(),
            Value::String("NON_EXISTING_ENV_VAR".to_string()),
        );

        let result = env(&args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Environment variable `NON_EXISTING_ENV_VAR` not found"
        );
    }
    #[test]
    fn test_find() {
        // Test case 1: Valid pattern and value
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("Hello, world!".to_string()),
        );
        args.insert("pattern".to_string(), Value::String(r"\w+".to_string()));

        let result = find(&args).unwrap();
        assert_eq!(
            result,
            Value::Array(vec![
                Value::String("Hello".to_string()),
                Value::String("world".to_string())
            ])
        );

        // Test case 2: Missing pattern argument
        let mut args = HashMap::new();
        args.insert(
            "value".to_string(),
            Value::String("Hello, world!".to_string()),
        );

        let result = find(&args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Function `find` didn't receive a `pattern` argument"
        );

        // Test case 3: Missing value argument
        let args = HashMap::new();

        let result = find(&args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Function `find` didn't receive a `value` argument"
        );
    }
}
