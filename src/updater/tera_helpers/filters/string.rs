use base64::Engine;
use regex::Regex;
use serde_json::value::Value;
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc, sync::OnceLock, sync::RwLock};
use tera::*;

type SharedEnv = Arc<RwLock<HashMap<String, String>>>;
static SHARED_ENV: OnceLock<SharedEnv> = OnceLock::new();

pub fn get_shared_environment_variables() -> &'static SharedEnv {
    SHARED_ENV.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

static REGEX_CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
fn cached_regex(pattern: &str) -> Result<Regex> {
    let cache = REGEX_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().map_err(Error::msg)?;

    if let Some(re) = cache.get(pattern) {
        return Ok(re.clone());
    }

    let re = Regex::new(pattern).map_err(|e| Error::msg(format!("Invalid regex: {e}")))?;

    cache.insert(pattern.to_string(), re.clone());
    Ok(re)
}

/// Returns encoded base64 string.
///
/// # Arguments
///
/// * `config` - Possible configuration: `standard_no_pad` | `url_safe` | `url_safe_no_pad` | `standard`
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::base64_encode;
/// use serde_json::json;
///
/// let value = json!("my_test");
/// let new_value = base64_encode(&value, &HashMap::new()).unwrap();
/// assert_eq!(json!("bXlfdGVzdA=="), new_value);
/// ```
pub fn base64_encode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let decode_string = match value {
        Value::String(s) => s,
        _ => {
            return Err(Error::msg(
                "Filter `base64_encode` require a string in input".to_string(),
            ))
        }
    };

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
/// * `config` - Possible configuration: `standard_no_pad` | `url_safe` | `url_safe_no_pad` | `standard`
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::base64_decode;
/// use serde_json::json;
///
/// let value = json!("bXlfdGVzdA==");
/// let new_value = base64_decode(&value, &HashMap::new()).unwrap();
/// assert_eq!(json!("my_test"), new_value);
/// ```
pub fn base64_decode(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    let encode_string = match value {
        Value::String(s) => s,
        _ => {
            return Err(Error::msg(
                "Filter `base64_decode` require a string in input".to_string(),
            ))
        }
    };

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

/// Set an environment variable.
///
/// Arguments:
///
/// * `name` - A string slice that contain the environment variable name.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::set_env;
/// use serde_json::json;
///
/// let value = json!("my_var");
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), json!("MY_KEY"));
///
/// let value = set_env(&value, &args).unwrap();
/// assert_eq!("my_var", value);
/// ```
pub fn set_env(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    // Use share environment map instead of system environment variables to avoid side effects with multi threading
    let shared_env = get_shared_environment_variables().clone();

    let value_string = match value {
        Value::String(s) => s,
        _ => return Err(Error::msg("Filter `set_env` require a string in input")),
    };

    // Extracting and validating the 'name' argument
    let name: String = args
        .get("name")
        .ok_or_else(|| Error::msg("Filter `set_env` didn't receive a `name` argument"))
        .and_then(|val| Ok(try_get_value!("env", "name", String, val)))?;

    // Avoiding to override the system environment variable
    let prefixed_key = format!("{}_{}", str::to_uppercase(crate::PROJECT_NAME), name);

    let mut env = shared_env.write().unwrap();
    env.insert(prefixed_key, value_string.clone());

    Ok(Value::String(value_string.clone()))
}

/// Returns a list of string found. See [https://docs.rs/regex/latest/regex/struct.Regex.html#method.find_iter](find_iter).
///
/// Arguments:
///
/// * `pattern` - regex expression to identify what you want to find from a string.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use serde_json::value::Value;
/// use chewdata::updater::tera_helpers::filters::string::find;
/// use serde_json::json;
///
/// let value = json!("Hello, world!");
/// let mut args = HashMap::new();
/// args.insert("pattern".to_string(), Value::String(r"\w+".to_string()));
///
/// let result = find(&value, &args).unwrap();
/// assert_eq!(
///     result,
///     json!([
///         "Hello",
///         "world"
///     ])
/// );
/// ```
pub fn find(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
    // Extracting and validating the 'value' argument
    let value_string = match value {
        Value::String(s) => s,
        _ => return Err(Error::msg("Filter `find` require a string in input")),
    };

    // Extracting and validating the 'pattern' argument
    let pattern = args
        .get("pattern")
        .ok_or_else(|| Error::msg("Function `find` didn't receive a `pattern` argument"))
        .and_then(|pattern| Ok(try_get_value!("find", "pattern", String, pattern)))?;

    // Creating a regex from the pattern
    let re = cached_regex(&pattern)?;

    // Collecting matching substrings into a Vec<Value>
    let vec = re
        .find_iter(value_string)
        .map(|s| Value::String(s.as_str().to_string()))
        .collect();

    Ok(Value::Array(vec))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ---------- Helpers ----------
    fn args(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn test_base64_encode() {
        // Test case 1: Default encoding (standard)
        let value = json!("Hello, world!");

        let result = base64_encode(&value, &HashMap::new()).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::STANDARD.encode("Hello, world!"))
        );

        // Test case 2: url_safe encoding
        let arguments = args(&[("config", json!("url_safe"))]);
        let result = base64_encode(&value, &arguments).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::URL_SAFE.encode("Hello, world!"))
        );

        // Test case 3: Custom encoding (standard_no_pad)
        let arguments = args(&[("config", json!("standard_no_pad"))]);
        let result = base64_encode(&value, &arguments).unwrap();
        assert_eq!(
            result,
            Value::String(base64::engine::general_purpose::STANDARD_NO_PAD.encode("Hello, world!"))
        );
    }
    #[test]
    fn test_base64_decode() {
        // Test case 1: Default decoding (STANDARD)
        let value =
            Value::String(base64::engine::general_purpose::STANDARD.encode("Hello, world!"));

        let result = base64_decode(&value, &HashMap::new()).unwrap();
        assert_eq!(result, json!("Hello, world!"));

        // Test case 2: URL_SAFE decoding
        let value =
            Value::String(base64::engine::general_purpose::URL_SAFE.encode("Hello, world!"));
        let arguments = args(&[("config", json!("URL_SAFE"))]);
        let result = base64_decode(&value, &arguments).unwrap();
        assert_eq!(result, json!("Hello, world!"));

        // Test case 3: Custom decoding (STANDARD_NO_PAD)
        let value =
            Value::String(base64::engine::general_purpose::STANDARD_NO_PAD.encode("Hello, world!"));
        let arguments = args(&[("config", json!("STANDARD_NO_PAD"))]);
        let result = base64_decode(&value, &arguments).unwrap();
        assert_eq!(result, json!("Hello, world!"));
    }
    #[test]
    fn test_set_env() {
        let value = json!("new_value");

        // Test case 1: Valid name and value
        let arguments = args(&[("name", json!("MY_ENV_VAR1"))]);
        let result = set_env(&value, &arguments).unwrap();
        assert_eq!(result, value);

        // Test case 2: Missing name argument
        let result = set_env(&value, &HashMap::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Filter `set_env` didn't receive a `name` argument"
        );
    }
    #[test]
    fn test_find() {
        // Test case 1: Valid pattern and value
        let value = json!("Hello, world!");
        let arguments = args(&[("pattern", json!("\\w+"))]);

        let result = find(&value, &arguments).unwrap();
        assert_eq!(result, json!(["Hello", "world"]));

        // Test case 2: Missing pattern argument
        let result = find(&value, &HashMap::new());
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Function `find` didn't receive a `pattern` argument"
        );
    }
}
