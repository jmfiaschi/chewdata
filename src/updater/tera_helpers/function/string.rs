use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;
use uuid::Uuid;

use crate::updater::tera_helpers::filters::string::get_shared_environment_variables;

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
/// args.insert("name".to_string(), Value::String("MY_KEY_WITH_PREFIX".to_string()));
///
/// std::env::set_var("CHEWDATA_MY_KEY_WITH_PREFIX", "my_var");
///
/// let value = env(&args).unwrap();
/// assert_eq!("my_var", value.as_str().unwrap());
///
/// let mut args = HashMap::new();
/// args.insert("name".to_string(), Value::String("MY_KEY_WITHOUT_PREFIX".to_string()));
///
/// std::env::set_var("MY_KEY_WITHOUT_PREFIX", "my_var");
///
/// let value = env(&args).unwrap();
/// assert_eq!("my_var", value.as_str().unwrap());
/// ```
pub fn env(args: &HashMap<String, Value>) -> Result<Value> {
    // Use share environment map instead of system environment variables to avoid side effects with multi threading
    let shared_env = get_shared_environment_variables().clone();

    // Extracting and validating the 'name' argument
    let name: String = args
        .get("name")
        .ok_or_else(|| Error::msg("Function `env` didn't receive a `name` argument"))
        .and_then(|val| Ok(try_get_value!("env", "name", String, val)))?;

    let prefixed_key = format!("{}_{}", crate::PROJECT_NAME.to_uppercase(), name);

    // First check in the shared environment map
    {
        let env = shared_env.read().unwrap();

        if let Some(value) = env.get(&prefixed_key) {
            return Ok(Value::String(value.clone()));
        }

        if let Some(value) = env.get(&name) {
            return Ok(Value::String(value.clone()));
        }
    }

    // Then check in the system environment variables
    let value = std::env::var(&prefixed_key)
        .or_else(|_| std::env::var(&name))
        .map(|var| {
            // Store in the shared environment map to avoid multiple system calls
            let mut env = shared_env.write().unwrap();
            env.insert(prefixed_key.clone(), var.clone());

            Value::String(var)
        })
        .or_else(|_| {
            args.get("default")
                .cloned()
                .ok_or_else(|| Error::msg(format!("Environment variable `{}` not found", name)))
        })?;

    Ok(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::updater::tera_helpers::filters::string::set_env;

    use super::*;

    // ---------- Helpers ----------
    fn args(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn test_uuid_v4() {
        // Test case 1: Default format (simple)
        let result = uuid_v4(&HashMap::new()).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 2: Hyphenated format
        let arguments = args(&[("format", json!("hyphenated"))]);
        let result = uuid_v4(&arguments).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 3: URN format
        let arguments = args(&[("format", json!("urn"))]);
        let result = uuid_v4(&arguments).unwrap();
        let uuid = Uuid::parse_str(&result.as_str().unwrap()).unwrap();
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
        assert_eq!(uuid.get_version(), Some(uuid::Version::Random));

        // Test case 4: Two generate uuid are not the same
        let first_result = uuid_v4(&HashMap::new());
        let second_result = uuid_v4(&HashMap::new());
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        let first_value = first_result.unwrap();
        let second_value = second_result.unwrap();
        assert_ne!(first_value, second_value);
    }
    #[test]
    fn test_env() {
        // Test case 1: Environment variable exists
        let value = json!("TestValue");
        let arguments = args(&[("name", json!("MY_ENV_VAR2"))]);
        set_env(&value, &arguments).unwrap();
        let result = env(&arguments).unwrap();
        assert_eq!(result, Value::String("TestValue".to_string()));

        // Test case 2: Environment variable does not exist, but default value is provided
        let arguments = args(&[
            ("name", json!("NON_EXISTING_ENV_VAR")),
            ("default", json!("DefaultValue")),
        ]);
        let result = env(&arguments).unwrap();
        assert_eq!(result, json!("DefaultValue"));

        // Test case 3: Environment variable does not exist, and no default value is provided
        let arguments = args(&[("name", json!("NON_EXISTING_ENV_VAR"))]);
        let result = env(&arguments);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Environment variable `NON_EXISTING_ENV_VAR` not found"
        );
    }
}
