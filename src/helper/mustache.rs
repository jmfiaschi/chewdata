use super::json_pointer::JsonPointer;
use regex::Regex;
use serde_json::Value;

const MUSTACHE_PATTERN: &str = "\\{{2}[^}]*\\}{2}";

/// Trait used to apply actions on an object with mustache pattern.
pub trait Mustache {
    /// Check if the object has mustache pattern.
    fn has_mustache(&self) -> bool;
    /// Replace mustache pattern by an object value.
    fn replace_mustache(&self, object: Value) -> String;
}

impl Mustache for String {
    /// Test if the string contain mustache pattern.
    ///
    /// # Examples
    /// ```
    /// use serde_json::Value;
    /// use chewdata::helper::mustache::Mustache;
    ///
    /// let mustache_string = "my value: {{ field }}".to_string();
    /// assert_eq!(true, mustache_string.has_mustache());
    /// let mustache_string = "my value".to_string();
    /// assert_eq!(false, mustache_string.has_mustache());
    /// ```
    fn has_mustache(&self) -> bool {
        let reg = Regex::new(MUSTACHE_PATTERN).unwrap();
        reg.is_match(self.as_ref())
    }
    /// Replace.
    ///
    /// # Examples: Merge two array together.
    /// ```
    /// use serde_json::Value;
    /// use json_value_merge::Merge;
    /// use chewdata::helper::mustache::Mustache;
    ///
    /// let path = "my_path/{{ field_1 }}/{{ field_2 }}".to_string();
    ///
    /// let mut parameters = Value::default();
    /// parameters.merge_in("/field_1", Value::String("var_1".to_string()));
    /// parameters.merge_in("/field_2", Value::String("var_2".to_string()));
    ///
    /// assert_eq!("my_path/var_1/var_2", path.replace_mustache(parameters).as_str());
    /// ```
    /// # Examples: Not remplace the pattern
    /// ```
    /// use serde_json::Value;
    /// use json_value_merge::Merge;
    /// use chewdata::helper::mustache::Mustache;
    ///
    /// let path = "my_path/{{ field_1 }}".to_string();
    ///
    /// let mut parameters = Value::default();
    /// parameters.merge_in("/field_2", Value::String("var_2".to_string()));
    ///
    /// assert_eq!("my_path/{{ field_1 }}", path.replace_mustache(parameters).as_str());
    /// ```
    fn replace_mustache(&self, object: Value) -> String {
        let mut resolved_path = self.to_owned();

        if let Value::Null = object {
            return resolved_path;
        }

        let regex = Regex::new("\\{{2}([^}]*)\\}{2}").unwrap();
        for captured in regex.captures_iter(self.as_ref()) {
            let pattern_captured = captured[0].to_string();
            let value_captured = captured[1].trim().to_string();
            let json_pointer = value_captured.to_string().to_json_pointer();

            let var: String = match object.pointer(&json_pointer) {
                Some(Value::String(string)) => string.to_string(),
                Some(Value::Number(number)) => format!("{}", number),
                Some(Value::Bool(boolean)) => format!("{}", boolean),
                None => {
                    warn!(slog_scope::logger(),
                        "replace_mustache: Can't resolve";
                        "value" => value_captured,
                        "json_pointer" => json_pointer,
                        "object" => format!("{:?}", object),
                    );
                    continue;
                }
                Some(_) => {
                    warn!(slog_scope::logger(),
                        "replace_mustache: This parameter is not handle, only scalar";
                        "parameter" => format!("{:?}", object.pointer(&json_pointer))
                    );
                    continue;
                }
            };

            resolved_path = resolved_path.replace(pattern_captured.as_str(), var.as_str());
        }

        resolved_path
    }
}
