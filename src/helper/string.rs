use std::{env, fmt::Debug};

pub trait Obfuscate {
    /// Obfusctate a part of the object.
    fn to_obfuscate(&mut self) -> &mut String;
}

impl Obfuscate for String {
    /// obfuscate a part of the string.
    fn to_obfuscate(&mut self) -> &mut Self {
        let len = self.len();
        if len == 0 {
            return self;
        }

        let half = len / 2;
        let obfuscation = "#".repeat(len - half); // second half replaced by #
        self.replace_range(half..len, &obfuscation);

        self
    }
}

pub const LOG_DATA: &str = "LOG_DATA";
pub const MESSAGE_SEE_VALUE_IN_DEBUG_MODE: &str = "[HIDDEN: set LOG_DATA=1 🔎]";

pub trait DisplayOnlyForDebugging {
    /// Obfusctate a part of the object.
    fn display_only_for_debugging(&self) -> String;
}

// Display data only if the environment variable LOG_DATA = 1.
// By default = 0
impl<T> DisplayOnlyForDebugging for T
where
    T: Debug,
{
    fn display_only_for_debugging(&self) -> String {
        if log_enabled() {
            format!("{:?}", self)
        } else {
            MESSAGE_SEE_VALUE_IN_DEBUG_MODE.to_string()
        }
    }
}

/// Check if logging of full data is enabled
fn log_enabled() -> bool {
    match env::var(LOG_DATA) {
        Ok(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"),
        _ => false,
    }
}
