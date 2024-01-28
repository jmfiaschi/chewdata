use std::{env, fmt::Debug};

pub trait Obfuscate {
    /// Obfusctate a part of the object.
    fn to_obfuscate(&mut self) -> &mut String;
}

impl Obfuscate for String {
    /// obfuscate a part of the string.
    fn to_obfuscate(&mut self) -> &mut Self {
        self.replace_range(
            (self.len() / 2)..self.len(),
            (0..(self.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        self
    }
}

pub const LOG_DATA: &str = "LOG_DATA";
pub const MESSAGE_SEE_VALUE_IN_DEBUG_MODE: &str = "[set LOG_DATA=1 to see 🔎]";

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
        match env::var(LOG_DATA) {
            Ok(env) => match env.as_str() {
                "true" | "1" => format!("{:?}", self),
                _ => MESSAGE_SEE_VALUE_IN_DEBUG_MODE.to_string(),
            },
            _ => MESSAGE_SEE_VALUE_IN_DEBUG_MODE.to_string(),
        }
    }
}
