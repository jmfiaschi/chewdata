use std::fmt::Debug;
use tracing::{enabled, Level};

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

pub const MESSAGE_SEE_VALUE_IN_DEBUG_MODE: &str = "[Enable TRACE/DEBUG mode 🔎]";

pub trait DisplayOnlyForDebugging {
    /// Obfusctate a part of the object.
    fn display_only_for_debugging(&self) -> String;
}

impl<T> DisplayOnlyForDebugging for T
where
    T: Debug,
{
    fn display_only_for_debugging(&self) -> String {
        match enabled!(Level::DEBUG) {
            true => format!("{:?}", self),
            false => MESSAGE_SEE_VALUE_IN_DEBUG_MODE.to_string(),
        }
    }
}
