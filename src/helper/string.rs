use std::{env, fmt::Debug};

pub const OFUSCATE_CHAR: &str = "*";

pub trait Obfuscate {
    /// obfuscate the string.
    fn to_obfuscate(&self) -> String;
}

impl Obfuscate for String {
    /// obfuscate the string.
    fn to_obfuscate(&self) -> String {
        // Try URI-style obfuscation first
        if let Some(obfuscated) = obfuscate_uri_password(self) {
            return obfuscated;
        }

        // Fallback: generic obfuscation
        if let Some(obfuscated) = obfuscate_tail(self) {
            return obfuscated;
        }

        self.clone()
    }
}

fn obfuscate_uri_password(source: &str) -> Option<String> {
    let mut s = source.to_owned();
    let proto_end = s.find("://")?;
    let at_rel = s[proto_end + 3..].find('@')?;
    let at = proto_end + 3 + at_rel;

    let colon_rel = s[proto_end + 3..at].find(':')?;
    let colon = proto_end + 3 + colon_rel;

    let password_range = colon + 1..at;
    let password_len = password_range.end - password_range.start;

    let obfuscation = OFUSCATE_CHAR.repeat(password_len);
    s.replace_range(password_range, &obfuscation);

    Some(s)
}

fn obfuscate_tail(source: &str) -> Option<String> {
    let mut s = source.to_owned();
    let len = s.len();
    if len == 0 {
        return None;
    }

    let half = len / 2;
    let obfuscation = OFUSCATE_CHAR.repeat(len - half);
    s.replace_range(half..len, &obfuscation);
    Some(s)
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
