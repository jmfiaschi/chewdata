pub mod authenticator;
pub mod bucket;
pub mod curl;
pub mod io;
pub mod local;
pub mod text;

use self::bucket::Bucket;
use self::curl::Curl;
use self::io::Io;
use self::local::Local;
use self::text::Text;
use crate::FieldPath;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum Connector {
    #[serde(rename = "text")]
    Text(Text),
    #[serde(rename = "io")]
    Io(Io),
    #[serde(rename = "local")]
    Local(Local),
    #[serde(rename = "bucket")]
    Bucket(Bucket),
    #[serde(rename = "curl")]
    Curl(Curl),
}

impl Default for Connector {
    fn default() -> Self {
        Connector::Io(Io::default())
    }
}

impl std::fmt::Display for Connector {
    /// Display a inner buffer into `Connector`.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, text::Text};
    /// use std::io::Write;
    ///
    /// let mut connector = Connector::Text(Text::new(""));
    /// connector.writer().write_all("My text".to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Connector::Text(connector) => write!(f, "{}", connector),
            Connector::Io(connector) => write!(f, "{}", connector),
            Connector::Local(connector) => write!(f, "{}", connector),
            Connector::Bucket(connector) => write!(f, "{}", connector),
            Connector::Curl(connector) => write!(f, "{}", connector),
        }
    }
}

impl Connector {
    pub fn inner(self) -> Box<dyn Connect> {
        match self {
            Connector::Text(connector) => Box::new(connector),
            Connector::Io(connector) => Box::new(connector),
            Connector::Local(connector) => Box::new(connector),
            Connector::Bucket(connector) => Box::new(connector),
            Connector::Curl(connector) => Box::new(connector),
        }
    }
    pub fn get(&self) -> Box<&dyn Connect> {
        match self {
            Connector::Text(connector) => Box::new(connector),
            Connector::Io(connector) => Box::new(connector),
            Connector::Local(connector) => Box::new(connector),
            Connector::Bucket(connector) => Box::new(connector),
            Connector::Curl(connector) => Box::new(connector),
        }
    }
    pub fn get_mut(&mut self) -> Box<&mut dyn Connect> {
        match self {
            Connector::Text(connector) => Box::new(connector),
            Connector::Io(connector) => Box::new(connector),
            Connector::Local(connector) => Box::new(connector),
            Connector::Bucket(connector) => Box::new(connector),
            Connector::Curl(connector) => Box::new(connector),
        }
    }
    pub fn reader(&mut self) -> Box<&mut dyn Connect> {
        self.get_mut()
    }
    pub fn writer(&mut self) -> Box<&mut dyn Connect> {
        self.get_mut()
    }
}

/// Struct that implement this trait can get a reader or writer in order to do something on a document.
pub trait Connect: Read + Write + Send {
    /// Set path parameters.
    fn set_path_parameters(&mut self, parameters: Value);
    /// Get the resolved path.
    fn path(&self) -> String;
    /// Get the connect buffer inner reference.
    fn inner(&self) -> &Vec<u8>;
    /// Check if the connector has data into the inner buffer.
    fn is_empty(&self) -> Result<bool>;
    /// Get the truncate value.
    fn will_be_truncated(&self) -> bool;
    /// Append the inner buffer into the end of the document and flush the inner buffer.
    fn seek_and_flush(&mut self, _position: i64) -> Result<()> {
        self.flush()
    }
    /// Get the total document size.
    fn len(&self) -> Result<usize> {
        Err(Error::new(ErrorKind::NotFound, "function not implemented"))
    }
    /// Set the mime type header of the document. Can be necessary for a connector.
    fn set_mime_type(&mut self, _mime_type: mime::Mime) -> () {}
}

/// Resolve path with varaible.
fn resolve_path(path: String, parameters: Value) -> String {
    trace!(slog_scope::logger(),
        "Resolve path";
        "path" => path.to_owned(),
        "parameters" => format!("{}", parameters)
    );

    let mut resolved_path = path.to_owned();
    let regex = Regex::new("\\{\\{([^}]*)\\}\\}").unwrap();
    for captured in regex.clone().captures_iter(path.as_ref()) {
        let pattern_captured = captured[0].to_string();
        let value_captured = captured[1].trim().to_string();
        let json_pointer = FieldPath::new(value_captured.to_string()).to_json_pointer();

        let var: String = match parameters.pointer(&json_pointer) {
            Some(Value::String(string)) => string.to_string(),
            Some(Value::Number(number)) => format!("{}", number),
            Some(Value::Bool(boolean)) => format!("{}", boolean),
            None => {
                warn!(slog_scope::logger(),
                    "Can't resolve";
                    "value" => value_captured
                );
                continue;
            }
            Some(_) => {
                warn!(slog_scope::logger(),
                    "This parameter is not handle, only scalar";
                    "parameter" => format!("{:?}", parameters.pointer(&json_pointer))
                );
                continue;
            }
        };

        resolved_path = resolved_path.replace(pattern_captured.as_str(), var.as_str());
    }

    trace!(slog_scope::logger(), "Resolve path ended"; "path" => resolved_path.to_owned());
    resolved_path
}

#[cfg(test)]
mod connector {
    use super::*;
    use json_value_merge::Merge;

    #[test]
    fn it_should_resolve_the_path() {
        let path = "my_path/{{ field_1 }}/{{ field_2 }}".to_string();

        let mut parameters = Value::default();
        parameters.merge_in("/field_1", Value::String("var_1".to_string()));
        parameters.merge_in("/field_2", Value::String("var_2".to_string()));

        let new_path = resolve_path(path, parameters);
        assert_eq!("my_path/var_1/var_2", new_path.as_str());
    }
}
