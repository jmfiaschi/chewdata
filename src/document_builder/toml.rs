use crate::connector::Connector;
use crate::document_builder::Build;
use crate::processor::{Data, DataResult};
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::io::prelude::*;
use toml;

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(default)]
pub struct Toml {
    pub connector: Connector,
}

impl Default for Toml {
    fn default() -> Self {
        Toml {
            connector: Connector::default(),
        }
    }
}

impl PartialEq for Toml {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}

impl Build for Toml {
    /// Read toml data.
    ///
    /// # Example: Should read toml data.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::toml::Toml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut toml = Toml::default();
    /// toml.connector = Connector::Text(Text::new(r#"[Title]
    /// key_1 = "value_1"
    /// key_2 = "value_2"
    /// "#));
    ///
    /// let mut data_iter = toml.read_data().unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    fn read_data(&self) -> io::Result<Data> {
        trace!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut string = String::new();
        self.connector
            .clone()
            .reader()
            .read_to_string(&mut string)?;
        let record: Value = toml::from_str(string.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator");
            match record {
                Value::Array(records) => {
                    for record in records {
                        trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        co.yield_(DataResult::Ok(record)).await;
                    }
                }
                record => {
                    trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                    co.yield_(DataResult::Ok(record)).await;
                }
            };
            trace!(slog_scope::logger(), "End generator");
        });
        trace!(slog_scope::logger(), "Read data ended");
        Ok(data)
    }
    /// Write toml data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::toml::Toml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut toml = Toml::default();
    /// toml.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// "#, &format!("{}", toml.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// column_1 = "line_2"
    /// "#, &format!("{}", toml.connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::toml::Toml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut toml = Toml::default();
    /// let mut text = Text::new(r#"column_1 = "line_1"
    /// "#);
    /// text.truncate = true;
    /// toml.connector = Connector::Text(text);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_2"
    /// "#, &format!("{}", toml.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_2"
    /// column_1 = "line_3"
    /// "#, &format!("{}", toml.connector));
    /// ```
    fn write_data_result(&mut self, data_result: DataResult) -> io::Result<()> {
        trace!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();

        self.connector.get_mut().set_path_parameters(value.clone());

        // Transform serde_json::Value to toml::Value
        let toml_value = toml::value::Value::try_from(&value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let mut toml = String::new();
        toml_value
            .serialize(&mut toml::Serializer::new(&mut toml))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Can't write the data into the connector. {}", e),
                )
            })?;
        self.connector.writer().write(toml.as_bytes())?;

        trace!(slog_scope::logger(), "Write data ended.");
        Ok(())
    }
    /// flush jsonl data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::toml::Toml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut toml = Toml::default();
    /// toml.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// toml.flush().unwrap();
    /// let mut buffer = String::default();
    /// toml.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// "#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// toml.write_data_result(DataResult::Ok(value)).unwrap();
    /// toml.flush().unwrap();
    /// let mut buffer = String::default();
    /// toml.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// column_1 = "line_2"
    /// "#, buffer);
    /// ```
    fn flush(&mut self) -> io::Result<()> {
        trace!(slog_scope::logger(), "Flush called.");
        self.connector
            .get_mut()
            .set_mime_type("application/toml".parse::<mime::Mime>().unwrap());
        self.connector.get_mut().flush()?;
        trace!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
    fn connector(&self) -> &Connector {
        &self.connector
    }
}
