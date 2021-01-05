use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::io::prelude::*;

const DEFAULT_MIME: &str = "application/toml";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Toml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Toml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(DEFAULT_MIME.to_string()),
            ..Default::default()
        };
        Toml { metadata }
    }
}

impl Document for Toml {
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// Read toml data.
    ///
    /// # Example: Should read toml data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Toml::default();
    /// let connector = InMemory::new(r#"[Title]
    /// key_1 = "value_1"
    /// key_2 = "value_2"
    /// "#);
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    fn read_data(&self, connector: Box<dyn Connector>) -> io::Result<Data> {
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut string = String::new();
        let mut connector = connector;

        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(DEFAULT_MIME.to_string());
        connector.set_metadata(metadata.clone());
        connector.read_to_string(&mut string)?;

        let record: Value = toml::from_str(string.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let data = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            match record {
                Value::Array(records) => {
                    for record in records {
                        debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        co.yield_(DataResult::Ok(record)).await;
                    }
                }
                record => {
                    debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                    co.yield_(DataResult::Ok(record)).await;
                }
            };
            debug!(slog_scope::logger(), "End generator");
        });
        debug!(slog_scope::logger(), "Read data ended"; "documents" => format!("{:?}", self));
        Ok(data)
    }
    /// Write toml data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Toml::default();
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// "#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// column_1 = "line_2"
    /// "#, &format!("{}", connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Toml::default();
    /// let mut connector = InMemory::new(r#"column_1 = "line_1"
    /// "#);
    /// connector.can_truncate = true;
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_2"
    /// "#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"column_1 = "line_2"
    /// column_1 = "line_3"
    /// "#, &format!("{}", connector));
    /// ```
    fn write_data_result(
        &mut self,
        connector: &mut dyn Connector,
        data_result: DataResult,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        connector.set_parameters(value.clone());

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
        connector.write_all(toml.as_bytes())?;

        debug!(slog_scope::logger(), "Write data ended"; "data" => format!("{:?}", data_result));
        Ok(())
    }
    /// flush jsonl data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    /// use std::io::Read;
    ///
    /// let mut document = Toml::default();
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// "#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// column_1 = "line_2"
    /// "#, buffer);
    /// ```
    fn flush(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        debug!(slog_scope::logger(), "Flush called");
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(DEFAULT_MIME.to_string());
        connector.set_metadata(metadata.clone());
        connector.flush()?;
        debug!(slog_scope::logger(), "Flush with success");
        Ok(())
    }
}
