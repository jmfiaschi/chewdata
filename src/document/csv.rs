extern crate csv;

use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use async_trait::async_trait;
use futures::AsyncReadExt;
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io;

const DEFAULT_QUOTE: &str = "\"";
const DEFAULT_DELIMITER: &str = ",";
const DEFAULT_HAS_HEADERS: bool = true;
const DEFAULT_ESCAPE: &str = "\"";
const DEFAULT_COMMENT: &str = "#";
const DEFAULT_TERMINATOR: &str = "\n";
const DEFAULT_QUOTE_STYLE: &str = "NOT_NUMERIC";
const DEFAULT_IS_FLEXIBLE: bool = true;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default)]
pub struct Csv {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_flexible: bool,
    pub quote_style: String,
}

impl Default for Csv {
    fn default() -> Self {
        let metadata = Metadata {
            has_headers: Some(DEFAULT_HAS_HEADERS),
            delimiter: Some(DEFAULT_DELIMITER.to_string()),
            quote: Some(DEFAULT_QUOTE.to_string()),
            escape: Some(DEFAULT_ESCAPE.to_string()),
            comment: Some(DEFAULT_COMMENT.to_string()),
            terminator: Some(DEFAULT_TERMINATOR.to_string()),
            mime_type: Some(mime::TEXT_CSV_UTF_8.to_string()),
            ..Default::default()
        };
        Csv {
            metadata,
            is_flexible: DEFAULT_IS_FLEXIBLE,
            quote_style: DEFAULT_QUOTE_STYLE.to_string(),
        }
    }
}

impl Csv {
    fn reader_builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::default();
        let metadata = self.metadata();

        builder.flexible(self.is_flexible);

        metadata.has_headers.map(|value| builder.has_headers(value));
        metadata.clone().quote.map(|value| match value.as_str() {
            "\"" => builder.double_quote(true),
            _ => builder.double_quote(false),
        });
        metadata.clone().quote.map(|value| match value.as_str() {
            "'" | "\"" => builder.quoting(true),
            _ => builder.quoting(false),
        });
        metadata
            .clone()
            .quote
            .map(|value| builder.quote(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .clone()
            .delimiter
            .map(|value| builder.delimiter(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .clone()
            .escape
            .map(|value| builder.escape(Some(*value.as_bytes().to_vec().first().unwrap())));
        metadata
            .clone()
            .comment
            .map(|value| builder.comment(Some(*value.as_bytes().to_vec().first().unwrap())));
        metadata.terminator.map(|value| match value.as_str() {
            "CRLF" | "CR" | "LF" | "\n\r" => builder.terminator(csv::Terminator::CRLF),
            _ => builder.terminator(csv::Terminator::Any(
                *value.as_bytes().to_vec().first().unwrap(),
            )),
        });

        builder
    }
    fn writer_builder(&self) -> csv::WriterBuilder {
        let mut builder = csv::WriterBuilder::default();
        let metadata = self.metadata();

        builder.flexible(self.is_flexible);

        metadata
            .has_headers
            .map(|has_headers| builder.has_headers(has_headers));
        metadata.clone().quote.map(|value| match value.as_str() {
            "\"" => builder.double_quote(true),
            _ => builder.double_quote(false),
        });
        metadata
            .clone()
            .quote
            .map(|value| builder.quote(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .clone()
            .delimiter
            .map(|value| builder.delimiter(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .clone()
            .escape
            .map(|value| builder.escape(*value.as_bytes().to_vec().first().unwrap()));
        metadata.terminator.map(|value| match value.as_str() {
            "CRLF" | "CR" | "LF" | "\n\r" => builder.terminator(csv::Terminator::CRLF),
            _ => builder.terminator(csv::Terminator::Any(*value.as_bytes().to_vec().first().unwrap())),
        });
        match self.quote_style.clone().to_uppercase().as_ref() {
            "ALWAYS" => builder.quote_style(csv::QuoteStyle::Always),
            "NEVER" => builder.quote_style(csv::QuoteStyle::Never),
            "NECESSARY" => builder.quote_style(csv::QuoteStyle::Necessary),
            _ => builder.quote_style(csv::QuoteStyle::NonNumeric),
        };

        builder
    }
    /// Read csv data with header.
    ///
    /// # Example: Read csv document.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Csv::default();
    ///     let mut connector = InMemory::new("column1,column2\nA1,A2\nB1,B2\n");
    ///     connector.fetch().await?;
    ///     let mut boxed_connector: Box<dyn Connector> = Box::new(connector);
    ///
    ///     let mut data_iter = document.read_data(&mut boxed_connector).await?.into_iter();
    ///     let line_1 = data_iter.next().unwrap().to_json_value();
    ///     let line_2 = data_iter.next().unwrap().to_json_value();
    ///     let expected_line_1: Value = serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#)?;
    ///     let expected_line_2: Value = serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#)?;
    ///     assert_eq!(expected_line_1, line_1);
    ///     assert_eq!(expected_line_2, line_2);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Can't read csv document because not same column number.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Csv::default();
    ///     let mut connector = InMemory::new("column1,column2\nA1\n");
    ///     connector.fetch().await?;
    ///     let mut boxed_connector: Box<dyn Connector> = Box::new(connector);
    ///
    ///     let mut data_iter = document.read_data(&mut boxed_connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap();
    ///     match line {
    ///         DataResult::Ok(_) => assert!(false, "The line readed by the csv builder should be in error."),
    ///         DataResult::Err(_) => ()
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    fn read_with_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<Data> {
        Ok(GenBoxed::new_boxed(|co| async move {
            let data = reader.into_deserialize::<Map<String, Value>>();
            for record in data {
                let data_result = match record {
                    Ok(record) => {
                        debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        DataResult::Ok(Value::Object(record))
                    }
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't deserialize the record"; "error"=>format!("{:?}",e));
                        if let super::csv::csv::ErrorKind::Io(_) = e.kind() {
                            return;
                        };

                        DataResult::Err((Value::Null, e.into()))
                    }
                };
                co.yield_(data_result).await;
            }
            debug!(slog_scope::logger(), "End generator");
        }))
    }
    /// Read csv data without header.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut metadata = Metadata::default();
    ///     metadata.has_headers = Some(false);
    ///
    ///     let mut document = Csv::default();
    ///     document.metadata = metadata;
    ///
    ///     let mut connector = InMemory::new("A1,A2\nB1,B2\n");
    ///     connector.fetch().await?;
    ///     let mut boxed_connector: Box<dyn Connector> = Box::new(connector);
    ///
    ///     let mut data_iter = document.read_data(&mut boxed_connector).await?.into_iter();
    ///     let line_1 = data_iter.next().unwrap().to_json_value();
    ///     let line_2 = data_iter.next().unwrap().to_json_value();
    ///     let expected_line_1 = Value::Array(vec![Value::String("A1".to_string()),Value::String("A2".to_string())]);
    ///     let expected_line_2 = Value::Array(vec![Value::String("B1".to_string()),Value::String("B2".to_string())]);
    ///     assert_eq!(expected_line_1, line_1);
    ///     assert_eq!(expected_line_2, line_2);
    ///
    ///     Ok(())
    /// }
    /// ```
    fn read_without_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<Data> {
        Ok(GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            for record in reader.into_records() {
                let data_result = match record {
                    Ok(record) => {
                        debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        let map: Vec<Value> = record
                            .iter()
                            .map(|value| Value::String(value.to_string()))
                            .collect();
                        DataResult::Ok(Value::Array(map))
                    }
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't deserialize the record"; "error"=>format!("{:?}",e));
                        DataResult::Err((
                            Value::Null,
                            io::Error::new(io::ErrorKind::InvalidData, e),
                        ))
                    }
                };
                co.yield_(data_result).await;
            }
            debug!(slog_scope::logger(), "End generator");
        }))
    }
}

#[async_trait]
impl Document for Csv {
    fn metadata(&self) -> Metadata {
        self.metadata.clone().merge(Csv::default().metadata)
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut metadata = Metadata::default();
    ///     metadata.delimiter = Some([b'|']);
    ///
    ///     let mut document = Csv::default();
    ///     document.metadata = metadata;
    ///
    ///     let mut connector = InMemory::new(r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    ///     "#);
    ///     connector.fetch().await?;
    ///     let mut boxed_connector: Box<dyn Connector> = Box::new(connector);
    ///
    ///     let mut data_iter = document.read_data(&mut boxed_connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap().to_json_value();
    ///     let expected_line: Value = serde_json::from_str(r#"{
    ///     "string":"My text",
    ///     "string_backspace":"My text with\n backspace",
    ///     "special_char":"€",
    ///     "int":10,
    ///     "float":9.5,
    ///     "bool":true
    ///     }"#)?;
    ///     assert_eq!(expected_line, line);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Data> {
        let mut buf = Vec::new();
        connector.read_to_end(&mut buf).await?;

        let cursor = io::Cursor::new(buf);

        let builder_reader = self.reader_builder().from_reader(cursor);
        let data = match self.metadata.has_headers {
            Some(false) => Csv::read_without_header(builder_reader),
            _ => Csv::read_with_header(builder_reader),
        };

        data
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example: Add header if connector data empty.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Csv::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#""column_1"
    /// "line_1"
    /// "#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#""column_1"
    /// "line_1"
    /// "line_2"
    /// "#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: handle complex csv
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut metadata = Metadata::default();
    ///     metadata.delimiter = Some([b'|']);
    ///
    ///     let mut document = Csv::default();
    ///     document.metadata = metadata;
    ///
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let complex_value: Value = serde_json::from_str(r#"{
    ///     "string":"My text",
    ///     "string_backspace":"My text with\n backspace",
    ///     "special_char":"€",
    ///     "int":10,
    ///     "float":9.5,
    ///     "bool":true
    /// }"#).unwrap();
    ///
    ///     document.write_data(&mut connector, complex_value).await?;
    ///     let expected_str = r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    /// "#;
    ///     assert_eq!(expected_str, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        let write_header = match (
            self.metadata().has_headers,
            connector.metadata().has_headers,
        ) {
            (None, _) => false,
            (Some(false), _) => false,
            (_, Some(true)) => false,
            (_, _) => true,
        };
        // Use a buffer here because the csv builder flush everytime it write something.
        let mut builder_writer = self.writer_builder().from_writer(vec![]);

        match value {
            Value::Bool(value) => {
                builder_writer.serialize(value)?;
                Ok(())
            }
            Value::Number(value) => {
                builder_writer.serialize(value)?;
                Ok(())
            }
            Value::String(value) => {
                builder_writer.serialize(value)?;
                Ok(())
            }
            Value::Null => Ok(()),
            Value::Object(object) => {
                let mut values = Vec::<Value>::new();
                let mut keys = Vec::<String>::new();

                for (key, value) in object.clone() {
                    keys.push(key);
                    values.push(value);
                }

                if write_header {
                    builder_writer.write_record(keys)?;
                    let mut metadata = connector.metadata();
                    metadata.has_headers = Some(true);
                    connector.set_metadata(metadata);
                }
                builder_writer.serialize(values)?;
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("This object is not handle. {:?}", value),
            )),
        }?;

        connector
            .write_all(
                builder_writer
                    .into_inner()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                    .as_slice(),
            )
            .await
    }
}
