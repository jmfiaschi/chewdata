extern crate csv;

use crate::connector::Connector;
use crate::document::Document;
use crate::{Dataset, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use async_stream::stream;
use async_trait::async_trait;
use csv::Trim;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io;

const DEFAULT_QUOTE: &str = "\"";
const DEFAULT_DELIMITER: &str = ",";
const DEFAULT_HAS_HEADERS: bool = true;
const DEFAULT_ESCAPE: &str = "\\";
const DEFAULT_COMMENT: &str = "#";
const DEFAULT_TERMINATOR: &str = "\n";
const DEFAULT_QUOTE_STYLE: &str = "NOT_NUMERIC";
const DEFAULT_IS_FLEXIBLE: bool = true;
const DEFAULT_TRIM: &str = "ALL";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Csv {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_flexible: bool,
    pub quote_style: String,
    pub trim: String,
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
            mime_type: Some(mime::TEXT.to_string()),
            mime_subtype: Some(mime::CSV.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Csv {
            metadata,
            is_flexible: DEFAULT_IS_FLEXIBLE,
            trim: DEFAULT_TRIM.to_string(),
            quote_style: DEFAULT_QUOTE_STYLE.to_string(),
        }
    }
}

impl Csv {
    fn reader_builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::default();
        let metadata = self.metadata();

        builder.flexible(self.is_flexible);
        builder.trim(match self.trim.to_uppercase().as_str() {
            "ALL" => Trim::All,
            "FIELDS" | "FIELD" => Trim::Fields,
            "HEADERS" | "HEADER" => Trim::Headers,
            _ => Trim::None,
        });

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
            _ => builder.terminator(csv::Terminator::Any(
                *value.as_bytes().to_vec().first().unwrap(),
            )),
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
    ///     let mut dataset = document.read_data(&mut boxed_connector).await?;
    ///     let data_1 = dataset.next().await.unwrap().to_value();
    ///     let data_2 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_1: Value = serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#)?;
    ///     let expected_data_2: Value = serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#)?;
    ///     assert_eq!(expected_data_1, data_1);
    ///     assert_eq!(expected_data_2, data_2);
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
    /// use chewdata::DataResult;
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
    ///     let mut dataset = document.read_data(&mut boxed_connector).await?;
    ///     let data = dataset.next().await.unwrap();
    ///     match data {
    ///         DataResult::Ok(_) => assert!(false, "The line readed by the csv builder should be in error."),
    ///         DataResult::Err(_) => ()
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    fn read_with_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<Dataset> {
        Ok(Box::pin(stream! {
            let data = reader.into_deserialize::<Map<String, Value>>();
            for record in data {
                let data_result = match record {
                    Ok(record) => {
                        trace!(record = format!("{:?}",record).as_str(),  "Record deserialized");
                        DataResult::Ok(Value::Object(record))
                    }
                    Err(e) => {
                        warn!(error = format!("{:?}",e).as_str(),  "Can't deserialize the record");
                        if let super::csv::csv::ErrorKind::Io(_) = e.kind() {
                            return;
                        };

                        DataResult::Err((Value::Null, e.into()))
                    }
                };
                yield data_result;
            }
            trace!("End generator");
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
    ///     let mut dataset = document.read_data(&mut boxed_connector).await?;
    ///     let data_1 = dataset.next().await.unwrap().to_value();
    ///     let data_2 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_1 = Value::Array(vec![Value::String("A1".to_string()),Value::String("A2".to_string())]);
    ///     let expected_data_2 = Value::Array(vec![Value::String("B1".to_string()),Value::String("B2".to_string())]);
    ///     assert_eq!(expected_data_1, data_1);
    ///     assert_eq!(expected_data_2, data_2);
    ///
    ///     Ok(())
    /// }
    /// ```
    fn read_without_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<Dataset> {
        Ok(Box::pin(stream! {
            for record in reader.into_records() {
                let data_result = match record {
                    Ok(record) => {
                        trace!(record = format!("{:?}",record).as_str(),  "Record deserialized");
                        let map: Vec<Value> = record
                            .iter()
                            .map(|value| Value::String(value.to_string()))
                            .collect();
                        DataResult::Ok(Value::Array(map))
                    }
                    Err(e) => {
                        warn!(error = format!("{:?}",e).as_str(),  "Can't deserialize the record");
                        DataResult::Err((
                            Value::Null,
                            io::Error::new(io::ErrorKind::InvalidData, e),
                        ))
                    }
                };
                yield data_result;
            }
        }))
    }
}

#[async_trait]
impl Document for Csv {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Csv::default().metadata.merge(self.metadata.clone())
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
    ///     let mut metadata = Csv::default().metadata;
    ///     metadata.delimiter = Some("|".to_string());
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
    ///     let mut dataset = document.read_data(&mut boxed_connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(r#"{
    ///     "string":"My text",
    ///     "string_backspace":"My text with\n backspace",
    ///     "special_char":"€",
    ///     "int":10,
    ///     "float":9.5,
    ///     "bool":true
    ///     }"#)?;
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut buf = Vec::new();
        connector.read_to_end(&mut buf).await?;

        let cursor = io::Cursor::new(buf);
        let builder_reader = self.reader_builder().from_reader(cursor);
        let data = match self.metadata().has_headers {
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
    /// use chewdata::document::{DocumentType, csv::Csv};
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Csv::default();
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
    /// use chewdata::document::{DocumentType, csv::Csv};
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut metadata = Metadata::default();
    ///     metadata.delimiter = Some("|".to_string());
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
    #[instrument]
    async fn write_data(&mut self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        let write_header = connector.metadata().has_headers.unwrap_or_else(|| self.metadata().has_headers.unwrap_or(false));
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

                for (key, value) in object {
                    keys.push(key);
                    values.push(value);
                }

                if write_header {
                    builder_writer.write_record(keys)?;
                    let mut metadata = connector.metadata();
                    metadata.has_headers = Some(false);
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
