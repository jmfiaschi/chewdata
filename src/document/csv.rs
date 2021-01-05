extern crate csv;

use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use genawaiter::sync::GenBoxed;
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default)]
pub struct Csv {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_flexible: bool,
    pub quote_style: String,
    #[serde(skip)]
    header_added: bool,
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
            header_added: false,
        }
    }
}

impl Csv {
    fn reader_builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::default();
        let metadata = self.metadata.clone();

        builder.flexible(self.is_flexible);

        metadata.has_headers.map(|value| builder.has_headers(value));
        metadata.clone().quote.map(|value| match value.as_str() {
            "\"" => builder.double_quote(true),
            _ => builder.double_quote(false),
        });
        metadata.clone().quote.map(|value| match value.as_str() {
            "\'" | "\"" => builder.quoting(true),
            _ => builder.quoting(false),
        });
        metadata
            .clone()
            .quote
            .map(|value| builder.quote(*value.as_bytes().first().unwrap()));
        metadata
            .clone()
            .delimiter
            .map(|value| builder.delimiter(*value.as_bytes().first().unwrap()));
        metadata
            .clone()
            .escape
            .map(|value| builder.escape(Some(*value.as_bytes().first().unwrap())));
        metadata
            .clone()
            .comment
            .map(|value| builder.comment(Some(*value.as_bytes().first().unwrap())));
        metadata
            .terminator
            .map(|value| match value.to_uppercase().as_str() {
                "CRLF" => builder.terminator(csv::Terminator::CRLF),
                _ => builder.terminator(csv::Terminator::Any(
                    *value.to_uppercase().as_str().as_bytes().first().unwrap(),
                )),
            });

        builder
    }
    fn writer_builder(&self) -> csv::WriterBuilder {
        let mut builder = csv::WriterBuilder::default();
        let metadata = self.metadata.clone();

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
            .map(|value| builder.quote(*value.as_bytes().first().unwrap()));
        metadata
            .clone()
            .delimiter
            .map(|value| builder.delimiter(*value.as_bytes().first().unwrap()));
        metadata
            .clone()
            .escape
            .map(|value| builder.escape(*value.as_bytes().first().unwrap()));
        metadata
            .terminator
            .map(|value| match value.to_uppercase().as_str() {
                "CRLF" => builder.terminator(csv::Terminator::CRLF),
                _ => builder.terminator(csv::Terminator::Any(
                    *value.to_uppercase().as_str().as_bytes().first().unwrap(),
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
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Csv::default();
    /// let connector = Box::new(InMemory::new("column1,column2\nA1,A2\nB1,B2\n"));
    ///
    /// let mut data_iter = document.read_data(connector).unwrap().into_iter();
    /// let line_1 = data_iter.next().unwrap().to_json_value();
    /// let line_2 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_1: Value = serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#).unwrap();
    /// let expected_line_2: Value = serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#).unwrap();
    /// assert_eq!(expected_line_1, line_1);
    /// assert_eq!(expected_line_2, line_2);
    /// ```
    /// # Example: Can't read csv document because not same column number.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Csv::default();
    /// let connector = Box::new(InMemory::new("column1,column2\nA1\n"));
    ///
    /// let mut data_iter = document.read_data(connector).unwrap().into_iter();
    /// let line = data_iter.next().unwrap();
    /// match line {
    ///     DataResult::Ok(_) => assert!(false, "The line readed by the csv builder should be in error."),
    ///     DataResult::Err(_) => ()
    /// };
    /// ```
    fn read_with_header(reader: csv::Reader<Box<dyn Connector>>) -> io::Result<Data> {
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
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    ///
    /// let mut metadata = Metadata::default();
    /// metadata.has_headers = Some(false);
    ///
    /// let mut document = Csv::default();
    /// document.metadata = metadata;
    ///
    /// let connector = Box::new(InMemory::new("A1,A2\nB1,B2\n"));
    ///
    /// let mut data_iter = document.read_data(connector).unwrap().into_iter();
    /// let line_1 = data_iter.next().unwrap().to_json_value();
    /// let line_2 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_1 = Value::Array(vec![Value::String("A1".to_string()),Value::String("A2".to_string())]);
    /// let expected_line_2 = Value::Array(vec![Value::String("B1".to_string()),Value::String("B2".to_string())]);
    /// assert_eq!(expected_line_1, line_1);
    /// assert_eq!(expected_line_2, line_2);
    /// ```
    fn read_without_header(reader: csv::Reader<Box<dyn Connector>>) -> io::Result<Data> {
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

impl Document for Csv {
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// Read complex csv data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    ///
    /// let mut metadata = Metadata::default();
    /// metadata.delimiter = Some("|".to_string());
    ///
    /// let mut document = Csv::default();
    /// document.metadata = metadata;
    ///
    /// let connector = Box::new(InMemory::new(r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    /// "#));
    ///
    /// let mut data_iter = document.read_data(connector).unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(r#"{
    /// "string":"My text",
    /// "string_backspace":"My text with\n backspace",
    /// "special_char":"€",
    /// "int":10,
    /// "float":9.5,
    /// "bool":true
    /// }"#).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    fn read_data(&self, connector: Box<dyn Connector>) -> io::Result<Data> {
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut connector = connector;
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_CSV_UTF_8.to_string());
        connector.set_metadata(metadata.clone());
        let builder_reader = self.reader_builder().from_reader(connector);
        let data = match metadata.has_headers {
            Some(false) => Csv::read_without_header(builder_reader),
            _ => Csv::read_with_header(builder_reader),
        };
        debug!(slog_scope::logger(), "Read data ended"; "documents" => format!("{:?}", self));
        data
    }
    /// Write complex csv data.
    ///
    /// # Example: Add header if connector data empty or if the connector will truncate the previous data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Csv::default();
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_1"
    /// "#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_1"
    /// "line_2"
    /// "#, &format!("{}", connector));
    /// ```
    /// # Example: truncate and write data into the connector
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Csv::default();
    /// let mut connector = InMemory::new(r#""column_1"
    /// "line_1"
    /// "line_2""#);
    /// connector.can_truncate = true;
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_3"
    /// "#, &format!("{}", connector));
    /// ```
    /// # Example: handle complex csv
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut metadata = Metadata::default();
    /// metadata.delimiter = Some("|".to_string());
    ///
    /// let mut document = Csv::default();
    /// document.metadata = metadata;
    ///
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// let complex_value: Value = serde_json::from_str(r#"{
    /// "string":"My text",
    /// "string_backspace":"My text with\n backspace",
    /// "special_char":"€",
    /// "int":10,
    /// "float":9.5,
    /// "bool":true
    /// }"#).unwrap();
    /// let data_result = DataResult::Ok(complex_value);
    ///
    /// document.write_data_result(&mut connector, data_result).unwrap();
    /// let expected_str = r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    /// "#;
    /// assert_eq!(expected_str, &format!("{}", connector));
    /// ```
    fn write_data_result(
        &mut self,
        connector: &mut dyn Connector,
        data_result: DataResult,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        let metadata = self.metadata.clone();
        let has_header = metadata.has_headers.unwrap_or(true);

        connector.set_parameters(value.clone());

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
                // Write header in these cases:
                // builder::add_header	builder::header_added   connector::truncate	connector::empty 	add_header?
                // 1			        0                       1			        1		            1
                // 1			        0                       1			        0		            1
                // 1			        0                       0			        1		            1
                if has_header
                    && !self.header_added
                    && (connector.will_be_truncated() || connector.is_empty()?)
                {
                    self.header_added = true;
                    builder_writer.write_record(keys)?;
                }
                builder_writer.serialize(values)?;
                Ok(())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("This object is not handle. {:?}", value),
            )),
        }?;

        connector.write_all(builder_writer.into_inner().unwrap().as_slice())?;

        debug!(slog_scope::logger(), "Write data ended"; "data" => format!("{:?}", data_result));
        Ok(())
    }
    /// Push data from the inner buffer into the document and flush the connector.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Csv::default();
    /// let mut connector = InMemory::new(r#"My InMemory"#);
    ///
    /// document.flush(&mut connector).unwrap();
    /// assert_eq!(r#""#, &format!("{}", connector));
    /// ```
    fn flush(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        debug!(slog_scope::logger(), "Flush called");
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_CSV_UTF_8.to_string());
        connector.set_metadata(metadata.clone());
        connector.flush()?;
        self.header_added = false;
        debug!(slog_scope::logger(), "Flush with success");
        Ok(())
    }
}
