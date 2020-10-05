extern crate csv;

use crate::connector::{Connect, Connector};
use crate::document_builder::Build;
use crate::processor::{Data, DataResult};
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Csv {
    pub connector: Connector,
    pub header: bool,
    pub flexible: bool,
    pub double_quote: bool,
    pub quoting: bool,
    pub delimiter: String,
    pub quote: String,
    pub escape: String,
    pub comment: String,
    pub trim: String,
    pub terminator: String,
    pub quote_style: String,
    #[serde(skip)]
    header_added: bool,
}

impl Default for Csv {
    fn default() -> Self {
        Csv {
            header: true,
            flexible: false,
            double_quote: true,
            quoting: true,
            delimiter: ",".to_string(),
            quote: "\"".to_string(),
            escape: "\\".to_string(),
            comment: "#".to_string(),
            trim: "ALL".to_string(),
            terminator: "\n".to_string(),
            quote_style: "NOT_NUMERIC".to_string(),
            connector: Connector::default(),
            header_added: false,
        }
    }
}

impl PartialEq for Csv {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}

impl Csv {
    fn reader_builder(&self) -> csv::ReaderBuilder {
        let mut reader_builder = csv::ReaderBuilder::default();

        reader_builder.has_headers(self.header);
        reader_builder.flexible(self.flexible);
        reader_builder.double_quote(self.double_quote);
        reader_builder.quoting(self.quoting);
        reader_builder.delimiter(*self.delimiter.as_bytes().first().unwrap());
        reader_builder.quote(*self.quote.as_bytes().first().unwrap());
        reader_builder.escape(Some(*self.escape.as_bytes().first().unwrap()));
        reader_builder.comment(Some(*self.comment.as_bytes().first().unwrap()));
        match self.trim.clone().to_uppercase().as_ref() {
            "HEADER" => reader_builder.trim(csv::Trim::Headers),
            "BODY" => reader_builder.trim(csv::Trim::Fields),
            "NONE" => &mut reader_builder,
            "ALL" | _ => reader_builder.trim(csv::Trim::All),
        };
        match self.terminator.clone().to_uppercase().as_ref() {
            "CRLF" => reader_builder.terminator(csv::Terminator::CRLF),
            _ => reader_builder.terminator(csv::Terminator::Any(
                *self.terminator.clone().as_bytes().first().unwrap(),
            )),
        };

        reader_builder
    }
    fn writer_builder(&self) -> csv::WriterBuilder {
        let mut writer_builder = csv::WriterBuilder::default();

        writer_builder.has_headers(self.header);
        writer_builder.flexible(self.flexible);
        writer_builder.double_quote(self.double_quote);
        writer_builder.delimiter(*self.delimiter.as_bytes().first().unwrap());
        writer_builder.quote(*self.quote.as_bytes().first().unwrap());
        writer_builder.escape(*self.escape.as_bytes().first().unwrap());
        match self.terminator.clone().to_lowercase().as_ref() {
            "CRLF" => writer_builder.terminator(csv::Terminator::CRLF),
            _ => writer_builder.terminator(csv::Terminator::Any(
                *self.terminator.clone().as_bytes().first().unwrap(),
            )),
        };
        match self.quote_style.clone().to_uppercase().as_ref() {
            "ALWAYS" => writer_builder.quote_style(csv::QuoteStyle::Always),
            "NEVER" => writer_builder.quote_style(csv::QuoteStyle::Never),
            "NECESSARY" => writer_builder.quote_style(csv::QuoteStyle::Necessary),
            "NOT_NUMERIC" | _ => writer_builder.quote_style(csv::QuoteStyle::NonNumeric),
        };

        writer_builder
    }
    /// Read csv data with header.
    ///
    /// # Example: Read csv document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.connector = Connector::Text(Text::new("column1,column2\nA1,A2\nB1,B2\n"));
    ///
    /// let mut data_iter = csv.read_data().unwrap().into_iter();
    /// let line_1 = data_iter.next().unwrap().to_json_value();
    /// let line_2 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_1: Value = serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#).unwrap();
    /// let expected_line_2: Value = serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#).unwrap();
    /// assert_eq!(expected_line_1, line_1);
    /// assert_eq!(expected_line_2, line_2);
    /// ```
    ///
    /// # Example: Can't read csv document because not same column number.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.connector = Connector::Text(Text::new("column1,column2\nA1\n"));
    ///
    /// let mut data_iter = csv.read_data().unwrap().into_iter();
    /// let line = data_iter.next().unwrap();
    /// match line {
    ///     DataResult::Ok(_) => assert!(false, "The line readed by the csv builder should be in error."),
    ///     DataResult::Err(_) => ()
    /// };
    /// ```
    fn read_with_header(reader: csv::Reader<Box<dyn Connect>>) -> io::Result<Data> {
        Ok(GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator");
            for record in reader.into_deserialize::<Map<String, Value>>() {
                let data_result = match record {
                    Ok(record) => {
                        trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        DataResult::Ok(Value::Object(record))
                    }
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't deserialize the record"; "error"=>format!("{:?}",e));
                        match e.kind() {
                            super::csv::csv::ErrorKind::Io(_) => {
                                return;
                            }
                            _ => (),
                        };

                        DataResult::Err((Value::Null, e.into()))
                    }
                };
                co.yield_(data_result).await;
            }
            trace!(slog_scope::logger(), "End generator");
        }))
    }
    /// Read csv data without header.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = false;
    /// csv.connector = Connector::Text(Text::new("A1,A2\nB1,B2\n"));
    ///
    /// let mut data_iter = csv.read_data().unwrap().into_iter();
    /// let line_1 = data_iter.next().unwrap().to_json_value();
    /// let line_2 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_1 = Value::Array(vec![Value::String("A1".to_string()),Value::String("A2".to_string())]);
    /// let expected_line_2 = Value::Array(vec![Value::String("B1".to_string()),Value::String("B2".to_string())]);
    /// assert_eq!(expected_line_1, line_1);
    /// assert_eq!(expected_line_2, line_2);
    /// ```
    fn read_without_header(reader: csv::Reader<Box<dyn Connect>>) -> io::Result<Data> {
        Ok(GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator");
            for record in reader.into_records() {
                let data_result = match record {
                    Ok(record) => {
                        trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
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
            trace!(slog_scope::logger(), "End generator");
        }))
    }
}

impl Build for Csv {
    /// Read complex csv data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.delimiter = "|".to_string();
    /// csv.connector = Connector::Text(Text::new(r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    /// "#));
    ///
    /// let mut data_iter = csv.read_data().unwrap().into_iter();
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
    fn read_data(&self) -> io::Result<Data> {
        trace!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let connector_reader = self.connector.clone().inner();
        let builder_reader = self.reader_builder().from_reader(connector_reader);
        let data = match self.header {
            true => Csv::read_with_header(builder_reader),
            false => Csv::read_without_header(builder_reader),
        };
        trace!(slog_scope::logger(), "Read data ended");
        data
    }
    /// Write complex csv data.
    ///
    /// # Example: Add header if connector data empty or if the connector will truncate the previous data.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// csv.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_1"
    /// "#, &format!("{}", csv.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// csv.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_1"
    /// "line_2"
    /// "#, &format!("{}", csv.connector));
    /// ```
    ///
    /// # Example: truncate and write data into the connector
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// let mut text = Text::new(r#""column_1"
    /// "line_1"
    /// "line_2""#);
    /// text.truncate = true;
    /// csv.connector = Connector::Text(text);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// csv.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#""column_1"
    /// "line_3"
    /// "#, &format!("{}", csv.connector));
    /// ```
    ///
    /// # Example: handle complex csv
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.delimiter = "|".to_string();
    /// csv.connector = Connector::Text(Text::new(r#""#));
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
    /// csv.write_data_result(data_result).unwrap();
    /// let expected_str = r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
    /// "My text"|"My text with
    ///  backspace"|"€"|10|9.5|"true"
    /// "#;
    /// assert_eq!(expected_str, &format!("{}", csv.connector));
    /// ```
    fn write_data_result(&mut self, data_result: DataResult) -> io::Result<()> {
        trace!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        let has_header = self.header;

        self.connector.get_mut().set_path_parameters(value.clone());

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
                    && (self.connector.writer().will_be_truncated()
                        || self.connector.writer().is_empty()?)
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

        self.connector
            .writer()
            .write_all(builder_writer.into_inner().unwrap().as_slice())?;

        trace!(slog_scope::logger(), "Write data ended.");
        Ok(())
    }
    /// Push data from the inner buffer into the document and flush the connector.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::csv::Csv;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut csv = Csv::default();
    /// csv.header = true;
    /// csv.connector = Connector::Text(Text::new(r#"My Text"#));
    ///
    /// csv.flush().unwrap();
    /// assert_eq!(r#""#, &format!("{}", csv.connector));
    /// ```
    fn flush(&mut self) -> io::Result<()> {
        trace!(slog_scope::logger(), "Flush called.");
        self.connector.get_mut().set_mime_type(mime::TEXT_CSV_UTF_8);
        self.connector.writer().flush()?;
        self.header_added = false;
        trace!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
    fn connector(&self) -> &Connector {
        &self.connector
    }
}
