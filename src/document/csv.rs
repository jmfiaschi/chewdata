extern crate csv;

use crate::document::Document;
use crate::DataResult;
use crate::{DataSet, Metadata};
use csv::Trim;
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
    fn read_with_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<DataSet> {
        Ok(reader
            .into_deserialize::<Map<String, Value>>()
            .into_iter()
            .map(|record_result| match record_result {
                Ok(record) => {
                    trace!(
                        record = format!("{:?}", record).as_str(),
                        "Record deserialized"
                    );
                    DataResult::Ok(Value::Object(record))
                }
                Err(e) => {
                    warn!(
                        error = format!("{:?}", e).as_str(),
                        "Can't deserialize the record"
                    );
                    DataResult::Err((Value::Null, e.into()))
                }
            })
            .collect())
    }
    /// Read csv data without header.
    fn read_without_header(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> io::Result<DataSet> {
        Ok(reader
            .into_records()
            .into_iter()
            .map(|record_result| match record_result {
                Ok(record) => {
                    trace!(
                        record = format!("{:?}", record).as_str(),
                        "Record deserialized"
                    );
                    let map = record
                        .iter()
                        .map(|value| Value::String(value.to_string()))
                        .collect();
                    DataResult::Ok(Value::Array(map))
                }
                Err(e) => {
                    warn!(
                        error = format!("{:?}", e).as_str(),
                        "Can't deserialize the record"
                    );
                    DataResult::Err((Value::Null, e.into()))
                }
            })
            .collect())
    }
}

impl Document for Csv {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Csv::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read`] for more details.
    /// 
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Csv::default();
    /// let buffer = "column1,column2\nA1,A2\nB1,B2\n".as_bytes().to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data_1 = dataset.next().unwrap().to_value();
    /// let data_2 = dataset.next().unwrap().to_value();
    /// let expected_data_1: Value =
    ///     serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#).unwrap();
    /// let expected_data_2: Value =
    ///     serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#).unwrap();
    /// assert_eq!(expected_data_1, data_1);
    /// assert_eq!(expected_data_2, data_2);
    /// ```
    /// 
    /// ```no_run
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::Metadata;
    /// 
    /// let mut metadata = Metadata::default();
    /// metadata.has_headers = Some(false);
    /// 
    /// let mut document = Csv::default();
    /// document.metadata = metadata;
    /// let buffer = "A1,A2\nB1,B2\n".as_bytes().to_vec();
    /// 
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data_1 = dataset.next().unwrap().to_value();
    /// let data_2 = dataset.next().unwrap().to_value();
    /// let expected_data_1 = Value::Array(vec![
    ///     Value::String("A1".to_string()),
    ///     Value::String("A2".to_string()),
    /// ]);
    /// let expected_data_2 = Value::Array(vec![
    ///     Value::String("B1".to_string()),
    ///     Value::String("B2".to_string()),
    /// ]);
    /// assert_eq!(expected_data_1, data_1);
    /// assert_eq!(expected_data_2, data_2);
    /// ```
    #[instrument]
    fn read(&self, buffer: &Vec<u8>) -> io::Result<DataSet> {
        let builder_reader = self
            .reader_builder()
            .from_reader(io::Cursor::new(buffer.clone()));
        match self.metadata().has_headers {
            Some(false) => Csv::read_without_header(builder_reader),
            _ => Csv::read_with_header(builder_reader),
        }
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult; 
    /// 
    /// let mut document = Csv::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#""line_1"
    /// "#.as_bytes().to_vec(), buffer);
    ///
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#""line_2"
    /// "#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset))]
    fn write(&mut self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut builder_writer = self.writer_builder().from_writer(Vec::default());

        for data in dataset {
            let record = data.to_value();
            match record.clone() {
                Value::Bool(value) => builder_writer.serialize(value),
                Value::Number(value) => builder_writer.serialize(value),
                Value::String(value) => builder_writer.serialize(value),
                Value::Null => Ok(()),
                Value::Object(object) => {
                    let mut values = Vec::<Value>::new();

                    for (_, value) in object {
                        values.push(value);
                    }

                    builder_writer.serialize(values)
                }
                Value::Array(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Can't transform an array to csv string. {:?}", data),
                    ))
                }
            }?;
            trace!(
                record = format!("{:?}", record).as_str(),
                "Record serialized"
            );
        }

        Ok(builder_writer
            .into_inner()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .as_slice()
            .to_vec())
    }
    /// See [`Document::header`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::csv::Csv;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult; 
    /// 
    /// let document = Csv::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.header(&dataset).unwrap();
    /// assert_eq!(r#""column_1"
    /// "#.as_bytes().to_vec(), buffer);
    /// ```
    fn header(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        if dataset.is_empty() {
            return Ok(Vec::default());
        }

        let mut builder_writer = self.writer_builder().from_writer(Vec::default());
        let write_header = self.metadata().has_headers.unwrap_or(false);
        let data = dataset.iter().next().unwrap();

        let header = match data.to_value() {
            Value::Object(object) => {
                let keys = object
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect::<Vec<String>>();

                match write_header {
                    true => {
                        builder_writer.write_record(keys)?;
                        builder_writer
                            .into_inner()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                            .as_slice()
                            .to_vec()
                    }
                    false => Vec::default(),
                }
            }
            _ => Vec::default(),
        };

        if !header.is_empty() {
            trace!(
                header = format!("{:?}", &header).as_str(),
                "Header serialized"
            );
        }

        Ok(header)
    }
    /// See [`Document::terminator`] for more details.
    fn terminator(&self) -> io::Result<Vec<u8>> {
        Ok(self
            .metadata
            .terminator
            .clone()
            .unwrap_or_else(|| DEFAULT_TERMINATOR.to_string())
            .as_bytes()
            .to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_with_header() {
        let document = Csv::default();
        let buffer = "column1,column2\nA1,A2\nB1,B2\n".as_bytes().to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_1: Value =
            serde_json::from_str(r#"{"column1":"A1","column2":"A2"}"#).unwrap();
        let expected_data_2: Value =
            serde_json::from_str(r#"{"column1":"B1","column2":"B2"}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn not_read_with_header() {
        let document = Csv::default();
        let buffer = "column1,column2\nA1\n".as_bytes().to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap();
        match data {
            DataResult::Ok(_) => assert!(
                false,
                "The line read by the csv builder should be in error."
            ),
            DataResult::Err(_) => (),
        };
    }
    #[test]
    fn read_without_header() {
        let mut metadata = Metadata::default();
        metadata.has_headers = Some(false);
        let mut document = Csv::default();
        let buffer = "A1,A2\nB1,B2\n".as_bytes().to_vec();
        document.metadata = metadata;
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_1 = Value::Array(vec![
            Value::String("A1".to_string()),
            Value::String("A2".to_string()),
        ]);
        let expected_data_2 = Value::Array(vec![
            Value::String("B1".to_string()),
            Value::String("B2".to_string()),
        ]);
        assert_eq!(expected_data_1, data_1);
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn read() {
        let mut metadata = Csv::default().metadata;
        metadata.delimiter = Some("|".to_string());
        let mut document = Csv::default();
        document.metadata = metadata;
        let buffer = r#""string"|"string_backspace"|"special_char"|"int"|"float"|"bool"
"My text"|"My text with
 backspace"|"€"|10|9.5|"true"
        "#
        .as_bytes()
        .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_json::from_str(
            r#"{
            "string":"My text",
            "string_backspace":"My text with\n backspace",
            "special_char":"€",
            "int":10,
            "float":9.5,
            "bool":true
        }"#,
        )
        .unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write() {
        let mut document = Csv::default();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#""line_1"
"#
            .as_bytes()
            .to_vec(),
            buffer
        );

        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#""line_2"
"#
            .as_bytes()
            .to_vec(),
            buffer
        );
    }
    #[test]
    fn header() {
        let document = Csv::default();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.header(&dataset).unwrap();
        assert_eq!(
            r#""column_1"
"#
            .as_bytes()
            .to_vec(),
            buffer
        );
    }
}
