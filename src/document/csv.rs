//! Read and Write in CSV format.
//!
//! ### Configuration
//!
//! | key         | alias | Description                                                                                                      | Default Value | Possible Values                                  |
//! | ----------- | ----- | ---------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------ |
//! | type        | -     | Required in order to use this document.                                                                          | `csv`         | `csv`                                            |
//! | metadata    | meta  | Metadata describe the resource.                                                                                  | `null`        | [`crate::Metadata`]                              |
//! | is_flexible | -     | If flexible is true, the application try to match the number of header's fields and the number of line's fields. | `true`        | `true` / `false`                                 |
//! | quote_style | -     | The quoting style to use when writing CSV.                                                                       | `NOT_NUMERIC` | `NOT_NUMERIC` / `ALWAYS` / `NEVER` / `NECESSARY` |
//! | trim        | -     | Define where you trim the data. The application can trimmed fields, headers or both.                             | `ALL`         | `ALL` / `FIELDS` / `HEADERS`                     |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "write",
//!         "document": {
//!             "type": "csv",
//!             "is_flexible": true,
//!             "quote_style": "NOT_NUMERIC",
//!             "trim": "ALL",
//!             "metadata": {
//!                 "has_headers": true,
//!                 "delimiter": ",",
//!                 "quote": "\"",
//!                 "escape": "\\",
//!                 "comment": "#",
//!                 "terminator": "\n"
//!             }
//!         }
//!     }
//! ]
//! ```
//!
//! input:
//!
//! ```json
//! [
//!     {"column1 ": "value1 ", " column2": " value2", ...},
//!     ...
//! ]
//! ```
//!
//! output:
//!
//! ```csv
//! "column1","column2",...
//! "value1","value2",...
//! ...
//! ```
extern crate csv;

use crate::document::Document;
use crate::DataResult;
use crate::{DataSet, Metadata};
use csv::Trim;
use json_value_resolve::Resolve;
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
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
        metadata.quote.as_ref().map(|value| match value.as_str() {
            "\"" => builder.double_quote(true),
            _ => builder.double_quote(false),
        });
        metadata.quote.as_ref().map(|value| match value.as_str() {
            "'" | "\"" => builder.quoting(true),
            _ => builder.quoting(false),
        });
        metadata
            .quote
            .as_ref()
            .map(|value| builder.quote(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .delimiter
            .as_ref()
            .map(|value| builder.delimiter(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .escape
            .as_ref()
            .map(|value| builder.escape(Some(*value.as_bytes().to_vec().first().unwrap())));
        metadata
            .comment
            .as_ref()
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
        metadata.quote.as_ref().map(|value| match value.as_str() {
            "\"" => builder.double_quote(true),
            _ => builder.double_quote(false),
        });
        metadata
            .quote
            .as_ref()
            .map(|value| builder.quote(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .delimiter
            .as_ref()
            .map(|value| builder.delimiter(*value.as_bytes().to_vec().first().unwrap()));
        metadata
            .escape
            .as_ref()
            .map(|value| builder.escape(*value.as_bytes().to_vec().first().unwrap()));
        metadata.terminator.map(|value| match value.as_str() {
            "CRLF" | "CR" | "LF" | "\n\r" => builder.terminator(csv::Terminator::CRLF),
            _ => builder.terminator(csv::Terminator::Any(
                *value.as_bytes().to_vec().first().unwrap(),
            )),
        });
        match self.quote_style.to_uppercase().as_ref() {
            "ALWAYS" => builder.quote_style(csv::QuoteStyle::Always),
            "NEVER" => builder.quote_style(csv::QuoteStyle::Never),
            "NECESSARY" => builder.quote_style(csv::QuoteStyle::Necessary),
            _ => builder.quote_style(csv::QuoteStyle::NonNumeric),
        };

        builder
    }
    /// Read csv data with header.
    fn read_with_header(reader: csv::Reader<io::Cursor<&[u8]>>) -> io::Result<DataSet> {
        Ok(reader
            .into_deserialize::<Map<String, Value>>()
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
    fn read_without_header(reader: csv::Reader<io::Cursor<&[u8]>>) -> io::Result<DataSet> {
        Ok(reader
            .into_records()
            .map(|record_result| match record_result {
                Ok(record) => {
                    trace!(
                        record = format!("{:?}", record).as_str(),
                        "Record deserialized"
                    );
                    let map = record
                        .iter()
                        .map(|value| Value::resolve(value.to_string()))
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
        Csv::default().metadata.merge(&self.metadata)
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
    #[instrument(skip(buffer), name = "csv::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let builder_reader = self.reader_builder().from_reader(io::Cursor::new(buffer));
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
    #[instrument(skip(dataset), name = "csv::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut builder_writer = self.writer_builder().from_writer(Vec::default());

        for data in dataset {
            let record = data.to_value();
            match &record {
                Value::Object(object) => {
                    let mut values = Vec::<Value>::new();

                    for (_, value) in flatten(object) {
                        values.push(value);
                    }

                    builder_writer.serialize(values)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Can transform only object to csv string. {:?}", record),
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
                let keys = flatten(&object)
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

fn flatten(json: &Map<String, Value>) -> Map<String, Value> {
    let mut obj = Map::new();
    insert_object(&mut obj, None, json);
    obj
}

fn insert_object(
    base_json: &mut Map<String, Value>,
    base_key: Option<&str>,
    object: &Map<String, Value>,
) {
    for (key, value) in object {
        let new_key = base_key.map_or_else(|| key.clone(), |base_key| format!("{base_key}.{key}"));

        if let Some(array) = value.as_array() {
            insert_array(base_json, Some(&new_key), array);
        } else if let Some(object) = value.as_object() {
            insert_object(base_json, Some(&new_key), object);
        } else {
            insert_value(base_json, &new_key, value.clone());
        }
    }
}

fn insert_array(base_json: &mut Map<String, Value>, base_key: Option<&str>, array: &[Value]) {
    for (key, value) in array.iter().enumerate() {
        let new_key = base_key.map_or_else(
            || key.clone().to_string(),
            |base_key| format!("{base_key}.{key}"),
        );
        if let Some(object) = value.as_object() {
            insert_object(base_json, Some(&new_key), object);
        } else if let Some(sub_array) = value.as_array() {
            insert_array(base_json, Some(&new_key), sub_array);
        } else {
            insert_value(base_json, &new_key, value.clone());
        }
    }
}

fn insert_value(base_json: &mut Map<String, Value>, key: &str, to_insert: Value) {
    debug_assert!(!to_insert.is_object());
    debug_assert!(!to_insert.is_array());

    // does the field aleardy exists?
    if let Some(value) = base_json.get_mut(key) {
        // is it already an array
        if let Some(array) = value.as_array_mut() {
            array.push(to_insert);
        // or is there a collision
        } else {
            let value = std::mem::take(value);
            base_json[key] = serde_json::json!([value, to_insert]);
        }
        // if it does not exist we can push the value untouched
    } else {
        base_json.insert(key.to_string(), serde_json::json!(to_insert));
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
        let document = Csv::default();
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
    fn write_object() {
        let document = Csv::default();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(
                r#"{"column_1":{"field_1":"value_1","field_2":["value_2","value_3"]}}"#,
            )
            .unwrap(),
        )];
        let buffer = document.header(&dataset).unwrap();
        assert_eq!(
            r#""column_1.field_1","column_1.field_2.0","column_1.field_2.1"
"#
            .as_bytes()
            .to_vec(),
            buffer
        );

        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#""value_1","value_2","value_3"
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
