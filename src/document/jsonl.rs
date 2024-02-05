//! Read and Write in JSON Lines format.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                       | Default Value | Possible Values                                                                |
//! | ---------- | ----- | ----------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------ |
//! | type       | -     | Required in order to use this document.                           | `jsonl`       | `jsonl`                                                                        |
//! | metadata   | meta  | Metadata describe the resource.                                   | `null`        | [`crate::Metadata`]                                                            |
//! | is_pretty  | -     | Display data in readable format for human.                        | `false`       | `false` / `true`                                                               |
//! | entry_path | -     | Use this field if you want target a specific field in the object. | `null`        | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |
//!
//! Examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "jsonl",
//!             "entry_path": "/field1"
//!         }
//!     },
//!     {
//!         "type": "write",
//!         "document": {
//!             "type": "jsonl",
//!             "is_pretty": true
//!         }
//!     }
//! ]
//! ```
//!
//! input:
//!
//! ```jsonl
//! [
//!     { "field1":"value1", ... },
//!     { "field1":"value2", ... },
//!     ...
//! ]
//! ```
//!
//! output:
//!
//! ```jsonl
//! value1
//! value2
//! ...
//! ```
use crate::document::Document;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::DataResult;
use crate::{DataSet, Metadata};
use json_value_merge::Merge;
use json_value_search::Search;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Write};

const DEFAULT_MIME_TYPE: &str = "x-ndjson";
const DEFAULT_TERMINATOR: &str = "\n";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Jsonl {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub entry_path: Option<String>,
}

impl Default for Jsonl {
    fn default() -> Self {
        let metadata = Metadata {
            terminator: Some(DEFAULT_TERMINATOR.to_string()),
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_MIME_TYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Jsonl {
            metadata,
            is_pretty: false,
            entry_path: None,
        }
    }
}

impl Document for Jsonl {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Jsonl::default().metadata.merge(&self.metadata)
    }
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = Some(entry_path);
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Jsonl::default();
    /// let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#.as_bytes().to_vec();
    ///
    /// let mut dataset = document.read(&json_str).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let expected_data: Value = serde_json::from_slice(&json_str).unwrap();
    /// assert_eq!(expected_data, data);
    /// ```
    #[instrument(skip(buffer), name = "jsonl::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let deserializer = serde_json::Deserializer::from_reader(io::Cursor::new(buffer));
        let iterator = deserializer.into_iter::<Value>();
        let mut dataset = Vec::default();

        for record_result in iterator {
            match (&record_result, &self.entry_path) {
                (Ok(record), Some(entry_path)) => {
                    match record.clone().search(entry_path)? {
                        Some(Value::Array(records)) => {
                            for record in records {
                                trace!(
                                    record = record.display_only_for_debugging(),
                                    "Record deserialized"
                                );
                                dataset.push(DataResult::Ok(record));
                            }
                        }
                        Some(record) => {
                            trace!(
                                record = record.display_only_for_debugging(),
                                "Record deserialized"
                            );
                            dataset.push(DataResult::Ok(record));
                        }
                        None => {
                            warn!(
                                entry_path = format!("{:?}", entry_path).as_str(),
                                record = record.display_only_for_debugging(),
                                "Entry path not found"
                            );
                            dataset.push(DataResult::Err((
                                record.clone(),
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("Entry path '{}' not found", entry_path),
                                ),
                            )));
                        }
                    };
                }
                (Ok(Value::Array(records)), None) => {
                    for record in records {
                        trace!(
                            record = record.display_only_for_debugging(),
                            "Record deserialized"
                        );
                        dataset.push(DataResult::Ok(record.clone()));
                    }
                }
                (Ok(record), None) => {
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record deserialized"
                    );
                    dataset.push(DataResult::Ok(record.clone()));
                }
                (Err(e), _) => {
                    warn!(
                        error = format!("{:?}", e).as_str(),
                        "Can't deserialize the record"
                    );
                    dataset.push(DataResult::Err((
                        Value::Null,
                        io::Error::new(io::ErrorKind::InvalidInput, e.to_string()),
                    )));
                }
            };
        }

        Ok(dataset)
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Jsonl::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "jsonl::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        let serialize_value_into_buffer =
            |has_terminator: bool, buf: &mut Vec<u8>, value: &Value| -> io::Result<()> {
                if has_terminator && self.entry_path.is_none() {
                    buf.write_all(DEFAULT_TERMINATOR.as_bytes())?;
                }

                if self.entry_path.is_some() && !buf.is_empty() {
                    buf.write_all(&[b','])?;
                }

                match self.is_pretty {
                    true => serde_json::to_writer_pretty(buf, &value)?,
                    false => serde_json::to_writer(buf, &value)?,
                }

                trace!(
                    record = value.display_only_for_debugging(),
                    "Record serialized"
                );

                Ok(())
            };

        for (pos, data) in dataset.iter().enumerate() {
            let record = data.to_value();
            match record {
                Value::Array(array) => {
                    array
                        .iter()
                        .enumerate()
                        .try_for_each(|(array_pos, array_value)| {
                            serialize_value_into_buffer(pos + array_pos != 0, &mut buf, array_value)
                        })?;
                }
                _ => {
                    serialize_value_into_buffer(pos != 0, &mut buf, &record)?;
                }
            };
        }

        Ok(buf)
    }
    /// See [`Document::has_data`] for more details.
    fn has_data(&self, buf: &[u8]) -> io::Result<bool> {
        if buf == br#"{}"#.to_vec() {
            return Ok(false);
        }
        if buf == br#"[]"#.to_vec() {
            return Ok(false);
        }
        Ok(!buf.is_empty())
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
    /// See [`Document::header`] for more details.
    fn header(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        if let Some(entry_path) = &self.entry_path {
            let mut entry_path_value = Value::default();
            entry_path_value.merge_in(entry_path, &Value::default())?;

            let re = Regex::new(r#"([}\]]|null\]|null\})"#)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            let entry_path_header = re
                .replace_all(entry_path_value.to_string().as_str(), "")
                .to_string();

            return Ok(entry_path_header.into_bytes());
        }

        Ok(Default::default())
    }
    /// See [`Document::footer`] for more details.
    fn footer(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        if let Some(entry_path) = &self.entry_path {
            let mut entry_path_value = Value::default();
            entry_path_value.merge_in(entry_path, &Value::default())?;

            let re = Regex::new(r#"[^}\]]"#)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            let entry_path_footer = re
                .replace_all(entry_path_value.to_string().as_str(), "")
                .to_string();

            return Ok(entry_path_footer.into_bytes());
        }

        Ok(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_data() {
        let document = Jsonl::default();
        let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#.as_bytes().to_vec();
        let mut dataset = document.read(&json_str).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_json::from_slice(&json_str).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_empty_data() {
        let document = Jsonl::default();
        let buffer = Vec::default();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        match dataset.next() {
            Some(_) => assert!(
                false,
                "The data read by the json builder should be in error."
            ),
            None => (),
        };
    }
    #[test]
    fn read_data_in_target_position() {
        let mut document = Jsonl::default();
        document.entry_path = Some("/array*/*".to_string());
        let buffer = r#"{"array1":[{"field":"value1"},{"field":"value2"}]}
{"array1":[{"field":"value3"},{"field":"value4"}]}"#
            .as_bytes()
            .to_vec();
        let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#).unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_data_without_finding_entry_path() {
        let mut document = Jsonl::default();
        document.entry_path = Some("/not_found/*".to_string());
        let buffer = r#"{"array1":[{"field":"value1"},{"field":"value2"}]}"#
            .as_bytes()
            .to_vec();
        let expected_data: Value = serde_json::from_str(r#"{"array1":[{"field":"value1"},{"field":"value2"}],"_error":"Entry path '/not_found/*' not found"}"#).unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write_object() {
        let document = Jsonl::default();
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"{"column_1":"line_1"}
{"column_1":"line_2"}"#
                .as_bytes()
                .to_vec(),
            buffer
        );
    }
    #[test]
    fn write_array() {
        let document = Jsonl::default();
        let dataset = vec![
            DataResult::Ok(
                serde_json::from_str(r#"[{"column_1":"line_1"},{"column_1":"line_2"}]"#).unwrap(),
            ),
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap()),
            DataResult::Ok(
                serde_json::from_str(r#"[{"column_1":"line_4"},{"column_1":"line_5"}]"#).unwrap(),
            ),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"{"column_1":"line_1"}
{"column_1":"line_2"}
{"column_1":"line_3"}
{"column_1":"line_4"}
{"column_1":"line_5"}"#
                .as_bytes()
                .to_vec(),
            buffer
        );
    }
    #[test]
    fn write_array_string() {
        let document = Jsonl::default();
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"["a","b"]"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#""c""#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#""a"
"b"
"c""#
                .as_bytes()
                .to_vec(),
            buffer
        );
    }
    #[test]
    fn write_array_string_with_entry_path() {
        let mut document = Jsonl::default();
        document.entry_path = Some("/field/*".to_owned());
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"["a","b"]"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#""c""#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#""a","b","c""#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_header_with_entry_path() {
        let mut document = Jsonl::default();
        document.entry_path = Some("/field/*".to_owned());
        let buffer = document.header(&Vec::default()).unwrap();
        assert_eq!(r#"{"field":["#.as_bytes().to_vec(), buffer);

        let mut document = Jsonl::default();
        document.entry_path = Some("/field".to_owned());
        let buffer = document.header(&Vec::default()).unwrap();
        assert_eq!(r#"{"field":"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_footer_with_entry_path() {
        let mut document = Jsonl::default();
        document.entry_path = Some("/field/*".to_owned());
        let buffer = document.footer(&Vec::default()).unwrap();
        assert_eq!(r#"]}"#.as_bytes().to_vec(), buffer);

        let mut document = Jsonl::default();
        document.entry_path = Some("/field".to_owned());
        let buffer = document.footer(&Vec::default()).unwrap();
        assert_eq!(r#"}"#.as_bytes().to_vec(), buffer);
    }
}
