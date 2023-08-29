//! Read and Write in JSON format.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                            | Default Value | Possible Values                                                                |
//! | ---------- | ----- | ---------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------ |
//! | type       | -     | Required in order to use this document.                                | `json`        | `json`                                                                         |
//! | metadata   | meta  | Metadata describe the resource.                                        | `null`        | [`crate::Metadata`]                                                            |
//! | is_pretty  | -     | Display json data readable for human.                                  | `false`       | `false` / `true`                                                               |
//! | entry_path | -     | Use this field if you want target a specific field in the json object. | `null`        | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |
//!
//! Examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "json",
//!             "entry_path": "/0"
//!         }
//!     },
//!     {
//!         "type": "write",
//!         "document": {
//!             "type": "json",
//!             "is_pretty": true
//!         }
//!     }
//! ]
//! ```
//!
//! input:
//!
//! ```json
//! [
//!     {"field1":"value1"},
//!     {"field1":"value2"},
//!     ...
//! ]
//! ```
//!
//! output:
//!
//! ```json
//! [
//!     {
//!         "field1":"value1"
//!     }
//! ]
//! ```
use crate::document::Document;
use crate::DataResult;
use crate::DataSet;
use crate::Metadata;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_TERMINATOR: &str = ",";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Json {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub entry_path: Option<String>,
}

impl Default for Json {
    fn default() -> Self {
        let metadata = Metadata {
            terminator: Some(DEFAULT_TERMINATOR.to_string()),
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(mime::JSON.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Json {
            metadata,
            is_pretty: false,
            entry_path: None,
        }
    }
}

impl Document for Json {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Json::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = Some(entry_path);
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
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Json::default();
    /// let json_str = r#"[{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}]"#.as_bytes().to_vec();
    /// let buffer = json_str.clone();
    ///
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let expected_data: Value = serde_json::from_slice(&json_str).unwrap();
    /// assert_eq!(expected_data, data);
    /// ```
    #[instrument(skip(buffer), name = "json::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let deserializer = serde_json::Deserializer::from_reader(io::Cursor::new(buffer));
        let iterator = deserializer.into_iter::<Value>();
        let entry_path_option = self.entry_path.clone();
        let mut dataset = Vec::default();

        for record_result in iterator {
            match (record_result, entry_path_option.clone()) {
                (Ok(record), Some(entry_path)) => {
                    match record.clone().search(entry_path.as_ref())? {
                        Some(Value::Array(records)) => {
                            for record in records {
                                trace!(
                                    record = format!("{:?}", record).as_str(),
                                    "Record deserialized"
                                );
                                dataset.push(DataResult::Ok(record));
                            }
                        }
                        Some(record) => {
                            trace!(
                                record = format!("{:?}", record).as_str(),
                                "Record deserialized"
                            );
                            dataset.push(DataResult::Ok(record));
                        }
                        None => {
                            warn!(
                                entry_path = format!("{:?}", entry_path).as_str(),
                                record = format!("{:?}", record.clone()).as_str(),
                                "Entry path not found in the record"
                            );
                            dataset.push(DataResult::Err((
                                record,
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("Entry path '{}' not found.", entry_path),
                                ),
                            )));
                        }
                    }
                }
                (Ok(Value::Array(records)), None) => {
                    for record in records {
                        trace!(
                            record = format!("{:?}", record).as_str(),
                            "Record deserialized"
                        );
                        dataset.push(DataResult::Ok(record));
                    }
                }
                (Ok(record), None) => {
                    trace!(
                        record = format!("{:?}", record).as_str(),
                        "Record deserialized"
                    );
                    dataset.push(DataResult::Ok(record));
                }
                (Err(e), _) => {
                    warn!(
                        error = format!("{:?}", e).as_str(),
                        "Can't deserialize the record"
                    );
                    dataset.push(DataResult::Err((Value::Null, e.into())));
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
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Json::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "json::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        let serialize_value_into_buffer =
            |has_terminator: bool, buf: &mut Vec<u8>, value: &Value| -> io::Result<()> {
                let mut new_buf = buf;

                if has_terminator {
                    new_buf.append(&mut DEFAULT_TERMINATOR.as_bytes().to_vec());
                }

                match self.is_pretty {
                    true => serde_json::to_writer_pretty(&mut new_buf, &value)?,
                    false => serde_json::to_writer(&mut new_buf, &value)?,
                };

                trace!(
                    record = format!("{:?}", value).as_str(),
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
                        .map(|(array_pos, array_value)| {
                            serialize_value_into_buffer(
                                pos + array_pos != 0,
                                &mut buf,
                                &array_value,
                            )
                        })
                        .collect::<io::Result<()>>()?;
                }
                _ => {
                    serialize_value_into_buffer(pos != 0, &mut buf, &record)?;
                }
            };
        }

        Ok(buf)
    }
    /// See [`Document::header`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    ///
    /// let document = Json::default();
    /// let buffer = document.header(&Vec::default()).unwrap();
    /// assert_eq!(r#"["#.as_bytes().to_vec(), buffer);
    /// ```
    fn header(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        Ok("[".as_bytes().to_vec())
    }
    /// See [`Document::footer`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    ///
    /// let document = Json::default();
    /// let buffer = document.footer(&Vec::default()).unwrap();
    /// assert_eq!(r#"]"#.as_bytes().to_vec(), buffer);
    /// ```
    fn footer(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        Ok("]".as_bytes().to_vec())
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
    fn read_data_array() {
        let document = Json::default();
        let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#.as_bytes().to_vec();
        let buffer = json_str.clone();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_json::from_slice(&json_str).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_data_object() {
        let document = Json::default();
        let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#.as_bytes().to_vec();
        let buffer = json_str.clone();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_json::from_slice(&json_str).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_empty_data() {
        let document = Json::default();
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
    fn read_empty_body() {
        let document = Json::default();
        let buffer = r#"[]"#.as_bytes().to_vec();
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
        let mut document = Json::default();
        document.entry_path = Some("/*/array*/*".to_string());
        let buffer = r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#
            .as_bytes()
            .to_vec();
        let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#).unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_data_without_finding_entry_path() {
        let mut document = Json::default();
        document.entry_path = Some("/*/not_found/*".to_string());
        let buffer = r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#
            .as_bytes()
            .to_vec();
        let expected_data: Value = serde_json::from_str(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"_error":"Entry path '/*/not_found/*' not found."}]"#).unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write_object() {
        let document = Json::default();
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"{"column_1":"line_1"},{"column_1":"line_2"}"#.as_bytes().to_vec(),
            buffer
        );
    }
    #[test]
    fn write_array() {
        let document = Json::default();
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
            r#"{"column_1":"line_1"},{"column_1":"line_2"},{"column_1":"line_3"},{"column_1":"line_4"},{"column_1":"line_5"}"#
                .as_bytes()
                .to_vec(),
            buffer
        );
    }
    #[test]
    fn write_array_string() {
        let document = Json::default();
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"["a","b"]"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#""c""#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#""a","b","c""#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn header() {
        let document = Json::default();
        let buffer = document.header(&Vec::default()).unwrap();
        assert_eq!(r#"["#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn footer() {
        let document = Json::default();
        let buffer = document.footer(&Vec::default()).unwrap();
        assert_eq!(r#"]"#.as_bytes().to_vec(), buffer);
    }
}
