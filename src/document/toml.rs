//! Read and Write in Toml format.
//!
//! ### Configuration
//!
//! | key      | alias | Description                             | Default Value | Possible Values       |
//! | -------- | ----- | --------------------------------------- | ------------- | --------------------- |
//! | type     | -     | Required in order to use this document. | `toml`        | `toml`                |
//! | metadata | meta  | Metadata describe the resource.         | `null`        | [`crate::Metadata`]   |
//!
//! examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "toml"
//!         }
//!     }
//! ]
//! ```
//!
//! input/output:
//!
//! ```toml
//! [[line]]
//! field= value
//! ...
//! ```
use crate::document::Document;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::Metadata;
use crate::{DataResult, DataSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_SUBTYPE: &str = "toml";
const DEFAULT_TERMINATOR: &str = "---";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Toml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Toml {
    fn default() -> Self {
        let metadata = Metadata {
            terminator: Some(DEFAULT_TERMINATOR.to_string()),
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Toml { metadata }
    }
}

impl Document for Toml {
    /// See [`Document::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata.clone();
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Toml::default().metadata.merge(&self.metadata)
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Toml::default();
    /// let buffer = r#"[Title]
    /// key_1 = "value_1"
    /// key_2 = "value_2"
    /// "#
    /// .as_bytes()
    /// .to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let expected_data: Value = serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#).unwrap();
    /// assert_eq!(expected_data, data);
    /// ```
    #[instrument(skip(buffer), name = "toml::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let str_buffer = std::str::from_utf8(buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let record: Value = toml::from_str(str_buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(match record {
            Value::Array(records) => records
                .into_iter()
                .map(|record| {
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record deserialized"
                    );
                    DataResult::Ok(record)
                })
                .collect(),
            record => {
                trace!(
                    record = record.display_only_for_debugging(),
                    "Record deserialized"
                );
                vec![DataResult::Ok(record)]
            }
        })
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Toml::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"column_1 = "line_1"
    /// "#.as_bytes().to_vec(), buffer);
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"column_1 = "line_2"
    /// "#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "toml::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();

        for data in dataset {
            let record = data.to_value();

            let toml = toml::to_string(&record)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            trace!(
                record = record.display_only_for_debugging(),
                "Record serialized"
            );

            buffer.append(&mut toml.as_bytes().to_vec());
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read() {
        let document = Toml::default();
        let buffer = r#"[Title]
key_1 = "value_1"
key_2 = "value_2"
"#
        .as_bytes()
        .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value =
            serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write() {
        let document = Toml::default();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"column_1 = "line_1"
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
            r#"column_1 = "line_2"
"#
            .as_bytes()
            .to_vec(),
            buffer
        );
    }
}
