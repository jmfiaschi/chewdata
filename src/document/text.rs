//! Read and Write in Text format.
//!
//! ### Configuration
//!
//! | key      | alias | Description                             | Default Value | Possible Values       |
//! | -------- | ----- | --------------------------------------- | ------------- | --------------------- |
//! | type     | -     | Required in order to use this document. | `text`        | `text`                |
//! | metadata | meta  | Metadata describe the resource.         | `null`        | [`crate::Metadata`]   |
//!
//! Examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "text"
//!         },
//!         "connector": {
//!             "type": "mem",
//!             "data": "Hello world !!!"
//!         }
//!     }
//! ]
//! ```
//!
//! output:
//!
//! ```text
//! Hello world !!!
//! ```
use crate::document::Document;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::Metadata;
use crate::{DataResult, DataSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_TERMINATOR: &str = "\n";

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Text {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Text {
    fn default() -> Self {
        let metadata = Metadata {
            terminator: Some(DEFAULT_TERMINATOR.to_string()),
            mime_type: Some(mime::PLAIN.to_string()),
            mime_subtype: Some(mime::TEXT.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Text { metadata }
    }
}

impl Document for Text {
    /// See [`Document::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata.clone();
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Text::default().metadata.merge(&self.metadata)
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Text::default();
    /// let buffer = r#"My text1 \n My text 2"#.as_bytes().to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// assert_eq!(r#"My text1 \n My text 2"#, data);
    /// ```
    #[instrument(skip(buffer), name = "text::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let record = Value::String(String::from_utf8_lossy(buffer).to_string());
        trace!(record = record.display_only_for_debugging(), "Record read");
        Ok(vec![DataResult::Ok(record)])
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Text::default();
    /// let dataset = vec![DataResult::Ok(Value::String("My text".to_string()))];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"My text"#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "text::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();
        for data in dataset {
            let record = &data.to_value();

            match record {
                Value::String(ref s) => {
                    buffer.extend_from_slice(s.as_bytes());
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record written"
                    );
                }
                Value::Array(_) => {
                    warn!(
                        record = record.display_only_for_debugging(),
                        "record is an array, skipped"
                    );
                }
                Value::Object(_) => {
                    warn!(
                        record = record.display_only_for_debugging(),
                        "record is an object, skipped"
                    );
                }
                Value::Number(ref n) => {
                    buffer.extend_from_slice(n.to_string().as_bytes());
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record written"
                    );
                }
                Value::Bool(ref b) => {
                    buffer.extend_from_slice(b.to_string().as_bytes());
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record written"
                    );
                }
                Value::Null => {
                    trace!("record is null, skipped");
                }
            }
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read() {
        let document = Text::default();
        let buffer = r#"My text1 \n My text 2"#.as_bytes().to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        assert_eq!(r#"My text1 \n My text 2"#, data);
    }
    #[test]
    fn write_string() {
        let document = Text::default();
        let dataset = vec![DataResult::Ok(Value::String("My text".to_string()))];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"My text"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_number() {
        let document = Text::default();
        let dataset = vec![DataResult::Ok(Value::Number(42.into()))];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"42"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_bool() {
        let document = Text::default();
        let dataset = vec![DataResult::Ok(Value::Bool(true))];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"true"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_null() {
        let document = Text::default();
        let dataset = vec![DataResult::Ok(Value::Null)];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(Vec::<u8>::new(), buffer);
    }
}
