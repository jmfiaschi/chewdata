//! Read and Write in Byte format.
//!
//! ### Configuration
//!
//! | key      | alias | Description                             | Default Value | Possible Values       |
//! | -------- | ----- | --------------------------------------- | ------------- | --------------------- |
//! | type     | -     | Required in order to use this document. | `byte`        | `byte`                |
//! | metadata | meta  | Metadata describe the resource.         | `null`        | [`crate::Metadata`]   |
//!
//! Examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "byte"
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Byte {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Byte {
    fn default() -> Self {
        Self {
            metadata: Metadata {
                mime_type: Some(mime::APPLICATION.to_string()),
                mime_subtype: Some(mime::OCTET_STREAM.to_string()),
                ..Default::default()
            },
        }
    }
}

impl Document for Byte {
    /// See [`Document::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Byte::default().metadata.merge(&self.metadata)
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::document::byte::Byte;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Byte::default();
    /// let buffer = b"My text1 \n My text 2".to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let expected: Value = b"My text1 \n My text 2".as_slice().into();
    /// assert_eq!(expected, data);
    /// ```
    #[instrument(skip(buffer), name = "byte::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let record: Value = buffer.into();
        trace!(
            record = record.display_only_for_debugging(),
            "Record deserialized"
        );
        Ok(vec![DataResult::Ok(record)])
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::document::byte::Byte;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Byte::default();
    /// let bytes: Value = b"My text".as_slice().into();
    /// let dataset = vec![DataResult::Ok(bytes)];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(b"My text".as_slice(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "byte::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for data in dataset {
            let record = data.to_value();

            let array = record.as_array().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Byte document expects a JSON array of integers",
                )
            })?;

            for value in array {
                let byte = value.as_u64().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Byte value must be an unsigned integer",
                    )
                })?;

                if byte > u8::MAX as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Byte value out of range (0–255)",
                    ));
                }

                buffer.push(byte as u8);
            }

            trace!(
                record = record.display_only_for_debugging(),
                "Record serialized"
            );
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn read() {
        let document = Byte::default();
        let buffer = b"My text1 \n My text 2".to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected: Value = b"My text1 \n My text 2".as_slice().into();
        assert_eq!(expected, data);
    }
    #[test]
    fn write() {
        let document = Byte::default();
        let bytes: Value = b"My text".as_slice().into();
        let dataset = vec![DataResult::Ok(bytes)];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(b"My text".as_slice(), buffer);
    }
}
