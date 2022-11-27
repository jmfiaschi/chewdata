use crate::document::Document;
use crate::Metadata;
use crate::{DataResult, DataSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, io};

const DEFAULT_SUBTYPE: &str = "x-yaml";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Yaml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl fmt::Display for Yaml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Yaml {{ ... }}")
    }
}

impl Default for Yaml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Yaml { metadata }
    }
}

impl Document for Yaml {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Yaml::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::yaml::Yaml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let document = Yaml::default();
    /// let buffer = r#"
    /// ---
    /// number: 10
    /// string: value to test
    /// long-string: "Long val\nto test"
    /// boolean: true
    /// special_char: é
    /// date: 2019-12-31
    /// "#
    /// .as_bytes()
    /// .to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let expected_data: Value = serde_yaml::from_slice(&buffer).unwrap();
    /// assert_eq!(expected_data, data);
    /// ```
    #[instrument]
    fn read(&self, buffer: &Vec<u8>) -> io::Result<DataSet> {
        let mut dataset = Vec::default();

        for document in serde_yaml::Deserializer::from_slice(buffer) {
            let record = Value::deserialize(document)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            trace!(
                record = format!("{:?}", &record).as_str(),
                "Record deserialized"
            );

            dataset.push(DataResult::Ok(record));
        }

        Ok(dataset)
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::yaml::Yaml;
    /// use chewdata::document::Document;
    /// use chewdata::DataResult;
    /// use serde_json::Value;
    ///
    /// let mut document = Yaml::default();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"---
    /// column_1: line_1
    /// "#
    ///     .as_bytes()
    ///     .to_vec(),
    ///     buffer
    /// );
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"---
    /// column_1: line_2
    /// "#
    ///     .as_bytes()
    ///     .to_vec(),
    ///     buffer
    /// );
    /// ```
    #[instrument(skip(dataset))]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();

        for data in dataset {
            buffer.append(&mut "---\n".as_bytes().to_vec());

            let record = data.to_value();
            let mut buf = Vec::default();
            serde_yaml::to_writer(&mut buf, &record.clone()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Can't write the data into the connector. {}", e),
                )
            })?;

            trace!(
                record = format!("{:?}", &record).as_str(),
                "Record serialized"
            );

            buffer.append(&mut buf);
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read() {
        let document = Yaml::default();
        let buffer = r#"
---
number: 10
string: value to test
long-string: "Long val\nto test"
boolean: true
special_char: é
date: 2019-12-31
"#
        .as_bytes()
        .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_yaml::from_slice(&buffer).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write() {
        let document = Yaml::default();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"---
column_1: line_1
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
            r#"---
column_1: line_2
"#
            .as_bytes()
            .to_vec(),
            buffer
        );
    }
}
