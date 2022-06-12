use crate::document::Document;
use crate::Metadata;
use crate::{DataResult, DataSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_SUBTYPE: &str = "toml";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Toml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Toml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Toml { metadata }
    }
}

impl Document for Toml {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Toml::default().metadata.merge(self.metadata.clone())
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
    #[instrument]
    fn read(&self, buffer: &Vec<u8>) -> io::Result<DataSet> {
        let record: Value = toml::from_slice(buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(match record {
            Value::Array(records) => records
                .into_iter()
                .map(|record| {
                    trace!(
                        record = format!("{:?}", record).as_str(),
                        "Record deserialized"
                    );
                    DataResult::Ok(record)
                })
                .collect(),
            record => {
                trace!(
                    record = format!("{:?}", record).as_str(),
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
    #[instrument(skip(dataset))]
    fn write(&mut self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();

        for data in dataset {
            let record = data.to_value();
            // Transform serde_json::Value to toml::Value
            let toml_value = toml::value::Value::try_from(record.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            let mut toml = String::new();
            toml_value
                .serialize(&mut toml::Serializer::new(&mut toml))
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Can't write the data into the connector. {}", e),
                    )
                })?;

            trace!(
                record = format!("{:?}", record).as_str(),
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
        let mut document = Toml::default();
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
