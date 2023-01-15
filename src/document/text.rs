use crate::document::Document;
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
            mime_type: Some(mime::TEXT.to_string()),
            mime_subtype: Some(mime::PLAIN.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Text { metadata }
    }
}

impl Document for Text {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Text::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    #[instrument]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let record = Value::String(
            String::from_utf8(buffer.to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        );
        trace!(
            record = format!("{:?}", record).as_str(),
            "Record deserialized"
        );
        Ok(vec![DataResult::Ok(record)])
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    #[instrument(skip(dataset))]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();
        for data in dataset {
            let record = data.to_value();
            buffer.append(&mut record.clone().as_str().unwrap_or("").as_bytes().to_vec());
            trace!(
                record = format!("{:?}", record).as_str(),
                "Record serialized"
            );
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
    fn write() {
        let document = Text::default();
        let dataset = vec![DataResult::Ok(Value::String("My text".to_string()))];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"My text"#.as_bytes().to_vec(), buffer);
    }
}
