#[cfg(feature = "csv")]
pub mod csv;
pub mod json;
pub mod jsonl;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod text;
#[cfg(feature = "toml")]
pub mod toml;
#[cfg(feature = "xml")]
pub mod xml;
pub mod yaml;

#[cfg(feature = "csv")]
use self::csv::Csv;
use self::json::Json;
use self::jsonl::Jsonl;
#[cfg(feature = "parquet")]
use self::parquet::Parquet;
use self::text::Text;
#[cfg(feature = "toml")]
use self::toml::Toml;
#[cfg(feature = "xml")]
use self::xml::Xml;
use self::yaml::Yaml;
use super::Metadata;
use crate::DataSet;
use serde::{Deserialize, Serialize};
use std::io::{self, Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum DocumentType {
    #[cfg(feature = "csv")]
    #[serde(rename = "csv")]
    Csv(Csv),
    #[serde(rename = "json")]
    Json(Json),
    #[serde(rename = "jsonl")]
    Jsonl(Jsonl),
    #[cfg(feature = "xml")]
    #[serde(rename = "xml")]
    Xml(Xml),
    #[serde(rename = "yaml")]
    #[serde(alias = "yml")]
    Yaml(Yaml),
    #[cfg(feature = "toml")]
    #[serde(rename = "toml")]
    Toml(Toml),
    #[serde(rename = "text")]
    #[serde(alias = "txt")]
    Text(Text),
    #[cfg(feature = "parquet")]
    #[serde(rename = "parquet")]
    Parquet(Parquet),
}

impl Default for DocumentType {
    fn default() -> Self {
        DocumentType::Json(Json::default())
    }
}

impl DocumentType {
    pub fn boxed_inner(self) -> Box<dyn Document> {
        match self {
            #[cfg(feature = "csv")]
            DocumentType::Csv(document) => Box::new(document),
            DocumentType::Json(document) => Box::new(document),
            DocumentType::Jsonl(document) => Box::new(document),
            #[cfg(feature = "xml")]
            DocumentType::Xml(document) => Box::new(document),
            DocumentType::Yaml(document) => Box::new(document),
            #[cfg(feature = "toml")]
            DocumentType::Toml(document) => Box::new(document),
            DocumentType::Text(document) => Box::new(document),
            #[cfg(feature = "parquet")]
            DocumentType::Parquet(document) => Box::new(document),
        }
    }
    pub fn ref_inner(&self) -> &dyn Document {
        match self {
            #[cfg(feature = "csv")]
            DocumentType::Csv(document) => document,
            DocumentType::Json(document) => document,
            DocumentType::Jsonl(document) => document,
            #[cfg(feature = "xml")]
            DocumentType::Xml(document) => document,
            DocumentType::Yaml(document) => document,
            #[cfg(feature = "toml")]
            DocumentType::Toml(document) => document,
            DocumentType::Text(document) => document,
            #[cfg(feature = "parquet")]
            DocumentType::Parquet(document) => document,
        }
    }
    pub fn ref_mut_inner(&mut self) -> &mut dyn Document {
        match self {
            #[cfg(feature = "csv")]
            DocumentType::Csv(document) => document,
            DocumentType::Json(document) => document,
            DocumentType::Jsonl(document) => document,
            #[cfg(feature = "xml")]
            DocumentType::Xml(document) => document,
            DocumentType::Yaml(document) => document,
            #[cfg(feature = "toml")]
            DocumentType::Toml(document) => document,
            DocumentType::Text(document) => document,
            #[cfg(feature = "parquet")]
            DocumentType::Parquet(document) => document,
        }
    }
    pub fn guess(metadata: &Metadata) -> Result<Box<dyn Document>> {
        Ok(match &metadata.mime_subtype {
            Some(mime_subtype) => match mime_subtype.as_str() {
                #[cfg(feature = "csv")]
                "csv" => Box::new(Csv {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                "json" => Box::new(Json {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                "x-ndjson" => Box::new(Jsonl {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                #[cfg(feature = "parquet")]
                "parquet" => Box::new(Parquet {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                "text" => Box::new(Text {
                    metadata: metadata.clone(),
                }),
                #[cfg(feature = "toml")]
                "toml" => Box::new(Toml {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                #[cfg(feature = "xml")]
                "xml" => Box::new(Xml {
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                "x-yaml" => Box::new(Yaml {
                    metadata: metadata.clone(),
                }),
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "The document can't be guessed.",
                    ))
                }
            },
            None => DocumentType::default().boxed_inner(),
        })
    }
}

/// Every document_builder that implement this trait can get/write json_value through a connector.
pub trait Document: Send + Sync + DocumentClone + std::fmt::Debug {
    fn metadata(&self) -> Metadata {
        Metadata::default()
    }
    /// Check if the buffer has data
    fn has_data(&self, buffer: &[u8]) -> io::Result<bool> {
        Ok(!buffer.is_empty())
    }
    /// Check if it's possible to append new data into the end of the document
    ///
    /// True: Append the data to the end of the document
    /// False: Replace the document
    fn can_append(&self) -> bool {
        true
    }
    /// Return the header data used to identify when the data start
    ///             |--------|------|--------|
    /// document => | header | data | footer |
    ///             |--------|------|--------|
    fn header(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        Ok(Default::default())
    }
    /// Return the footer data used to identify when the data end
    ///             |--------|------|--------|
    /// document => | header | data | footer |
    ///             |--------|------|--------|
    fn footer(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        Ok(Default::default())
    }
    /// Return the terminator to seperate lines of data
    fn terminator(&self) -> io::Result<Vec<u8>> {
        Ok(Default::default())
    }
    /// Set the entry path. The entry path is the path to reach the data into the document.
    ///
    /// For example, in json, the entry path for `{"field1":{"sub_field1":10}}` will be `/field1/sub_field1`
    fn set_entry_path(&mut self, _entry_point: String) {}
    /// Read buffer of bytes and transform it into dataset
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet>;
    /// Write dataset into a buffer of bytes
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>>;
}

pub trait DocumentClone {
    fn clone_box(&self) -> Box<dyn Document>;
}

impl<T> DocumentClone for T
where
    T: 'static + Document + Clone,
{
    fn clone_box(&self) -> Box<dyn Document> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Document> {
    fn clone(&self) -> Box<dyn Document> {
        self.clone_box()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[cfg(feature = "csv")]
    #[test]
    fn it_should_deserialize_in_csv_type() {
        let config = r#"{"type":"csv"}"#;
        let document_builder_expected = DocumentType::Csv(Csv::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_json_type() {
        let config = r#"{"type":"json"}"#;
        let document_builder_expected = DocumentType::Json(Json::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_jsonl_type() {
        let config = r#"{"type":"jsonl"}"#;
        let document_builder_expected = DocumentType::Jsonl(Jsonl::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_yaml_type() {
        let config = r#"{"type":"yaml"}"#;
        let document_builder_expected = DocumentType::Yaml(Yaml::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[cfg(feature = "xml")]
    #[test]
    fn it_should_deserialize_in_xml_type() {
        let config = r#"{"type":"xml"}"#;
        let document_builder_expected = DocumentType::Xml(Xml::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[cfg(feature = "toml")]
    #[test]
    fn it_should_deserialize_in_toml_type() {
        let config = r#"{"type":"toml"}"#;
        let document_builder_expected = DocumentType::Toml(Toml::default());
        let document_builder_result: DocumentType =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    #[should_panic(expected = "missing field `type`")]
    fn it_should_not_deserialize_without_type() {
        let config = r#"{}"#;
        let _document_builder_result: DocumentType = serde_json::from_str(config).unwrap();
    }
}
