#[cfg(feature = "use_csv_document")]
pub mod csv;
pub mod json;
pub mod jsonl;
pub mod text;
#[cfg(feature = "use_toml_document")]
pub mod toml;
#[cfg(feature = "use_xml_document")]
pub mod xml;
#[cfg(feature = "use_yaml_document")]
pub mod yaml;

#[cfg(feature = "use_csv_document")]
use self::csv::Csv;
use self::json::Json;
use self::jsonl::Jsonl;
use self::text::Text;
#[cfg(feature = "use_toml_document")]
use self::toml::Toml;
#[cfg(feature = "use_xml_document")]
use self::xml::Xml;
#[cfg(feature = "use_yaml_document")]
use self::yaml::Yaml;
use crate::connector::Connector;
use crate::step::Data;
use serde::{Deserialize, Serialize};
use std::io;
use super::Metadata;
use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum DocumentType {
    #[cfg(feature = "use_csv_document")]
    #[serde(rename = "csv")]
    Csv(Csv),
    #[serde(rename = "json")]
    Json(Json),
    #[serde(rename = "jsonl")]
    Jsonl(Jsonl),
    #[cfg(feature = "use_xml_document")]
    #[serde(rename = "xml")]
    Xml(Xml),
    #[cfg(feature = "use_yaml_document")]
    #[serde(rename = "yaml")]
    #[serde(alias = "yml")]
    Yaml(Yaml),
    #[cfg(feature = "use_toml_document")]
    #[serde(rename = "toml")]
    Toml(Toml),
    #[serde(rename = "text")]
    #[serde(alias = "txt")]
    Text(Text),
}

impl Default for DocumentType {
    fn default() -> Self {
        DocumentType::Json(Json::default())
    }
}

impl DocumentType {
    pub fn document_inner(self) -> Box<dyn Document> {
        match self {
            #[cfg(feature = "use_csv_document")]
            DocumentType::Csv(document) => Box::new(document),
            DocumentType::Json(document) => Box::new(document),
            DocumentType::Jsonl(document) => Box::new(document),
            #[cfg(feature = "use_xml_document")]
            DocumentType::Xml(document) => Box::new(document),
            #[cfg(feature = "use_yaml_document")]
            DocumentType::Yaml(document) => Box::new(document),
            #[cfg(feature = "use_toml_document")]
            DocumentType::Toml(document) => Box::new(document),
            DocumentType::Text(document) => Box::new(document),
        }
    }
    pub fn document(&self) -> &dyn Document {
        match self {
            #[cfg(feature = "use_csv_document")]
            DocumentType::Csv(document) => document,
            DocumentType::Json(document) => document,
            DocumentType::Jsonl(document) => document,
            #[cfg(feature = "use_xml_document")]
            DocumentType::Xml(document) => document,
            #[cfg(feature = "use_yaml_document")]
            DocumentType::Yaml(document) => document,
            #[cfg(feature = "use_toml_document")]
            DocumentType::Toml(document) => document,
            DocumentType::Text(document) => document,
        }
    }
    pub fn document_mut(&mut self) -> &mut dyn Document {
        match self {
            #[cfg(feature = "use_csv_document")]
            DocumentType::Csv(document) => document,
            DocumentType::Json(document) => document,
            DocumentType::Jsonl(document) => document,
            #[cfg(feature = "use_xml_document")]
            DocumentType::Xml(document) => document,
            #[cfg(feature = "use_yaml_document")]
            DocumentType::Yaml(document) => document,
            #[cfg(feature = "use_toml_document")]
            DocumentType::Toml(document) => document,
            DocumentType::Text(document) => document,
        }
    }
}

/// Every document_builder that implement this trait can get/write json_value through a connector.
#[async_trait]
pub trait Document: Send + Sync + DocumentClone {
    /// Apply some actions and read the data though the Connector.
    async fn read_data(&self, reader: &mut Box<dyn Connector>) -> io::Result<Data>;
    /// Format the data result into the document format, apply some action and write into the connector.
    async fn write_data(
        &self,
        writer: &mut dyn Connector,
        value: Value,
    ) -> io::Result<()>;
    /// Apply actions and flush the connector.
    async fn flush(&self, writer: &mut dyn Connector) -> io::Result<()>;
    fn metadata(&self) -> Metadata {
        Metadata::default()
    }
    /// Check if the str in argument has an empty data
    fn has_data(&self, str: &str) -> bool {
        match str {
            "" => false,
            _ => true
        }
    }
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
    // #[cfg(feature = "use_csv_document")]
    // #[test]
    // fn it_should_deserialize_in_csv_type() {
    //     let config = r#"{"type":"csv"}"#;
    //     let document_builder_expected = DocumentType::Csv(Csv::default());
    //     let document_builder_result: DocumentType =
    //         serde_json::from_str(config).expect("Can't deserialize the config");
    //     assert_eq!(document_builder_expected, document_builder_result);
    // }
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
    // #[cfg(feature = "use_yaml_document")]
    // #[test]
    // fn it_should_deserialize_in_yaml_type() {
    //     let config = r#"{"type":"yaml"}"#;
    //     let document_builder_expected = DocumentType::Yaml(Yaml::default());
    //     let document_builder_result: DocumentType =
    //         serde_json::from_str(config).expect("Can't deserialize the config");
    //     assert_eq!(document_builder_expected, document_builder_result);
    // }
    // #[cfg(feature = "use_xml_document")]
    // #[test]
    // fn it_should_deserialize_in_xml_type() {
    //     let config = r#"{"type":"xml"}"#;
    //     let document_builder_expected = DocumentType::Xml(Xml::default());
    //     let document_builder_result: DocumentType =
    //         serde_json::from_str(config).expect("Can't deserialize the config");
    //     assert_eq!(document_builder_expected, document_builder_result);
    // }
    // #[cfg(feature = "use_toml_document")]
    // #[test]
    // fn it_should_deserialize_in_toml_type() {
    //     let config = r#"{"type":"toml"}"#;
    //     let document_builder_expected = DocumentType::Toml(Toml::default());
    //     let document_builder_result: DocumentType =
    //         serde_json::from_str(config).expect("Can't deserialize the config");
    //     assert_eq!(document_builder_expected, document_builder_result);
    // }
    #[test]
    #[should_panic(expected = "missing field `type`")]
    fn it_should_not_deserialize_without_type() {
        let config = r#"{}"#;
        let _document_builder_result: DocumentType = serde_json::from_str(config).unwrap();
    }
}
