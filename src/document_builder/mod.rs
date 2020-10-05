pub mod csv;
pub mod json;
pub mod jsonl;
pub mod toml;
pub mod xml;
pub mod yaml;

use self::csv::Csv;
use self::json::Json;
use self::jsonl::Jsonl;
use self::toml::Toml;
use self::xml::Xml;
use self::yaml::Yaml;
use crate::connector::Connector;
use crate::processor::{Data, DataResult};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum DocumentBuilder {
    #[serde(rename = "csv")]
    Csv(Csv),
    #[serde(rename = "json")]
    Json(Json),
    #[serde(rename = "jsonl")]
    Jsonl(Jsonl),
    #[serde(rename = "xml")]
    Xml(Xml),
    #[serde(rename = "yaml")]
    #[serde(alias = "yml")]
    Yaml(Yaml),
    #[serde(alias = "toml")]
    Toml(Toml),
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        DocumentBuilder::Json(Json::default())
    }
}

impl DocumentBuilder {
    pub fn inner(self) -> Box<dyn Build> {
        match self {
            DocumentBuilder::Csv(builder) => Box::new(builder),
            DocumentBuilder::Json(builder) => Box::new(builder),
            DocumentBuilder::Jsonl(builder) => Box::new(builder),
            DocumentBuilder::Xml(builder) => Box::new(builder),
            DocumentBuilder::Yaml(builder) => Box::new(builder),
            DocumentBuilder::Toml(builder) => Box::new(builder),
        }
    }
    pub fn get(&self) -> Box<&dyn Build> {
        match self {
            DocumentBuilder::Csv(builder) => Box::new(builder),
            DocumentBuilder::Json(builder) => Box::new(builder),
            DocumentBuilder::Jsonl(builder) => Box::new(builder),
            DocumentBuilder::Xml(builder) => Box::new(builder),
            DocumentBuilder::Yaml(builder) => Box::new(builder),
            DocumentBuilder::Toml(builder) => Box::new(builder),
        }
    }
    pub fn get_mut(&mut self) -> Box<&mut dyn Build> {
        match self {
            DocumentBuilder::Csv(builder) => Box::new(builder),
            DocumentBuilder::Json(builder) => Box::new(builder),
            DocumentBuilder::Jsonl(builder) => Box::new(builder),
            DocumentBuilder::Xml(builder) => Box::new(builder),
            DocumentBuilder::Yaml(builder) => Box::new(builder),
            DocumentBuilder::Toml(builder) => Box::new(builder),
        }
    }
}

/// Every document_builder that implement this trait can get/write json_value through a connector.
pub trait Build: Send {
    /// This method get json_values from a document. Each json_value are yield for better performance.
    fn read_data(&self) -> io::Result<Data>;
    /// This method format the json_value that will be pushed into the document through a connector.
    fn write_data_result(&mut self, data_result: DataResult) -> io::Result<()>;
    /// Flush and push all data store into the document_builder into the connector writer.
    fn flush(&mut self) -> io::Result<()>;
    /// Retrieve the current connector
    fn connector(&self) -> &Connector;
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn it_should_deserialize_in_csv_type() {
        let config = r#"{"type":"csv"}"#;
        let document_builder_expected = DocumentBuilder::Csv(Csv::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_json_type() {
        let config = r#"{"type":"json"}"#;
        let document_builder_expected = DocumentBuilder::Json(Json::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_jsonl_type() {
        let config = r#"{"type":"jsonl"}"#;
        let document_builder_expected = DocumentBuilder::Jsonl(Jsonl::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_yaml_type() {
        let config = r#"{"type":"yaml"}"#;
        let document_builder_expected = DocumentBuilder::Yaml(Yaml::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_xml_type() {
        let config = r#"{"type":"xml"}"#;
        let document_builder_expected = DocumentBuilder::Xml(Xml::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    fn it_should_deserialize_in_toml_type() {
        let config = r#"{"type":"toml"}"#;
        let document_builder_expected = DocumentBuilder::Toml(Toml::default());
        let document_builder_result: DocumentBuilder =
            serde_json::from_str(config).expect("Can't deserialize the config");
        assert_eq!(document_builder_expected, document_builder_result);
    }
    #[test]
    #[should_panic(expected = "missing field `type`")]
    fn it_should_not_deserialize_without_type() {
        let config = r#"{}"#;
        let _document_builder_result: DocumentBuilder = serde_json::from_str(config).unwrap();
    }
}
