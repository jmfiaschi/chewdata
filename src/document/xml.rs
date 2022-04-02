use crate::connector::Connector;
use crate::document::Document;
use crate::helper::json_pointer::JsonPointer;
use crate::Metadata;
use crate::{DataResult, Dataset};
use async_std::io::prelude::WriteExt;
use async_stream::stream;
use async_trait::async_trait;
use futures::AsyncReadExt;
use json_value_merge::Merge;
use json_value_search::Search;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Xml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub indent_char: u8,
    pub indent_size: usize,
    // Elements to target
    pub entry_path: String,
}

impl Default for Xml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::TEXT.to_string()),
            mime_subtype: Some(mime::XML.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Xml {
            metadata,
            is_pretty: false,
            indent_char: b' ',
            indent_size: 4,
            entry_path: "/root/*/item".to_string(),
        }
    }
}

impl Xml {
    /// Convert Number/Bool to String. Jxon not handle Number/Bool/Null transformation.
    /// Todo : https://github.com/definitelynobody/jxon/blob/948bea9475ca836ab2a253d87ae04b1d60a00258/src/to_xml.rs#L16-L18
    fn convert_numeric_to_string(json_value: &mut Value) {
        match json_value {
            Value::Array(vec) => {
                for value in vec {
                    Xml::convert_numeric_to_string(value);
                }
            }
            Value::Object(map) => {
                for (_string, value) in map.iter_mut() {
                    Xml::convert_numeric_to_string(value);
                }
            }
            Value::Bool(value) => *json_value = Value::String(value.to_string()),
            Value::Number(value) => *json_value = Value::String(value.to_string()),
            Value::Null => *json_value = Value::String("".to_string()),
            _ => (),
        }
    }
    // jxon add some characteres. This function clean the json_value and normalize it.
    // Use this method after the convertion xml_to_json.
    fn clean_json_value(value: &mut Value) -> io::Result<()> {
        let remove_added_char = Regex::new(r#"\$([^"]+)"#).unwrap();
        let new_json: String = remove_added_char
            .replace_all(value.to_string().as_ref(), "$1")
            .to_string();
        let transform_string_to_scalar =
            Regex::new(r#""([1-9][[:digit:]]+|[0-9][0-9]*\.[0-9]+|true|false)""#).unwrap();
        let new_json_transformed: String = transform_string_to_scalar
            .replace_all(new_json.as_ref(), "$1")
            .to_string();
        *value = serde_json::from_str(new_json_transformed.as_ref())?;
        Ok(())
    }
    // jxon add some characteres in order to define attributes.
    // This function add this attribute '$' for every fields except "_". Use this method before the convertion json_to_xml.
    fn add_attribute_character(value: &mut Value) -> io::Result<()> {
        let re = Regex::new(r#""([^_]|[^"]{2,})": *""#).unwrap();
        let new_json: String = re
            .replace_all(value.to_string().as_ref(), r#""$$$1":""#)
            .to_string();
        *value = serde_json::from_str(new_json.as_ref())?;
        Ok(())
    }
    // Remove cumulative array into a value, useful after a search.
    fn trim_array(value: &Value) -> Value {
        match value {
            Value::Array(vec) => {
                if vec.len() > 1 {
                    value.clone()
                } else {
                    Xml::trim_array(&vec[0])
                }
            }
            _ => value.clone(),
        }
    }
    /// Build json value with entry_path
    fn value_entry_path(&self, value: Value) -> io::Result<Value> {
        let mut fields: Vec<&str> = self.entry_path.split('/').collect();
        let last_field_opt = fields.pop();
        let mut value_entry_path: Value = Value::Null;

        if let Some(last_field) = last_field_opt {
            match last_field.parse::<usize>() {
                Ok(_) => {
                    value_entry_path
                        .merge_in(&self.entry_path.to_string().to_json_pointer(), value)?;
                }
                Err(_) => match last_field {
                    "*" => {
                        value_entry_path
                            .merge_in(&self.entry_path.to_string().to_json_pointer(), value)?;
                    }
                    _ => {
                        value_entry_path.merge_in(
                            &self.entry_path.to_string().to_json_pointer(),
                            Value::Array(vec![value]),
                        )?;
                    }
                },
            }
        }

        Ok(value_entry_path)
    }
    /// Document an entry xml with the entry_path.
    fn xml_entry_path(&self) -> io::Result<String> {
        let entry_path_value = self.value_entry_path(Value::Object(Map::default()))?;
        self.value_to_xml(&entry_path_value)
    }
    /// Transform a json value to xml.
    fn value_to_xml(&self, value: &Value) -> io::Result<String> {
        let indent = match self.is_pretty {
            true => Some((self.indent_char, self.indent_size)),
            false => None,
        };

        jxon::json_to_xml(value.to_string().as_ref(), indent)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
    }
}

#[async_trait]
impl Document for Xml {
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = entry_path;
    }
    /// See [`Document::header`] for more details.
    async fn header(&self, _connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        let xml_entry_path = match self.xml_entry_path() {
            Ok(xml) => xml,
            Err(e) => {
                warn!(
                    entry_path = self.entry_path.clone().as_str(),
                    error = e.to_string().as_str(),
                    "Can't generate the xml entry path start"
                );
                "".to_string()
            }
        };

        let header: String = xml_entry_path
            .split('<')
            .filter(|node| !node.contains('/') && !node.is_empty())
            .map(|node| format!("<{}", node))
            .collect();

        Ok(header.as_bytes().to_vec())
    }
    /// See [`Document::footer`] for more details.
    async fn footer(&self, _connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        let xml_entry_path = match self.xml_entry_path() {
            Ok(xml) => xml,
            Err(e) => {
                warn!(
                    entry_path = self.entry_path.clone().as_str(),
                    error = e.to_string().as_str(),
                    "Can't generate the xml entry path end"
                );
                "".to_string()
            }
        };

        let footer: String = xml_entry_path
            .split('>')
            .filter(|node| node.contains("</") && !node.is_empty())
            .map(|node| format!("{}>", node))
            .collect();

        Ok(footer.as_bytes().to_vec())
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Xml::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read data in key of an xml object
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     document.entry_path = "/root/*/item".to_string();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"<root>
    ///     <item key_1="value_1" />
    ///     <item key_1="value_2" />
    ///     </root>"#));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data_1 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_1: Value = serde_json::from_str(r#"{"key_1":"value_1"}"#)?;
    ///     assert_eq!(expected_data_1, data_1);
    ///
    ///     let data_2 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_2: Value = serde_json::from_str(r#"{"key_1":"value_2"}"#)?;
    ///     assert_eq!(expected_data_2, data_2);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read data in body of an xml object
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     document.entry_path = "/root/*/item".to_string();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"<root>
    ///     <item>value_1</item>
    ///     <item>value_2</item>
    ///     </root>"#));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data_1 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_1: Value = serde_json::from_str(r#"{"_":"value_1"}"#)?;
    ///     assert_eq!(expected_data_1, data_1);
    ///
    ///     let data_2 = dataset.next().await.unwrap().to_value();
    ///     let expected_data_2: Value = serde_json::from_str(r#"{"_":"value_2"}"#)?;
    ///     assert_eq!(expected_data_2, data_2);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut string = String::new();
        connector.read_to_string(&mut string).await?;

        let mut root_element: Value = jxon::xml_to_json(string.as_ref()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't parse the xml. {}", e),
            )
        })?;
        Xml::clean_json_value(&mut root_element)?;
        let mut records_option = root_element.search(&self.entry_path)?;
        if let Some(records) = records_option {
            records_option = Some(Xml::trim_array(&records));
        } else {
            warn!(
                entry_path = &self.entry_path.as_str(),
                "Entry path not found"
            );
            return Ok(Box::pin(
                stream! { yield DataResult::Ok(serde_json::Value::Null); },
            ));
        }
        let entry_path = self.entry_path.clone();
        Ok(Box::pin(stream! {
            match records_option {
                Some(record) => match record {
                    Value::Array(vec) => {
                        for json_value in vec {
                            trace!(record = format!("{:?}",json_value).as_str(),  "Record deserialized");
                            yield DataResult::Ok(json_value.clone());
                        }
                    }
                    _ => {
                        trace!(record = format!("{:?}",record).as_str(),  "Record deserialized");
                        yield DataResult::Ok(record.clone());
                    }
                },
                None => {
                    warn!(path = entry_path.clone().as_str(), xml = string.clone().as_str(),  "This path not found into the document.");
                    yield DataResult::Err((
                        Value::Null,
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "This path '{}' not found into the document.",
                                entry_path.clone()
                            ),
                        ),
                    ));
                }
            };
        }))
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///     document.entry_path = "/root/*/item".to_string();
    ///
    ///     let value: Value = serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"<item><object column_1="line_1"/></item>"#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"object":[{"column_1":"line_2"}]}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"<item><object column_1="line_1"/></item><item><object column_1="line_2"/></item>"#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"object":[{"_":"line_3"}]}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"<item><object column_1="line_1"/></item><item><object column_1="line_2"/></item><item><object>line_3</object></item>"#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn write_data(&mut self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        let header = self.header(connector).await?;
        let footer = self.footer(connector).await?;

        let mut new_value = self.value_entry_path(value)?;
        Xml::convert_numeric_to_string(&mut new_value);
        Xml::add_attribute_character(&mut new_value)?;

        let mut xml_new_value = self.value_to_xml(&new_value)?;

        if !header.is_empty() && !footer.is_empty() {
            xml_new_value = xml_new_value.replace(std::str::from_utf8(&header).unwrap(), "");
            xml_new_value = xml_new_value.replace(std::str::from_utf8(&footer).unwrap(), "");
        }

        connector.write_all(xml_new_value.as_bytes()).await
    }
    /// See [`Document::close`] for more details.
    ///
    /// # Example: Remote document don't have data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"<root><item column_1="line_1"/></root>"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has empty data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#"<root></root>"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"<root><item column_1="line_1"/></root>"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#"<root><item column_1="line_1"/></root>"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"<item column_1="line_2"/></root>"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn close(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        let remote_len = connector.len().await?;
        let buff = connector.inner().clone();

        connector.clear();

        let header = self.header(connector).await?;
        let footer = self.footer(connector).await?;

        if remote_len == 0
            || remote_len == header.len() + footer.len()
        {
            connector.write_all(&header).await?;
            connector.write_all(&buff).await?;
            connector.write_all(&footer).await?;
        }

        if remote_len > header.len() + footer.len() {
            connector.write_all(&buff).await?;
            connector.write_all(&footer).await?;
        }

        Ok(())
    }
    /// See [`Document::has_data`] for more details.
    ///
    /// # Example: Empty data
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#"<root></root>"#);
    ///     connector.fetch().await?;
    ///     document.entry_path = "/root/*/item".to_string();
    ///
    ///     let mut buffer = String::default();
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(false, document.has_data(buffer.as_str()).unwrap());
    ///
    ///     let mut connector = InMemory::new(r#"<root/>"#);
    ///     connector.fetch().await?;
    ///     document.entry_path = "/root/*/item".to_string();
    ///
    ///     let mut buffer = String::default();
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(false, document.has_data(buffer.as_str()).unwrap());
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Empty remote document
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///     connector.fetch().await?;
    ///     document.entry_path = "/root/*/item".to_string();
    ///
    ///     let mut buffer = String::default();
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(false, document.has_data(buffer.as_str()).unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Not empty remote document
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Xml::default();
    ///     let mut connector = InMemory::new(r#"<root><item column_1="line_1"/></root>"#);
    ///     connector.fetch().await?;
    ///     document.entry_path = "/root/*/item".to_string();
    ///
    ///     let mut buffer = String::default();
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(true, document.has_data(buffer.as_str()).unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    fn has_data(&self, str: &str) -> io::Result<bool> {
        let data_value = jxon::xml_to_json(str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        if data_value.search(self.entry_path.as_str())?.is_none() {
            return Ok(false);
        }

        Ok(!matches!(str, ""))
    }
}
