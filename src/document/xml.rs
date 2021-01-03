extern crate jxon;

use crate::connector::Connector;
use crate::document::Document;
use crate::helper::json_pointer::JsonPointer;
use crate::step::{Data, DataResult};
use crate::Metadata;
use genawaiter::sync::GenBoxed;
use json_value_merge::Merge;
use json_value_search::Search;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    pub entry_path: String,
}

impl Default for Xml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::TEXT_XML.to_string()),
            ..Default::default()
        };
        Xml {
            metadata,
            is_pretty: false,
            indent_char: b' ',
            indent_size: 4,
            entry_path: "/root/0/item".to_string(),
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
        let transform_string_to_scalar = Regex::new(r#""([0-9.]+|true|false)""#).unwrap();
        let new_json_transformed: String = transform_string_to_scalar
            .replace_all(new_json.as_ref(), "$1")
            .to_string();
        *value = serde_json::from_str(new_json_transformed.as_ref())?;
        Ok(())
    }
    // jxon add some characteres in order to define attributes.
    // This function add this attribute '$' for every fields. Use this method before the convertion json_to_xml.
    fn add_attribute_character(value: &mut Value) -> io::Result<()> {
        let re = Regex::new(r#""([^"]+)": *""#).unwrap();
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
    /// Document an entry xml with the entry_path.
    fn xml_entry_path(&self) -> io::Result<String> {
        let mut entry_path_value: Value = Value::Null;
        entry_path_value.merge_in(
            &self.entry_path.to_string().to_json_pointer(),
            Value::Array(Vec::default()),
        );

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

impl Document for Xml {
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// Read toml data.
    ///
    /// # Example: Should read toml data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item/*".to_string();
    /// let connector = InMemory::new(r#"<root>
    /// <item key_1="value_1" />
    /// <item key_1="value_2" />
    /// </root>"#);
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line_1 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_1: Value = serde_json::from_str(r#"{"key_1":"value_1"}"#).unwrap();
    /// assert_eq!(expected_line_1, line_1);
    ///
    /// let line_2 = data_iter.next().unwrap().to_json_value();
    /// let expected_line_2: Value = serde_json::from_str(r#"{"key_1":"value_2"}"#).unwrap();
    /// assert_eq!(expected_line_2, line_2);
    /// ```
    fn read_data(&self, connector: Box<dyn Connector>) -> io::Result<Data> {
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut string = String::new();
        let mut connector = connector;

        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_XML.to_string());
        connector.set_metadata(metadata.clone());
        connector.read_to_string(&mut string)?;

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
            warn!(slog_scope::logger(), "Entry path not found"; "entry_path" => &self.entry_path);
            return Ok(GenBoxed::new_boxed(|_| async move {}));
        }

        let entry_path = self.entry_path.clone();
        let data = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            match records_option {
                Some(record) => match record {
                    Value::Array(vec) => {
                        for json_value in vec {
                            debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",json_value));
                            co.yield_(DataResult::Ok(json_value.clone())).await;
                        }
                    }
                    _ => {
                        debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        co.yield_(DataResult::Ok(record.clone())).await;
                    }
                },
                None => {
                    warn!(slog_scope::logger(), "This path not found into the document."; "path"=>entry_path.clone(), "xml"=>string.clone());
                    co.yield_(DataResult::Err((
                        Value::Null,
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "This path '{}' not found into the document.",
                                entry_path.clone()
                            ),
                        ),
                    )))
                    .await;
                }
            };
            debug!(slog_scope::logger(), "End generator");
        });

        debug!(slog_scope::logger(), "Read data ended"; "documents" => format!("{:?}", self));
        Ok(data)
    }
    /// Write complex xml data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Xml::default();
    /// let mut connector = InMemory::new(r#""#);
    /// document.entry_path = "/root/0/item".to_string();
    ///
    /// let value: Value = serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<root><item><object column_1="line_1"/></item>"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"object":[{"column_1":"line_2"}]}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<root><item><object column_1="line_1"/></item><item><object column_1="line_2"/></item>"#, &format!("{}", connector));
    /// ```
    /// # Example: Write multi data into truncate inner document and document init with '[]'.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Xml::default();
    /// let mut connector = InMemory::new(r#"<root></root>"#);
    /// document.entry_path = "/root/0/item".to_string();
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<item column_1="line_1"/>"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<item column_1="line_1"/><item column_1="line_2"/>"#, &format!("{}", connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Xml::default();
    /// let mut connector = InMemory::new(r#"<root><item column_1="line_1"/></root>"#);
    /// connector.can_truncate = true;
    /// document.entry_path = "/root/0/item".to_string();
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<root><item column_1="line_2"/>"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"<root><item column_1="line_2"/><item column_1="line_3"/>"#, &format!("{}", connector));
    /// ```
    fn write_data_result(
        &mut self,
        connector: &mut dyn Connector,
        data_result: DataResult,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        connector.set_parameters(value.clone());

        let xml_entry_path = match self.xml_entry_path() {
            Ok(xml) => xml,
            Err(e) => {
                warn!(slog_scope::logger(), "Entry path not valid in order to write data."; "entry_path" => self.entry_path.clone(), "error" => e.to_string());
                "".to_string()
            }
        };

        let xml_entry_path_begin: String = xml_entry_path
            .split('<')
            .filter(|node| !node.contains('/') && !node.is_empty())
            .map(|node| format!("<{}", node))
            .collect();
        let xml_entry_path_end: String = xml_entry_path
            .split('<')
            .filter(|node| node.contains('/') && !node.is_empty())
            .map(|node| format!("<{}", node))
            .collect();

        let mut new_value: Value = Value::Null;
        new_value.merge_in(
            &self.entry_path.to_string().to_json_pointer(),
            Value::Array(vec![value]),
        );
        Xml::convert_numeric_to_string(&mut new_value);
        Xml::add_attribute_character(&mut new_value)?;

        let mut xml_new_value = self.value_to_xml(&new_value)?;
        xml_new_value = xml_new_value.replace(xml_entry_path_begin.as_str(), "");
        xml_new_value = xml_new_value.replace(xml_entry_path_end.as_str(), "");

        if connector.is_empty()? && connector.inner().is_empty()
            || connector.will_be_truncated() && connector.inner().is_empty()
        {
            connector.write_all(xml_entry_path_begin.as_bytes())?;
        }

        connector.write_all(xml_new_value.as_bytes())?;

        debug!(slog_scope::logger(), "Write data ended."; "data" => format!("{:?}", data_result));
        Ok(())
    }
    /// flush xml data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    /// use std::io::Read;
    ///
    /// let mut document = Xml::default();
    /// let mut connector = InMemory::new(r#"<root></root>"#);
    /// document.entry_path = "/root/0/item".to_string();
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"<root><item column_1="line_1"/></root>"#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector,DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"<root><item column_1="line_1"/><item column_1="line_2"/></root>"#, buffer);
    /// ```
    fn flush(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        debug!(slog_scope::logger(), "Flush called.");
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_XML.to_string());
        connector.set_metadata(metadata.clone());

        let indent = match self.is_pretty {
            true => Some((self.indent_char, self.indent_size)),
            false => None,
        };

        let mut entry_path_value: Value = Value::Null;
        entry_path_value.merge_in(
            &self.entry_path.to_string().to_json_pointer(),
            Value::Array(Vec::default()),
        );

        let xml_entry_path = match jxon::json_to_xml(entry_path_value.to_string().as_ref(), indent)
        {
            Ok(xml) => xml,
            Err(e) => {
                warn!(slog_scope::logger(), "Entry path not valid in order to write data."; "entry_path" => self.entry_path.clone(), "error" => e.to_string());
                "".to_string()
            }
        };

        let xml_entry_path_end: String = xml_entry_path
            .split('<')
            .filter(|node| node.contains('/') && !node.is_empty())
            .map(|node| format!("<{}", node))
            .collect();

        connector.write_all(xml_entry_path_end.as_bytes())?;
        connector.seek_and_flush(-1 as i64 * xml_entry_path_end.len() as i64)?;

        debug!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
}
