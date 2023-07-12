//! Read and Write in Xml format. 
//!
//! ### Configuration
//! 
//! | key         | alias | Description                                                          | Default Value  | Possible Values                                                                |
//! | ----------- | ----- | -------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------ |
//! | type        | -     | Required in order to use this document.                              | `xml`          | `xml`                                                                          |
//! | metadata    | meta  | Metadata describe the resource.                                      | `null`         | [`crate::Metadata`]                                                            |
//! | is_pretty   | -     | Display data in readable format for human.                           | `false`        | `false` / `true`                                                               |
//! | indent_char | -     | Character to use for indentation in pretty mode.                     | `space`        | Simple character                                                               |
//! | indent_size | -     | Number of indentation to use for each line in pretty mode.           | `4`            | unsigned number                                                                |
//! | entry_path  | -     | Use this field if you want to target a specific field in the object. | `/root/*/item` | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |
//! 
//! Examples:
//! 
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "document": {
//!             "type": "xml",
//!             "is_pretty": true,
//!             "indet_char": " ",
//!             "indent_size": 4,
//!             "entry_path": "/root/*/item"
//!         }
//!     },
//!     {
//!         "type": "w"
//!     }
//! ]
//! ```
//! 
//! input:
//! 
//! ```xml
//! <root>
//!     <item field1="value1"/>
//!     ...
//! </root>
//! ```
//! 
//! output:
//! 
//! ```json
//! [{"field1":"value1"},...]
//! ```
use crate::document::Document;
use crate::helper::json_pointer::JsonPointer;
use crate::{DataResult, DataSet, Metadata};
use json_value_merge::Merge;
use json_value_search::Search;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
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
            entry_path: "/".to_string(),
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

impl Document for Xml {
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = entry_path;
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Xml::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item".to_string();
    /// let buffer = r#"<root>
    /// <item>value_1</item>
    /// <item>value_2</item>
    /// </root>"#
    ///     .as_bytes()
    ///     .to_vec();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data_1 = dataset.next().unwrap().to_value();
    /// let expected_data_1: Value = serde_json::from_str(r#"{"_":"value_1"}"#).unwrap();
    /// assert_eq!(expected_data_1, data_1);
    /// let data_2 = dataset.next().unwrap().to_value();
    /// let expected_data_2: Value = serde_json::from_str(r#"{"_":"value_2"}"#).unwrap();
    /// assert_eq!(expected_data_2, data_2);
    /// ```
    #[instrument(skip(buffer), name = "xml::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let mut dataset = Vec::default();
        let entry_path = self.entry_path.clone();
        let str = std::str::from_utf8(buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let mut root_element: Value = jxon::xml_to_json(str).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't parse the xml. {}", e),
            )
        })?;
        Xml::clean_json_value(&mut root_element)?;

        match root_element.clone().search(&entry_path)? {
            Some(record) => {
                let record_trimmed = Xml::trim_array(&record);
                match record_trimmed {
                    Value::Array(vec) => vec.into_iter().for_each(|record| {
                        trace!(
                            record = format!("{:?}", &record).as_str(),
                            "Record deserialized"
                        );
                        dataset.push(DataResult::Ok(record));
                    }),
                    _ => {
                        trace!(
                            record = format!("{:?}", &record_trimmed).as_str(),
                            "Record deserialized"
                        );
                        dataset.push(DataResult::Ok(record_trimmed));
                    }
                }
            }
            None => {
                warn!(
                    entry_path = format!("{:?}", entry_path).as_str(),
                    record = format!("{:?}", root_element.clone()).as_str(),
                    "Entry path not found"
                );
                dataset.push(DataResult::Err((
                    root_element,
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Entry path '{}' not found.", entry_path),
                    ),
                )));
            }
        };

        Ok(dataset)
    }
    /// See [`Document::write`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use chewdata::DataResult;
    /// use serde_json::Value;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item".to_string();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"<item><object column_1="line_1"/></item>"#.as_bytes().to_vec(),
    ///     buffer
    /// );
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"object":[{"column_1":"line_2"}]}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"<item><object column_1="line_2"/></item>"#
    ///         .as_bytes()
    ///         .to_vec(),
    ///     buffer
    /// );
    /// ```
    #[instrument(skip(dataset), name = "xml::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();
        let header = self.header(dataset)?;
        let footer = self.footer(dataset)?;

        for data in dataset {
            let record = data.to_value();
            let mut new_value = self.value_entry_path(record.clone())?;
            Xml::convert_numeric_to_string(&mut new_value);
            Xml::add_attribute_character(&mut new_value)?;

            trace!(
                record = format!("{:?}", record).as_str(),
                "Record serialized"
            );

            let mut xml_new_value = self.value_to_xml(&new_value)?;
            if !header.is_empty() && !footer.is_empty() {
                xml_new_value = xml_new_value.replace(std::str::from_utf8(&header).unwrap(), "");
                xml_new_value = xml_new_value.replace(std::str::from_utf8(&footer).unwrap(), "");
            }

            buffer.append(&mut xml_new_value.as_bytes().to_vec());
        }

        Ok(buffer)
    }
    /// See [`Document::header`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item".to_string();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
    /// )];
    /// let buffer = document.header(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"<root>"#.as_bytes().to_vec(),
    ///     buffer
    /// );
    /// ```
    fn header(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::DataResult;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item".to_string();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
    /// )];
    /// let buffer = document.footer(&dataset).unwrap();
    /// assert_eq!(
    ///     r#"</root>"#.as_bytes().to_vec(),
    ///     buffer
    /// );
    /// ```
    fn footer(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
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
    /// See [`Document::has_data`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::xml::Xml;
    /// use chewdata::document::Document;
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root/*/item".to_string();
    /// let buffer = r#"<root><item column_1="line_1"/></root>"#.as_bytes();
    /// assert_eq!(true, document.has_data(buffer).unwrap());
    /// ```
    fn has_data(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.is_empty() {
            return Ok(false);
        }

        let str = std::str::from_utf8(buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let data_value = jxon::xml_to_json(str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        if data_value.search(self.entry_path.as_str())?.is_none() {
            return Ok(false);
        }

        Ok(!buffer.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_data_in_target_position() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let buffer = r#"<root>
<item key_1="value_1" />
<item key_1="value_2" />
</root>"#
            .as_bytes()
            .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let expected_data_1: Value = serde_json::from_str(r#"{"key_1":"value_1"}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_2: Value = serde_json::from_str(r#"{"key_1":"value_2"}"#).unwrap();
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn read_data_in_body() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let buffer = r#"<root>
<item>value_1</item>
<item>value_2</item>
</root>"#
            .as_bytes()
            .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let expected_data_1: Value = serde_json::from_str(r#"{"_":"value_1"}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_2: Value = serde_json::from_str(r#"{"_":"value_2"}"#).unwrap();
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn write() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"<item><object column_1="line_1"/></item>"#.as_bytes().to_vec(),
            buffer
        );
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"object":[{"column_1":"line_2"}]}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(
            r#"<item><object column_1="line_2"/></item>"#.as_bytes().to_vec(),
            buffer
        );
    }
    #[test]
    fn header() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
        )];
        let buffer = document.header(&dataset).unwrap();
        assert_eq!(r#"<root>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn footer() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"object":[{"column_1":"line_1"}]}"#).unwrap(),
        )];
        let buffer = document.footer(&dataset).unwrap();
        assert_eq!(r#"</root>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn has_data_with_empty_document() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let buffer = r#"<root></root>"#.as_bytes();
        assert_eq!(false, document.has_data(&buffer).unwrap());
        let buffer = r#"<root/>"#.as_bytes();
        assert_eq!(false, document.has_data(buffer).unwrap());
    }
    #[test]
    fn has_data_with_empty_remote_document() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let buffer = r#""#.as_bytes();
        assert_eq!(false, document.has_data(buffer).unwrap());
    }
    #[test]
    fn has_data_with_not_empty_remote_document() {
        let mut document = Xml::default();
        document.entry_path = "/root/*/item".to_string();
        let buffer = r#"<root><item column_1="line_1"/></root>"#.as_bytes();
        assert_eq!(true, document.has_data(buffer).unwrap());
    }
}
