//! Read and Write in Xml format.
//!
//! ### Configuration
//!
//! | key         | alias | Description                                                          | Default Value  | Possible Values                                                                |
//! | ----------- | ----- | -------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------ |
//! | type        | -     | Required in order to use this document.                              | `xml`          | `xml`                                                                          |
//! | metadata    | meta  | Metadata describe the resource.                                      | `null`         | [`crate::Metadata`]                                                            |
//! | is_pretty   | -     | Display data in readable format for human.                           | `false`        | `false` / `true`                                                               |
//! | indent_char | -     | Character to use for indentation in pretty mode.                     | ` `            | Simple character                                                               |
//! | indent_size | -     | Number of indentation to use for each line in pretty mode.           | `4`            | unsigned number                                                                |
//! | entry_path  | -     | Use this field if you want to target a specific field in the object. The 'root' is extracted from this field. If it's not possible, the field 'root' will be used. | `/root/*/item` | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |
//! | attribute_key | -   | Key use to identify attribute xml value .                            | `@`            | Simple character                                                               |
//! | text_key    | -     | Key use to identify text xml value.                                  | `$`            | Simple character                                                               |
//! | root        | -     | root value by default to us by default. If the root can't be determine, this field is used.    | `root`            | String                                            |
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
//!             "entry_path": "/root/item",
//!             "attribute_key": "@",
//!             "text_key": "$",
//!             "root": "root"
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
use crate::helper::string::DisplayOnlyForDebugging;
use crate::helper::xml2json::JsonConfig;
use crate::{DataResult, DataSet, Metadata};
use json_value_merge::Merge;
use json_value_search::Search;
use quick_xml::se::Serializer;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Write;
use std::io;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Xml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub indent_char: char,
    pub indent_size: usize,
    #[serde(alias = "attk")]
    pub attribute_key: char,
    #[serde(alias = "txtk")]
    pub text_key: char,
    // Elements to target
    pub root: String,
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
            indent_char: ' ',
            indent_size: 4,
            attribute_key: '@',
            text_key: '$',
            entry_path: "/".to_string(),
            root: "root".to_string(),
        }
    }
}

impl Xml {
    /// Convert a json value into xml.
    fn convert_value_to_xml(&self, value: &Value) -> io::Result<String> {
        let mut buffer = String::new();
        let entry_path_root = self.entry_path_root();
        let mut ser = Serializer::with_root(&mut buffer, Some(entry_path_root.as_str())).unwrap();

        if self.is_pretty {
            ser.indent(self.indent_char, self.indent_size);
        }
        ser.expand_empty_elements(true);

        value
            .serialize(ser)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(buffer)
    }
    fn convert_xml_to_value(&self, buffer: &[u8]) -> io::Result<Value> {
        let mut config = JsonConfig::new();
        config.attrkey(self.attribute_key);
        config.charkey(self.text_key);

        config.finalize().build_from_xml(buffer)
    }
    // Return the root from the entry path. If empty, take the root parameter.
    fn entry_path_root(&self) -> String {
        let mut entry_path_splitted = self.entry_path.split('/').collect::<Vec<&str>>();

        if entry_path_splitted.is_empty() {
            return self.root.clone();
        }

        // remove the first empty element
        entry_path_splitted.remove(0);

        match entry_path_splitted.first() {
            Some(first) => {
                if !first.is_empty() {
                    return first.to_string();
                }
                self.root.clone()
            }
            None => self.root.clone(),
        }
    }
    // Return the entry path without the root value.
    fn entry_path_without_root(&self) -> String {
        self.entry_path
            .replacen(format!("/{}", self.entry_path_root()).as_str(), "", 1)
    }
}

impl Document for Xml {
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = entry_path;
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Xml::default().metadata.merge(&self.metadata)
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
    /// document.entry_path = "/root/item".to_string();
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
        let entry_path = &self.entry_path;
        let root_element = self.convert_xml_to_value(buffer)?;

        match root_element.clone().search(entry_path)? {
            Some(record) => match record {
                Value::Array(vec) => vec.into_iter().for_each(|record| {
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record deserialized"
                    );
                    dataset.push(DataResult::Ok(record));
                }),
                _ => {
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record deserialized"
                    );
                    dataset.push(DataResult::Ok(record));
                }
            },
            None => {
                warn!(
                    entry_path = format!("{:?}", entry_path).as_str(),
                    record = root_element.display_only_for_debugging(),
                    "Entry path not found"
                );
                dataset.push(DataResult::Err((
                    root_element,
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Entry path '{}' not found", entry_path),
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
    /// document.entry_path = "/custom_root".to_string();
    /// let dataset = vec![DataResult::Ok(
    ///     serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
    /// )];
    /// let buffer = document.write(&dataset).unwrap();
    /// assert_eq!(r#"<column_1>line_1</column_1>"#.as_bytes().to_vec(), buffer);
    /// ```
    #[instrument(skip(dataset), name = "xml::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::default();
        let entry_path_root = &self.entry_path_root();
        let values = Value::Array(
            dataset
                .iter()
                .map(|data| data.to_value())
                .collect::<Vec<Value>>(),
        );
        let mut value = Value::default();

        let re = Regex::new(
            format!(
                r#"^(?<descriptor><[?]xml[^>]*>)?(?<root_open_tag><{root}[^>]*>){newline}(?<body>.*){newline}(?<root_close_tag><\/{root}>)$"#,
                root = entry_path_root, newline = match self.is_pretty {
                    true => "\\\\n",
                    false => ""
                }
            )
            .as_str(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        value.merge_in("/", &values)?;

        let mut xml_with_root = self.convert_value_to_xml(&value)?;
        xml_with_root = xml_with_root.replace('\n', "\\n");

        let mut xml_without_root = re.replace(xml_with_root.as_str(), "$body").to_string();
        xml_without_root = xml_without_root.replace("\\n", "\n");

        trace!(
            xml = xml_without_root.display_only_for_debugging(),
            "Record serialized"
        );

        buffer.append(&mut xml_without_root.as_bytes().to_vec());

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
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root".to_string();
    /// let buffer = document.header(&vec![]).unwrap();
    /// assert_eq!(
    ///     r#"<root>"#.as_bytes().to_vec(),
    ///     buffer
    /// );
    /// ```
    fn header(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut value = Value::default();
        value.merge_in(
            &self.entry_path_without_root(),
            &match self.is_pretty {
                true => Value::String("\n".to_string()),
                false => Value::default(),
            },
        )?;
        let xml_with_entry_path = self.convert_value_to_xml(&value)?;

        let header: String = xml_with_entry_path
            .split('<')
            .filter(|node| !node.contains('/') && !node.is_empty())
            .fold(String::new(), |mut output, b| {
                let _ = write!(output, "<{}", b);
                output
            });

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
    ///
    /// let mut document = Xml::default();
    /// document.entry_path = "/root".to_string();
    /// let buffer = document.footer(&vec![]).unwrap();
    /// assert_eq!(
    ///     r#"</root>"#.as_bytes().to_vec(),
    ///     buffer
    /// );
    /// ```
    fn footer(&self, _dataset: &DataSet) -> io::Result<Vec<u8>> {
        let mut value = Value::default();
        value.merge_in(
            &self.entry_path_without_root(),
            &match self.is_pretty {
                true => Value::String("\n".to_string()),
                false => Value::default(),
            },
        )?;
        let xml_with_entry_path = self.convert_value_to_xml(&value)?;

        let footer: String = xml_with_entry_path
            .split('>')
            .filter(|node| node.contains("</") && !node.is_empty())
            .fold(String::new(), |mut output, b| {
                let _ = write!(output, "{}>", b);
                output
            });

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
    /// document.entry_path = "/root".to_string();
    /// let buffer = r#"<root><item column_1="line_1"/></root>"#.as_bytes();
    /// assert_eq!(true, document.has_data(buffer).unwrap());
    /// ```
    fn has_data(&self, buffer: &[u8]) -> io::Result<bool> {
        if buffer.is_empty() {
            return Ok(false);
        }

        let data_value = self.convert_xml_to_value(buffer)?;
        match data_value.search(self.entry_path.as_str())? {
            Some(Value::Array(array)) => Ok(!array.is_empty()),
            Some(Value::String(string)) => Ok(!string.is_empty()),
            Some(Value::Bool(_)) | Some(Value::Number(_)) | Some(Value::Object(_)) => Ok(true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_data_with_elements() {
        let mut document = Xml::default();
        document.entry_path = "/custom_root/item".to_string();
        let buffer = r#"<custom_root>
<item><key_1>value_1</key_1></item>
<item><key_1>value_2</key_1></item>
</custom_root>"#
            .as_bytes()
            .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let expected_data_1: Value =
            serde_json::from_str(r#"{"key_1":[{"$text":"value_1"}]}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_2: Value =
            serde_json::from_str(r#"{"key_1":[{"$text":"value_2"}]}"#).unwrap();
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn read_data_with_element_attributs() {
        let mut document = Xml::default();
        document.entry_path = "/custom_root/item".to_string();
        document.attribute_key = '&';
        let buffer = r#"<custom_root>
<item attr_1="value_1"/>
<item attr_1="value_2"/>
</custom_root>"#
            .as_bytes()
            .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let expected_data_1: Value = serde_json::from_str(r#"{"&attr_1":"value_1"}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_2: Value = serde_json::from_str(r#"{"&attr_1":"value_2"}"#).unwrap();
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn read_data_with_text() {
        let mut document = Xml::default();
        document.entry_path = "/custom_root/item".to_string();
        document.text_key = '$';
        let buffer = r#"<custom_root>
<item>value_1</item>
<item>value_2</item>
</custom_root>"#
            .as_bytes()
            .to_vec();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data_1 = dataset.next().unwrap().to_value();
        let expected_data_1: Value = serde_json::from_str(r#"{"$text":"value_1"}"#).unwrap();
        assert_eq!(expected_data_1, data_1);
        let data_2 = dataset.next().unwrap().to_value();
        let expected_data_2: Value = serde_json::from_str(r#"{"$text":"value_2"}"#).unwrap();
        assert_eq!(expected_data_2, data_2);
    }
    #[test]
    fn write() {
        let mut document = Xml::default();
        document.entry_path = "/custom_root".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"<column_1>line_1</column_1>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_with_object_in_sub_level() {
        let mut document = Xml::default();
        document.entry_path = "/custom_root/item".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();

        assert_eq!(r#"<column_1>line_1</column_1>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_with_attribute_key() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"elt":{"@column_1":"line_1"}}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"<elt column_1="line_1"/>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn write_with_text_key() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let dataset = vec![DataResult::Ok(
            serde_json::from_str(r#"{"elt":{"$text":"value_1"}}"#).unwrap(),
        )];
        let buffer = document.write(&dataset).unwrap();
        assert_eq!(r#"<elt>value_1</elt>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn header_without_first_level() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let buffer = document.header(&vec![]).unwrap();
        assert_eq!(r#"<root>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn header_with_first_level() {
        let mut document = Xml::default();
        document.entry_path = "/root/item".to_string();
        let buffer = document.header(&vec![]).unwrap();
        assert_eq!(r#"<root><item>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn footer_without_first_level() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let buffer = document.footer(&vec![]).unwrap();
        assert_eq!(r#"</root>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn footer_with_first_level() {
        let mut document = Xml::default();
        document.entry_path = "/root/item".to_string();
        let buffer = document.footer(&vec![]).unwrap();
        assert_eq!(r#"</item></root>"#.as_bytes().to_vec(), buffer);
    }
    #[test]
    fn has_data_with_empty_document() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let buffer = r#"<root></root>"#.as_bytes();
        assert_eq!(false, document.has_data(&buffer).unwrap());
        let buffer = r#"<root/>"#.as_bytes();
        assert_eq!(false, document.has_data(buffer).unwrap());
    }
    #[test]
    fn has_data_with_first_level() {
        let mut document = Xml::default();
        document.entry_path = "/root/item".to_string();
        let buffer = r#""#.as_bytes();
        assert_eq!(false, document.has_data(buffer).unwrap());
    }
    #[test]
    fn has_data_with_not_empty_document() {
        let mut document = Xml::default();
        document.entry_path = "/root".to_string();
        let buffer = r#"<root><item column_1="line_1"/></root>"#.as_bytes();
        assert_eq!(true, document.has_data(buffer).unwrap());
    }
    #[test]
    fn has_data_with_bad_entry_point() {
        let mut document = Xml::default();
        document.entry_path = "/root/value".to_string();
        let buffer = r#"<root><item column_1="line_1"/></root>"#.as_bytes();
        assert_eq!(false, document.has_data(buffer).unwrap());
    }
}
