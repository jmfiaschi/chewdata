use json_value_resolve::Resolve;
use quick_xml::{events::*, Reader};
use regex::Regex;
use serde_json::{json, Value as JsonValue};
use std::io::{Error, ErrorKind, Result};

const REGEX_WHITESPACE: &str = r"^\s*$";

/// Configuration options for JsonBuilder
#[derive(Default)]
pub struct JsonConfig {
    charkey: Option<char>,
    attrkey: Option<char>,
    lowercase_tags: Option<bool>,
}

/// JsonBuilder configuration options
impl JsonConfig {
    /// Initialze a new JsonConfig instance.
    ///
    /// This uses the builder pattern. All options are initialized to `None` and can be set using
    /// `self`s methods. Any options not set will use their defaults upon call to `finalize`.
    pub fn new() -> JsonConfig {
        JsonConfig {
            charkey: None,
            attrkey: None,
            lowercase_tags: None,
        }
    }

    /// Key to store character content under.
    ///
    /// (`"_"` by default)
    pub fn charkey<T: Into<char>>(&mut self, key: T) -> &mut JsonConfig {
        self.charkey = Some(key.into());
        self
    }

    /// Key to outer object containing tag attributes.
    ///
    /// (`"$"` by default)
    pub fn attrkey<T: Into<char>>(&mut self, key: T) -> &mut JsonConfig {
        self.attrkey = Some(key.into());
        self
    }

    /// Normalize all tags by converting them to lowercase.
    ///
    /// Corresponds to the `normalizeTags` option in node-xml2js.
    ///
    /// (`false` by default)
    pub fn lowercase_tags(&mut self, flag: bool) -> &mut JsonConfig {
        self.lowercase_tags = Some(flag);
        self
    }

    /// Finalize configuration options and build a JsonBuilder instance
    pub fn finalize(&self) -> JsonBuilder {
        JsonBuilder {
            charkey: self.charkey.unwrap_or('_'),
            attrkey: self.attrkey.unwrap_or('$'),
            lowercase_tags: self.lowercase_tags.unwrap_or(false),
        }
    }
}

// Text storage with state to distingiush between text in elements and text in CDATA sections
// CDATA (literal) text will be added to JSON even when it is whitespace.
struct Text {
    data: String,
    literal: bool,
}

impl Default for Text {
    fn default() -> Text {
        Text {
            data: "".to_owned(),
            literal: false,
        }
    }
}

// Stores state for the current and previous levels in the XML tree.
struct Node {
    value: JsonValue,
    text: Text,
}

impl Node {
    fn new() -> Node {
        Node {
            value: json!({}),
            text: Text::default(),
        }
    }
}

/// JSON builder struct for building JSON from XML
#[derive(Debug)]
pub struct JsonBuilder {
    charkey: char,
    attrkey: char,
    lowercase_tags: bool,
}

impl Default for JsonBuilder {
    fn default() -> JsonBuilder {
        JsonBuilder {
            charkey: '_',
            attrkey: '$',
            lowercase_tags: false,
        }
    }
}

impl JsonBuilder {
    // If text matches only newlines, spaces and tabs
    fn is_whitespace(&self, value: &str) -> Result<bool> {
        let re = Regex::new(REGEX_WHITESPACE).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        Ok(re.is_match(value))
    }

    // This function is used to build out the JSON object.
    // the behavior depends on the `explicit_array` setting. When this value is
    // - true: an array will be created at `key` if it doesn't exist and new values will be pushed
    // - false: `value` is assigned at `key` and converted into an array if there are multiple values
    // at that key
    fn assign_or_push(
        &self,
        object: &mut JsonValue,
        key: &str,
        value: JsonValue,
        explicit_array: bool,
    ) {
        if object.get(key).is_none() {
            if explicit_array {
                object[key] = json!([value]);
            } else {
                object[key] = value;
            }
        } else {
            // Wrap object[key] in an array if it isn't one already
            if !object[key].is_array() {
                let current = object[key].take();
                object[key] = json!([current]);
            }
            if let Some(array) = object[key].as_array_mut() {
                array.push(value);
            }
        }
    }

    // Process start tag
    fn process_start(
        &self,
        event: &BytesStart,
        stack: &mut Vec<Node>,
        reader: &mut Reader<&[u8]>,
    ) -> Result<()> {
        let mut node = Node::new();

        // Add any attributes
        for attr in event.attributes().flatten() {
            let value = attr.decode_and_unescape_value(reader).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let mut key = self.attrkey.to_string().as_bytes().to_vec();
            key.append(&mut attr.key.as_ref().to_vec());
            let value_key = reader.decoder().decode(&key).map_err(|e| Error::new(ErrorKind::InvalidData, e))?.to_string();

            let value_text = JsonValue::resolve(value.to_string());
            self.assign_or_push(&mut node.value, &value_key, value_text, false);
        }

        stack.push(node);
        Ok(())
    }

    // Process text
    fn process_text(&self, event: &BytesText, stack: &mut [Node]) -> Result<()> {
        let cdata = event.unescape().map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if let Some(last_node) = stack.last_mut() {
            let text = &mut last_node.text.data;
            text.push_str(&cdata);
        }

        Ok(())
    }

    // Process end, takes a `tag` rather than an `event` since an Event::Empty(e) uses this function as
    // well
    fn process_end(
        &self,
        tag: &[u8],
        stack: &mut Vec<Node>,
        reader: &mut Reader<&[u8]>,
    ) -> Result<Option<JsonValue>> {
        let close_tag = if self.lowercase_tags {
            reader
                .decoder()
                .decode(tag)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                .to_lowercase()
        } else {
            reader
                .decoder()
                .decode(tag)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                .to_string()
        };
        // The JSON value that which will be nested inside of `outer` (unless we are at EOF)
        let mut inner = match stack.pop() {
            Some(j) => j,
            None => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Expected stack item at close tag.",
                ))
            }
        };
        let stack_len = stack.len();
        let outer = stack.last_mut();

        // This can grow to contain other whitespace characters ('\s')
        let mut whitespace = "".to_owned();
        let text = inner.text.data.as_ref();

        if self.is_whitespace(text)? && !inner.text.literal {
            whitespace.push_str(text);
        } else {
            inner.value[format!(
                "{}text",
                reader
                    .decoder()
                    .decode(self.charkey.to_string().as_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            )] = text.into();
        }

        if json_is_empty(&inner.value) {
            inner.value = JsonValue::String(whitespace);
        }

        // Check if we have closed all open tags
        if stack_len > 0 {
            if let Some(outer) = outer {
                self.assign_or_push(&mut outer.value, &close_tag, inner.value, true);
            }
        } else {
            // At EOF - either wrap result in an explicit root or return inner's value
            let output = json!({
                close_tag: inner.value
            });
            return Ok(Some(output));
        }
        Ok(None)
    }

    // Process empty
    fn process_empty(
        &self,
        event: &BytesStart,
        stack: &mut Vec<Node>,
        reader: &mut Reader<&[u8]>,
    ) -> Result<Option<JsonValue>> {
        self.process_start(event, stack, reader)?;
        self.process_end(event.name().as_ref(), stack, reader)
    }

    // Process XML CDATA
    fn process_cdata(&self, event: &BytesCData, stack: &mut [Node]) -> Result<()> {
        self.process_text(
            &event
                .clone()
                .escape()
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            stack,
        )?;

        if let Some(last_node) = stack.last_mut() {
            last_node.text.literal = true;
        }
        Ok(())
    }

    /// Build JSON from xml
    pub fn build_from_xml(&self, buffer: &[u8]) -> Result<JsonValue> {
        let mut reader = Reader::from_reader(buffer);
        let mut output = JsonValue::Null;
        let mut stack = Vec::new();

        loop {
            match reader.read_event() {
                Ok(Event::Start(ref e)) => self.process_start(e, &mut stack, &mut reader)?,

                Ok(Event::Text(ref e)) => self.process_text(e, &mut stack)?,

                Ok(Event::End(ref e)) => {
                    if let Some(o) = self.process_end(e.name().as_ref(), &mut stack, &mut reader)? {
                        output = o;
                    }
                }

                Ok(Event::CData(ref e)) => self.process_cdata(e, &mut stack)?,

                Ok(Event::Empty(ref e)) => {
                    if let Some(o) = self.process_empty(e, &mut stack, &mut reader)? {
                        output = o;
                    }
                }

                Ok(Event::Eof) => {
                    break;
                }

                // Skip over everything else
                Ok(_) => (),

                Err(e) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Error at position {}: {:?}", reader.buffer_position(), e),
                    ))
                }
            }
        }

        Ok(output)
    }
}

pub fn json_is_empty(node: &JsonValue) -> bool {
    match node {
        JsonValue::Null => true,
        JsonValue::Bool(_) => false,
        JsonValue::Number(_) => false,
        JsonValue::String(ref v) => v.is_empty(),
        JsonValue::Array(ref v) => v.is_empty(),
        JsonValue::Object(ref v) => v.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_xml() {
        let builder = JsonBuilder::default();
        let err = builder
            .build_from_xml("<foo>bar</baz>".as_bytes())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData)
    }

    #[test]
    fn is_whitespace1() {
        let builder = JsonBuilder::default();
        assert!(builder.is_whitespace(" \t \n ").unwrap());
    }

    #[test]
    fn is_whitespace2() {
        let builder = JsonBuilder::default();
        assert!(!builder.is_whitespace(" \t A \n ").unwrap());
    }

    #[test]
    fn assign_or_push1() {
        let builder = JsonBuilder::default();
        let mut actual = json!({});
        let _ = builder.assign_or_push(&mut actual, "A", "B".into(), true);
        let _ = builder.assign_or_push(&mut actual, "C", "D".into(), true);
        let _ = builder.assign_or_push(&mut actual, "C", "E".into(), true);
        let expected: JsonValue = serde_json::from_str(r#"{"A":["B"],"C":["D","E"]}"#).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn assign_or_push2() {
        let builder = JsonConfig::new().finalize();
        let mut actual = json!({});
        let _ = builder.assign_or_push(&mut actual, "A", "B".into(), false);
        let _ = builder.assign_or_push(&mut actual, "C", "D".into(), false);
        let _ = builder.assign_or_push(&mut actual, "C", "E".into(), false);
        let expected: JsonValue = serde_json::from_str(r#"{"A":"B","C":["D","E"]}"#).unwrap();
        assert_eq!(actual, expected);
    }
}
