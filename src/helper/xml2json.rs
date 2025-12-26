use json_value_resolve::Resolve;
use quick_xml::{events::*, Reader};
use serde_json::{json, Value as JsonValue};
use std::io::{Error, ErrorKind, Result};

/// Configuration options for JsonBuilder
#[derive(Default)]
pub struct JsonConfig {
    charkey: Option<char>,
    attrkey: Option<char>,
    lowercase_tags: Option<bool>,
}

impl JsonConfig {
    pub fn new() -> JsonConfig {
        JsonConfig::default()
    }

    pub fn charkey<T: Into<char>>(&mut self, key: T) -> &mut JsonConfig {
        self.charkey = Some(key.into());
        self
    }

    pub fn attrkey<T: Into<char>>(&mut self, key: T) -> &mut JsonConfig {
        self.attrkey = Some(key.into());
        self
    }

    pub fn lowercase_tags(&mut self, flag: bool) -> &mut JsonConfig {
        self.lowercase_tags = Some(flag);
        self
    }

    pub fn finalize(&self) -> JsonBuilder {
        JsonBuilder {
            charkey: self.charkey.unwrap_or('_'),
            attrkey: self.attrkey.unwrap_or('$'),
            lowercase_tags: self.lowercase_tags.unwrap_or(false),
        }
    }
}

#[derive(Default)]
struct Text {
    data: String,
    literal: bool,
}

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
    #[inline]
    fn is_whitespace(&self, value: &str) -> bool {
        value.chars().all(|c| c.is_whitespace())
    }

    fn assign_or_push(
        &self,
        object: &mut JsonValue,
        key: &str,
        value: JsonValue,
        explicit_array: bool,
    ) {
        match object.get_mut(key) {
            Some(JsonValue::Array(arr)) => arr.push(value),
            Some(other) => {
                let old = std::mem::replace(other, JsonValue::Null);
                *other = JsonValue::Array(vec![old, value]);
            }
            None => {
                if explicit_array {
                    object[key] = JsonValue::Array(vec![value]);
                } else {
                    object[key] = value;
                }
            }
        }
    }

    fn process_start(&self, event: &BytesStart, stack: &mut Vec<Node>) -> Result<()> {
        let mut node = Node::new();

        for attr_res in event.attributes() {
            let attr = attr_res.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let key = format!(
                "{}{}",
                self.attrkey,
                std::str::from_utf8(attr.key.as_ref())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            );
            let value = attr
                .unescape_value()
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                .to_string();
            self.assign_or_push(&mut node.value, &key, JsonValue::resolve(value), false);
        }

        stack.push(node);
        Ok(())
    }

    fn process_text(&self, event: &BytesText, stack: &mut [Node]) -> Result<()> {
        let cdata = event
            .decode()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if let Some(last_node) = stack.last_mut() {
            last_node.text.data.push_str(&cdata);
        }

        Ok(())
    }

    fn process_end(
        &self,
        tag: &[u8],
        stack: &mut Vec<Node>,
        reader: &mut Reader<&[u8]>,
    ) -> Result<Option<JsonValue>> {
        let tag_str = reader
            .decoder()
            .decode(tag)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let close_tag = if self.lowercase_tags {
            tag_str.to_lowercase()
        } else {
            tag_str.to_string()
        };

        let mut inner = stack
            .pop()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Expected stack item at close tag"))?;
        let stack_len = stack.len();

        let text_key = format!("{}text", self.charkey);

        if self.is_whitespace(&inner.text.data) && !inner.text.literal {
            if inner.value.as_object().unwrap().is_empty() {
                inner.value = JsonValue::String(inner.text.data.clone());
            }
        } else {
            inner.value[text_key] = inner.text.data.into();
        }

        if stack_len > 0 {
            if let Some(outer) = stack.last_mut() {
                self.assign_or_push(&mut outer.value, &close_tag, inner.value, true);
            }
        } else {
            let output = json!({ close_tag: inner.value });
            return Ok(Some(output));
        }

        Ok(None)
    }

    fn process_empty(
        &self,
        event: &BytesStart,
        stack: &mut Vec<Node>,
        reader: &mut Reader<&[u8]>,
    ) -> Result<Option<JsonValue>> {
        self.process_start(event, stack)?;
        self.process_end(event.name().as_ref(), stack, reader)
    }

    fn process_cdata(&self, event: &BytesCData, stack: &mut [Node]) -> Result<()> {
        // Decode CDATA to &str
        let decoded = event
            .decode()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        // Append decoded text directly to the current node
        if let Some(last_node) = stack.last_mut() {
            last_node.text.data.push_str(&decoded);
            last_node.text.literal = true;
        }

        Ok(())
    }

    pub fn build_from_xml(&self, buffer: &[u8]) -> Result<JsonValue> {
        let mut reader = Reader::from_reader(buffer);
        let mut output = JsonValue::Null;
        let mut stack = Vec::with_capacity(16);

        loop {
            match reader.read_event() {
                Ok(Event::Start(ref e)) => self.process_start(e, &mut stack)?,
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
                Ok(Event::Eof) => break,
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
        assert!(builder.is_whitespace(" \t \n "));
    }

    #[test]
    fn is_whitespace2() {
        let builder = JsonBuilder::default();
        assert!(!builder.is_whitespace(" \t A \n "));
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
