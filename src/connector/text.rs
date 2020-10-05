use crate::connector::Connect;
use serde::{de, Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(default)]
pub struct Text {
    #[serde(alias = "value")]
    #[serde(alias = "doc")]
    #[serde(alias = "data")]
    #[serde(deserialize_with = "deserialize_inner")]
    #[serde(skip_serializing)]
    // The result value like if the document is in remote.
    // Read the content only with the method io::Read::read().
    document: io::Cursor<Vec<u8>>,
    pub truncate: bool,
    #[serde(skip)]
    is_truncated: bool,
    #[serde(skip)]
    inner: io::Cursor<Vec<u8>>,
}

fn deserialize_inner<'de, D>(deserializer: D) -> Result<io::Cursor<Vec<u8>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(io::Cursor::new(s.into_bytes()))
}

impl fmt::Debug for Text {
    /// Debug a `Text`.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    ///
    /// let connector = Text::new("My text");
    /// assert_eq!("Text { document: \"My text\", inner: \"\", truncate: false }", format!("{:?}",connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Text")
            .field(
                "document",
                &String::from_utf8_lossy(self.document.get_ref()),
            )
            .field("inner", &String::from_utf8_lossy(self.inner.get_ref()))
            .field("truncate", &self.truncate)
            .finish()
    }
}

impl fmt::Display for Text {
    /// Display a `Text`.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use std::io::Write;
    ///
    /// let mut connector = Text::new("My text");
    /// let buffer = "My new text".to_string();
    /// connector.write_all(&buffer.into_bytes()).unwrap();
    /// assert_eq!("My new text", format!("{}",connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &String::from_utf8_lossy(self.inner.get_ref()))
    }
}

impl Text {
    /// Creates a new document type `Text` that implement Connect.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Text::new("My text");
    /// assert_eq!("", format!("{}", connector));
    /// ```
    pub fn new(str: &str) -> Text {
        Text {
            inner: io::Cursor::new(Vec::default()),
            document: io::Cursor::new(str.to_string().into_bytes()),
            truncate: false,
            is_truncated: false,
        }
    }
}

impl Default for Text {
    fn default() -> Self {
        Text {
            inner: io::Cursor::default(),
            document: io::Cursor::default(),
            truncate: false,
            is_truncated: false,
        }
    }
}

impl Connect for Text {
    /// Get the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Text::new("My text");
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// Check if the inner buffer in the connector is empty.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Text::new("");
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// let connector = Text::new("My text");
    /// assert_eq!(false, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> io::Result<bool> {
        if 0 < self.inner.get_ref().len() {
            return Ok(false);
        }
        if 0 < self.document.get_ref().len() {
            return Ok(false);
        }
        Ok(true)
    }
    /// Get the truncate state of the connector.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    ///
    /// let mut connector = Text::default();
    /// assert_eq!(false, connector.will_be_truncated());
    /// connector.truncate = true;
    /// assert_eq!(true, connector.will_be_truncated());
    /// ```
    fn will_be_truncated(&self) -> bool {
        self.truncate && !self.is_truncated
    }
    /// Seek the position into the document, append the inner buffer data and flush the connector.
    ///
    /// # Example: Seek from the end
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector = Text::new(r#"[{"column":"value"}]"#);
    /// connector.write(r#",{"column":"value"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector.seek_and_flush(-1).unwrap();
    ///
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column":"value"},{"column":"value"}]"#, buffer);
    /// ```
    /// # Example: Seek from the start
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    /// use std::io::{Read, Write};
    ///
    /// let str = r#"[{"column1":"value1"}]"#;
    /// let mut connector = Text::new(str);
    /// connector.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector.seek_and_flush((str.len() as i64)-1).unwrap();
    ///
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    /// ```
    /// # Example: If the document must be truncated
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector = Text::new(r#"[{"column1":"value1"}]"#);
    /// connector.truncate = true;
    ///
    /// connector.write(r#"[{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value2"}]"#, buffer);
    ///
    /// connector.write(r#",{"column1":"value3"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value2"},{"column1":"value3"}]"#, buffer);
    /// ```
    fn seek_and_flush(&mut self, position: i64) -> io::Result<()> {
        trace!(slog_scope::logger(), "Seek & Flush");
        let mut position = position;
        if 0 >= (self.len()? as i64 + position) || self.will_be_truncated() {
            position = 0;
            self.document = io::Cursor::new(Vec::default());
            self.is_truncated = true;
        }

        if 0 < position {
            self.document.seek(SeekFrom::Start(position as u64))?;
        }
        if 0 > position {
            self.document.seek(SeekFrom::End(position as i64))?;
        }

        self.document.write_all(self.inner.get_ref())?;
        self.document.set_position(0);
        self.inner = io::Cursor::default();

        info!(slog_scope::logger(), "Seek & Flush ended");
        Ok(())
    }
    /// Get the total document size.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::text::Text;
    /// use chewdata::connector::Connect;
    ///
    /// let mut connector = Text::new(r#"[{"column1":"value1"}]"#);
    /// assert!(0 < connector.len().unwrap(), "The length of the document is not greather than 0.");
    /// ```
    fn len(&self) -> io::Result<usize> {
        Ok(self.document.get_ref().len())
    }
    /// Set the path parameters.
    /// Not used into this component.
    fn set_path_parameters(&mut self, _parameters: Value) {}
    /// Get a new path, but it's not used by this component.
    fn path(&self) -> String {
        String::new()
    }
}

impl io::Read for Text {
    /// Read text document.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use std::io::Read;
    ///
    /// let mut connector = Text::new(r#"My text"#);
    /// let mut buffer = [0; 10];
    ///
    /// let len = connector.read(&mut buffer).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", std::str::from_utf8(&buffer).unwrap().trim_matches(char::from(0)));
    /// let len = connector.read(&mut buffer).unwrap();
    /// assert_eq!(0, len);
    /// ```
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.document.read(buf)
    }
}

impl io::Write for Text {
    /// Write text into the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use std::io::Write;
    ///
    /// let mut connector = Text::new(r#""#);
    /// let mut buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}",connector));
    /// let mut buffer = " and another";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(12, len);
    /// assert_eq!("My text and another", format!("{}",connector));
    /// ```
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }
    /// Push the data form inner buffer to the document and flush the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use std::io::{Write,Read};
    ///
    /// let mut connector = Text::new(r#""#);
    /// connector.write_all("My text".to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!("My text", format!("{}",connector));
    /// connector.flush().unwrap();
    /// assert_eq!("", format!("{}",connector));
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!("My text", buffer);
    /// ```
    ///
    /// # Example: Truncate and flush the data.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use std::io::{Write,Read};
    ///
    /// let mut connector = Text::new(r#"My text"#);
    /// connector.truncate = true;
    ///
    /// assert_eq!("", format!("{}",connector));
    /// connector.write_all("My new text".to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!("My new text", format!("{}",connector));
    /// connector.flush().unwrap();
    /// assert_eq!("", format!("{}",connector));
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!("My new text", buffer);
    /// connector.write_all(" and more !".to_string().into_bytes().as_slice()).unwrap();
    /// connector.flush().unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!("My new text and more !", buffer);
    /// ```
    fn flush(&mut self) -> io::Result<()> {
        trace!(slog_scope::logger(), "Flush");
        if self.will_be_truncated() {
            self.document = io::Cursor::default();
            self.is_truncated = true;
        }
        self.document.write_all(self.inner.get_ref())?;
        self.document.set_position(0);
        self.inner = io::Cursor::default();
        info!(slog_scope::logger(), "Flush ended");
        Ok(())
    }
}
