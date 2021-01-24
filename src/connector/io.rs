use crate::connector::Connector;
use crate::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{stdin, stdout, Cursor, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(default)]
pub struct Io {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for Io {
    fn default() -> Self {
        Io {
            metadata: Metadata::default(),
            inner: Cursor::default(),
        }
    }
}

impl fmt::Display for Io {
    /// Can't display the content of `Io`.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    ///
    /// let connector = Io::default();
    /// assert_eq!("", format!("{}", connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &String::from_utf8_lossy(self.inner.get_ref()))
    }
}

impl Connector for Io {
    fn set_parameters(&mut self, _parameters: Value) {}
    fn is_variable_path(&self) -> bool { false }
    fn path(&self) -> String {
        String::new()
    }
    /// Check if the inner buffer in the connector is empty.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Io::default();
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> Result<bool> {
        Ok(0 == self.inner.get_ref().len())
    }
    /// Get the document size 0.
    ///  
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Io::default();
    /// assert_eq!(0, connector.len().unwrap());
    /// ```
    fn len(&self) -> Result<usize> {
        Ok(0)
    }
    /// Get the connect buffer inner reference.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Io::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    fn erase(&mut self) -> Result<()> { 
        info!(slog_scope::logger(), "Can't clean the document"; "connector" => format!("{:?}", self), "path" => self.path());
        Ok(()) 
    }
}

impl Read for Io {
    /// Read the data from the stdin and write it into the buffer.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        stdin().read(buf)
    }
}

impl Write for Io {
    /// Write the data into the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use std::io::Write;
    ///
    /// let mut connector = Io::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }
    /// The flush send all the data into the stdout.
    fn flush(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Flush started");
        stdout().write_all(self.inner.get_ref())?;
        stdout().flush()?;
        self.inner = Cursor::new(Vec::default());
        debug!(slog_scope::logger(), "Flush ended");
        Ok(())
    }
}
