use crate::connector::Connect;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{stdin, stdout, Cursor, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Io {
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for Io {
    fn default() -> Self {
        Io {
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

impl Connect for Io {
    fn set_path_parameters(&mut self, _parameters: Value) {}
    fn path(&self) -> String {
        String::new()
    }
    /// Check if the inner buffer in the connector is empty.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Io::default();
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> Result<bool> {
        Ok(0 == self.inner.get_ref().len())
    }
    /// Return true because the stdout truncate the inner when it write the data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connect;
    ///
    /// let mut connector = Io::default();
    /// assert_eq!(true, connector.will_be_truncated());
    /// ```
    fn will_be_truncated(&self) -> bool {
        true
    }
    /// Get the document size 0.
    ///  
    /// # Example
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connect;
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
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Io::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
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
        trace!(slog_scope::logger(), "Flush");
        stdout().write(self.inner.get_ref())?;
        stdout().flush()?;
        self.inner = Cursor::new(Vec::default());
        info!(slog_scope::logger(), "Flush ended");
        Ok(())
    }
}
