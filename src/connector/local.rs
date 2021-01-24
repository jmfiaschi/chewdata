use crate::connector::Connector;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use glob::glob;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{Cursor, Error, ErrorKind, Read, Result, SeekFrom, Write};
use std::path::Path;
use std::vec::IntoIter;
use std::{fmt, fs};

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Local {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub path: String,
    pub parameters: Value,
    #[serde(skip)]
    paths: Option<IntoIter<String>>,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Local {
    /// Display the inner content.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    ///
    /// let local = Local::default();
    /// assert_eq!("", format!("{}", local));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &String::from_utf8_lossy(self.inner.get_ref()))
    }
}

impl Default for Local {
    fn default() -> Self {
        Local {
            metadata: Metadata::default(),
            path: "".to_string(),
            paths: None,
            inner: Cursor::new(Vec::default()),
            parameters: Value::Null,
        }
    }
}

impl Clone for Local {
    fn clone(&self) -> Self {
        Local {
            metadata: self.metadata.to_owned(),
            path: self.path.to_owned(),
            paths: None,
            inner: Cursor::new(Vec::default()),
            parameters: self.parameters.to_owned(),
        }
    }
}

impl Local {
    fn init_paths(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Init paths"; "path" => self.path.to_owned());
        let paths: Vec<String> = match glob(self.path.as_str()) {
            Ok(paths) => Ok(paths
                .filter(|p| p.is_ok())
                .map(|p| p.unwrap().display().to_string())
                .collect()),
            Err(e) => Err(Error::new(ErrorKind::InvalidInput, e)),
        }?;

        if paths.is_empty() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("No files found with this path '{}'.", self.path),
            ));
        }

        self.paths = Some(paths.into_iter());
        debug!(slog_scope::logger(), "Init paths ended");
        Ok(())
    }
    fn init_inner(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Init inner buffer");
        self.inner = Cursor::new(Vec::default());
        debug!(slog_scope::logger(), "Init inner buffer ended");
        Ok(())
    }
}

impl Connector for Local {
    /// Get the connect buffer inner reference.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Local::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// Set the path parameters.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// assert_eq!(Value::Null, connector.parameters);
    /// let params: Value = Value::String("my param".to_string());
    /// connector.set_parameters(params.clone());
    /// assert_eq!(params.clone(), connector.parameters.clone());
    /// ```
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters.clone();
    }
    /// Test if the path is variable.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// assert_eq!(false, connector.is_variable_path());
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_variable_path());
    /// ```
    fn is_variable_path(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    /// Get the resolved path.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// assert_eq!("/dir/filename_value.ext", connector.path());
    /// ```
    fn path(&self) -> String {
        match (self.is_variable_path(), self.parameters.clone()) {
            (true, params) => self.path.clone().replace_mustache(params),
            _ => self.path.clone(),
        }
    }
    /// Test if the inner buffer and the document are empty.
    /// Not work for wildcard path.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Local::default();
    /// connector.path = "./Cargo.toml".to_string();
    /// assert_eq!(false, connector.is_empty().unwrap());
    /// connector.path = "./null_file".to_string();
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> Result<bool> {
        if 0 < self.inner().len() {
            return Ok(false);
        }

        if self.paths.is_some() {
            return Err(Error::new(
                ErrorKind::Other,
                "Is_empty method not available for wildcard path.",
            ));
        }

        match fs::metadata(self.path()) {
            Ok(metadata) => {
                if 0 < metadata.len() {
                    return Ok(false);
                }
            }
            Err(_) => {
                return Ok(true);
            }
        };

        Ok(true)
    }
    /// Get the total document size.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Local::default();
    /// connector.path = "./Cargo.toml".to_string();
    /// assert!(0 < connector.len().unwrap(), "The length of the document is not greather than 0");
    /// connector.path = "./not_found_file".to_string();
    /// assert_eq!(0, connector.len().unwrap());
    /// ```
    fn len(&self) -> Result<usize> {
        if let Some(paths) = &self.paths {
            if 1 < paths.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "len() method not available for wildcard path.",
                ));
            }
        }

        match fs::metadata(self.path()) {
            Ok(metadata) => Ok(metadata.len() as usize),
            Err(_) => Ok(0),
        }
    }
    /// Seek the position into the document, append the inner buffer data and flush the connector.
    ///
    /// # Example: Seek from the end
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Local::default();
    /// connector_write.path = "./data/out/test_local_seek_and_flush_1".to_string();
    /// connector_write.erase();
    ///
    /// connector_write.write(r#"[{"column1":"value1"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    /// connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    /// ```
    /// # Example: Seek from the start
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Local::default();
    /// connector_write.path = "./data/out/test_local_seek_and_flush_2".to_string();
    /// connector_write.erase();
    ///
    /// let str = r#"[{"column1":"value1"}]"#;
    /// connector_write.write(str.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    /// connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush((str.len() as i64)-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    /// ```
    fn seek_and_flush(&mut self, position: i64) -> Result<()> {
        debug!(slog_scope::logger(), "Seek & Flush"; "path"=>self.path.clone());

        if self.is_variable_path()
            && self.parameters == Value::Null
            && self.inner.get_ref().is_empty()
        {
            warn!(slog_scope::logger(), "Can't flush with variable path and without parameters";"path"=>self.path.clone(),"parameters"=>self.parameters.to_string());
            return Ok(());
        }

        let mut position = position;

        if 0 >= (self.len()? as i64 + position) {
            position = 0;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(Path::new(self.path().as_str()))?;

        if 0 < position {
            file.seek(SeekFrom::Start(position as u64))?;
        }
        if 0 > position {
            file.seek(SeekFrom::End(position as i64))?;
        }

        file.write_all(self.inner.get_ref())?;
        self.inner.flush()?;
        self.inner = Cursor::new(Vec::default());

        debug!(slog_scope::logger(), "Seek & Flush ended"; "path"=>self.path.clone());
        Ok(())
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    fn erase(&mut self) -> Result<()> {
        info!(slog_scope::logger(), "Erase"; "connector" => format!("{}", self), "path" => self.path());

        let mut file = OpenOptions::new()
            .read(false)
            .create(true)
            .append(false)
            .write(true)
            .truncate(true)
            .open(Path::new(self.path().as_str()))?;

        file.write_all(String::default().as_bytes())?;
        debug!(slog_scope::logger(), "Erase ended"; "path" => self.path());

        Ok(())
    }
}

impl Read for Local {
    /// The content of every file into Local::paths is push into the Local::inner (in memory) and after the Local::inner is readed.
    ///
    /// # Example: Read multi-files
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// connector.path = "./data/one_line.*".to_string();
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(1000 < len, "Should read multiple file in one time.");
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(1000 < len, "Should read multiple time the flow.");
    /// ```
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.paths.is_none() {
            self.init_paths()?;
        }

        if self.inner.position() >= self.inner.get_ref().len() as u64 {
            self.init_inner()?;
        }

        if self.inner.get_ref().is_empty() {
            match &mut self.paths {
                Some(paths) => {
                    match paths.next() {
                        Some(path) => {
                            let mut buffer = Vec::default();
                            let mut file = OpenOptions::new()
                                .read(true)
                                .write(false)
                                .create(false)
                                .append(false)
                                .truncate(false)
                                .open(Path::new(&path))?;

                            file.read_to_end(&mut buffer)?;
                            self.inner.write_all(buffer.as_slice())?;
                            self.inner.set_position(0);

                            trace!(slog_scope::logger(),
                                "Content pushed into the inner";
                                "file" => path
                            );
                        }
                        None => {
                            // reinit the paths after a full iteration.
                            self.init_paths()?;
                        }
                    };
                }
                None => (),
            };
        }

        self.inner.read(buf)
    }
}

impl Write for Local {
    /// Write the data into the inner buffer before to flush it.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use std::io::Write;
    ///
    /// let mut connector = Local::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }
    /// Write all into the file and flush the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Local::default();
    /// connector_write.path = "./data/out/test_local_flush_1".to_string();
    /// connector_write.erase();
    ///
    /// connector_write.write(r#"{"column1":"value1"}"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.flush().unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column1":"value1"}"#, buffer);
    ///
    /// connector_write.write(r#"{"column1":"value2"}"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.flush().unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column1":"value1"}{"column1":"value2"}"#, buffer);
    /// ```
    fn flush(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Flush started"; "path"=>self.path.clone());

        if self.is_variable_path()
            && self.parameters == Value::Null
            && self.inner.get_ref().is_empty()
        {
            warn!(slog_scope::logger(), "Can't flush with variable path and without parameters";"path"=>self.path.clone(),"parameters"=>self.parameters.to_string());
            return Ok(());
        }

        // initialize the position of the cursor
        self.inner.set_position(0);
        let mut file = OpenOptions::new()
            .read(false)
            .create(true)
            .append(true)
            .write(true)
            .truncate(false)
            .open(Path::new(self.path().as_str()))?;

        file.write_all(self.inner.get_ref())?;

        self.inner.flush()?;
        self.inner = Cursor::new(Vec::default());

        debug!(slog_scope::logger(), "Flush ended"; "path"=>self.path.clone());
        Ok(())
    }
}
