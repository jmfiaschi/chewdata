use super::authenticator::Authenticator;
use super::{resolve_path, Connect};
use curl::easy::{Easy, List};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Curl {
    pub authenticator: Option<Authenticator>,
    // The FQDN endpoint.
    pub endpoint: String,
    // The http uri.
    pub path: String,
    // The http method.
    pub method: Method,
    // Add complementaries headers. This headers override the default headers.
    pub headers: HashMap<String, String>,
    // Mime type of the document.
    pub mime_type: Option<String>,
    pub parameters: Value,
    // Fush data to an API, read the response and add it into the inner buffer.
    pub flush_and_read: bool,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Curl {
    /// Display the content.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    ///
    /// let connector = Curl::default();
    /// assert_eq!("", format!("{}", connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &String::from_utf8_lossy(self.inner.get_ref()))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Method {
    #[serde(alias = "get")]
    #[serde(alias = "GET")]
    Get,
    #[serde(alias = "post")]
    #[serde(alias = "POST")]
    Post,
    #[serde(alias = "put")]
    #[serde(alias = "PUT")]
    Put,
    #[serde(alias = "delete")]
    #[serde(alias = "DELETE")]
    Delete,
    #[serde(alias = "patch")]
    #[serde(alias = "PATCH")]
    Patch,
    #[serde(alias = "head")]
    #[serde(alias = "HEAD")]
    Head,
    #[serde(alias = "options")]
    #[serde(alias = "OPTIONS")]
    Options,
}

impl Default for Curl {
    fn default() -> Self {
        Curl {
            authenticator: None,
            endpoint: "".to_owned(),
            path: "".to_string(),
            method: Method::Get,
            inner: Cursor::new(Vec::default()),
            parameters: Value::Null,
            flush_and_read: false,
            headers: HashMap::new(),
            mime_type: None,
        }
    }
}

impl Curl {
    /// Test if the path is variable.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::Connect;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// assert_eq!(false, connector.is_variable_path());
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_path_parameters(params);
    /// connector.path = "/get/{{ field }}".to_string();
    /// assert_eq!(true, connector.is_variable_path());
    /// ```
    pub fn is_variable_path(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    fn init_inner(&mut self) -> Result<()> {
        trace!(slog_scope::logger(), "Init inner");
        let mut client = Easy::new();
        let mut headers = List::new();
        let curl = self.clone();
        let resolved_path = curl.path();
        client.url(format!("{}{}", curl.endpoint, resolved_path).as_ref())?;
        client.get(true)?;

        if let Some(auth) = self.authenticator.clone() {
            auth.get().add_authentication(&mut client, &mut headers)?;
        }

        if let Some(mine_type) = self.mime_type.clone() {
            headers.append(format!("{}:{}", http::header::CONTENT_TYPE, mine_type).as_ref())?;
        }

        if 0 < self.headers.len() {
            for (key, value) in self.headers.iter() {
                headers.append(format!("{}:{}", key, value).as_ref())?;
            }
        }

        client.http_headers(headers)?;

        // Log curl information
        info!(slog_scope::logger(), "Url"; "method" => format!("{:?}",curl.method), "endpoint" => curl.endpoint, "uri" => resolved_path);
        client.header_function(|header| {
            info!(
                slog_scope::logger(),
                "{:?}",
                std::str::from_utf8(header).unwrap()
            );
            true
        })?;

        {
            let mut transfer = client.transfer();
            transfer.write_function(|record| Ok(self.inner.write(record).unwrap()))?;
            transfer.perform()?;
        }

        info!(slog_scope::logger(), "Status"; "code" => client.response_code()?);
        let response_code = client.response_code()?;
        match response_code {
            200..=299 => (),
            _ => {
                error!(slog_scope::logger(), "Call in error"; "code" => response_code);
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("HTTP Error '{}'", response_code),
                ));
            }
        }

        // initialize the position of the cursor
        self.inner.set_position(0);
        info!(slog_scope::logger(), "Init inner ended");
        Ok(())
    }
}

impl Connect for Curl {
    /// Get the resolved path.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::Connect;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// connector.path = "/resource/{{ field }}".to_string();
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_path_parameters(params);
    /// assert_eq!("/resource/value", connector.path());
    /// ```
    fn path(&self) -> String {
        match (self.is_variable_path(), self.parameters.clone()) {
            (true, params) => resolve_path(self.path.clone(), params),
            _ => self.path.clone(),
        }
    }
    /// Return true because the curl truncate the inner when it write the data everytime.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::Connect;
    ///
    /// let mut connector = Curl::default();
    /// assert_eq!(true, connector.will_be_truncated());
    /// ```
    fn will_be_truncated(&self) -> bool {
        true
    }
    /// Get the inner buffer reference.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Curl::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    fn set_path_parameters(&mut self, parameters: Value) {
        self.parameters = parameters.clone();
    }
    /// Check only if the current inner buffer is empty.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::Connect;
    ///
    /// let connector = Curl::default();
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> Result<bool> {
        if 0 < self.inner().len() {
            return Ok(false);
        }

        Ok(true)
    }
    /// Set the mime type of the document.
    fn set_mime_type(&mut self, mime_type: mime::Mime) -> () {
        self.mime_type = Some(mime_type.to_string());
    }
}

impl Read for Curl {
    /// Fetch the document from the bucket and push it into the inner memory and read it.
    ///
    /// # Example:
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector_get = Curl::default();
    /// connector_get.endpoint = "http://localhost:8080".to_string();
    /// connector_get.method = Method::Get;
    /// connector_get.path = "/get".to_string();
    /// let mut buffer = String::default();
    /// let len = connector_get.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if 0 == self.inner.get_ref().len() {
            self.init_inner()?;
        }
        self.inner.read(buf)
    }
}

impl Write for Curl {
    /// Write the data into the inner buffer before to flush it.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::Curl;
    /// use std::io::Write;
    ///
    /// let mut connector = Curl::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }
    /// Write all into the document and flush the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::Connect;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Curl::default();
    /// connector_write.endpoint = "http://localhost:8080".to_string();
    /// connector_write.method = Method::Post;
    /// connector_write.path = "/post".to_string();
    ///
    /// connector_write.write(r#"{"column1":"value1"}"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.flush().unwrap();
    /// assert_eq!(r#""#, format!("{}",connector_write));
    fn flush(&mut self) -> Result<()> {
        trace!(slog_scope::logger(), "Flush");

        if self.is_variable_path()
            && self.parameters == Value::Null
            && 0 == self.inner.get_ref().len()
        {
            warn!(slog_scope::logger(), "Can't flush with variable path and without parameters";"path"=>self.path.clone(),"parameters"=>self.parameters.to_string());
            return Ok(());
        }

        let path_resolved = self.path();
        let mut client = Easy::new();
        let mut headers = List::new();
        let list = List::new();

        // initialize the position of the cursor
        self.inner.set_position(0);

        if let Some(mine_type) = self.mime_type.clone() {
            headers.append(format!("{}:{}", http::header::CONTENT_TYPE, mine_type).as_ref())?;
        }

        if 0 < self.headers.len() {
            for (key, value) in self.headers.iter() {
                headers.append(format!("{}:{}", key, value).as_ref())?;
            }
        }

        client.http_headers(list)?;
        client.url(format!("{}{}", self.endpoint, path_resolved).as_ref())?;

        match self.method {
            Method::Post => {
                client.post(true)?;
                client.post_field_size(self.inner.clone().into_inner().len() as u64)?;
            }
            Method::Put => {
                client.put(true)?;
                client.upload(true)?;
                client.in_filesize(self.inner.clone().into_inner().len() as u64)?;
            }
            Method::Patch => client.custom_request("PATCH")?,
            Method::Delete => client.custom_request("DELETE")?,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "This method '{:?}' is not used to update data into a document.",
                        self.method
                    ),
                ))
            }
        };

        if let Some(auth) = self.authenticator.clone() {
            auth.get().add_authentication(&mut client, &mut headers)?;
        }

        client.http_headers(headers)?;

        // Log curl information
        info!(slog_scope::logger(), "Url"; "method" => format!("{:?}",self.method), "endpoint" => self.endpoint.to_owned(), "uri" => path_resolved);
        client.header_function(|header| {
            info!(
                slog_scope::logger(),
                "{:?}",
                std::str::from_utf8(header).unwrap()
            );
            true
        })?;

        let mut received_data = Cursor::new(Vec::default());
        {
            let mut transfer = client.transfer();
            transfer.read_function(|buf| Ok(self.inner.read(buf).unwrap()))?;
            transfer.write_function(|record| Ok(received_data.write(record).unwrap()))?;
            transfer.perform()?;
        }

        let status_code = client.response_code()?;
        info!(slog_scope::logger(), "Status"; "code" => status_code);

        match status_code {
            200..=299 => Ok(()),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Http response code '{}'", status_code),
            )),
        }?;

        self.inner.flush()?;
        self.inner = Cursor::new(Vec::default());

        if self.flush_and_read {
            self.inner.write(received_data.get_ref())?;
            self.inner.set_position(0);
        }

        info!(slog_scope::logger(), "Flush ended");
        Ok(())
    }
}
