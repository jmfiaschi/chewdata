use super::authenticator::AuthenticatorType;
use super::{Connector, Paginator};
use crate::helper::mustache::Mustache;
use crate::Metadata;
use async_trait::async_trait;
use json_value_merge::Merge;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Error, ErrorKind, Result, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{collections::HashMap, fmt};
use surf::http::{headers, Method, Url};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Curl {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "auth")]
    #[serde(rename = "authenticator")]
    pub authenticator_type: Option<Box<AuthenticatorType>>,
    // The endpoint like http://my_site.com:80
    pub endpoint: String,
    // The http uri.
    #[serde(alias = "uri")]
    pub path: String,
    // The http method.
    pub method: Method,
    // Add complementaries headers. This headers override the default headers.
    pub headers: Box<HashMap<String, String>>,
    #[serde(alias = "params")]
    pub parameters: Value,
    pub limit: usize,
    pub skip: usize,
    #[serde(alias = "paginator")]
    pub paginator_parameters: Option<PaginatorParameters>,
    #[serde(skip)]
    pub inner: Box<Cursor<Vec<u8>>>,
}
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct PaginatorParameters {
    #[serde(default = "default_limit")]
    #[serde(alias = "limit")]
    pub limit_name: String,
    #[serde(default = "default_skip")]
    #[serde(alias = "skip")]
    pub skip_name: String,
}

fn default_limit() -> String {
    "limit".to_string()
}

fn default_skip() -> String {
    "skip".to_string()
}

impl Default for Curl {
    fn default() -> Self {
        Curl {
            metadata: Metadata::default(),
            authenticator_type: None,
            endpoint: "".into(),
            path: "".into(),
            method: Method::Get,
            headers: Box::new(HashMap::default()),
            parameters: Value::Null,
            limit: 100,
            skip: 0,
            paginator_parameters: None,
            inner: Box::new(Cursor::default()),
        }
    }
}

impl fmt::Display for Curl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Curl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Curl")
            .field("metadata", &self.metadata)
            .field("authenticator_type", &self.authenticator_type)
            .field("endpoint", &self.endpoint)
            .field("path", &self.path)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("parameters", &self.parameters)
            .field("limit", &self.limit)
            .field("skip", &self.skip)
            .field("paginator_parameters", &self.paginator_parameters)
            .finish()
    }
}

#[async_trait]
impl Connector for Curl {
    /// See [`Connector::path`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// connector.path = "/resource/{{ field }}".to_string();
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// assert_eq!("/resource/value", connector.path());
    /// ```
    fn path(&self) -> String {
        match (self.is_variable(), self.parameters.clone()) {
            (true, params) => {
                let mut path = self.path.clone();
                path.replace_mustache(params);
                path
            },
            _ => self.path.clone(),
        }
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// let params = serde_json::from_str(r#"{"field":"test"}"#).unwrap();
    /// assert_eq!(false, connector.is_resource_will_change(Value::Null).unwrap());
    /// connector.path = "/dir/static.ext".to_string();
    /// assert_eq!(false, connector.is_resource_will_change(Value::Null).unwrap());
    /// connector.path = "/dir/dynamic_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_resource_will_change(params).unwrap());
    /// ```
    fn is_resource_will_change(&self, new_parameters: Value) -> Result<bool> {
        if !self.is_variable() {
            return Ok(false);
        }

        let mut actuel_path = self.path.clone();
        actuel_path.replace_mustache(self.parameters.clone());
        
        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if actuel_path == new_path {
            return Ok(false);
        }

        Ok(true)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use surf::http::Method;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/json".to_string();
    ///     connector.fetch().await?;
    ///     assert!(0 < connector.inner().len(), "The inner connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn fetch(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Fetch started");
        let client = surf::client();
        let url = Url::parse(format!("{}{}", self.endpoint, self.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let mut request_builder = surf::RequestBuilder::new(self.method, url);

        if let Some(ref mut authenticator_type) = self.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            authenticator.set_parameters(self.parameters.clone());
            request_builder = authenticator.authenticate(request_builder).await?;
        }

        if !self.metadata().content_type().is_empty() {
            request_builder = request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        if !self.headers.is_empty() {
            for (key, value) in self.headers.iter() {
                request_builder = request_builder.header(key.as_str(), value.as_str());
            }
        }

        let req = request_builder.build();
        let mut res = client
            .send(req.clone())
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let data = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        self.inner = Box::new(Cursor::new(data));
        debug!(slog_scope::logger(), "Fetch ended");

        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(CurlPaginator::new(self.clone())))
    }
    /// See [`Connector::is_variable_path`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use surf::http::Method;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// assert_eq!(false, connector.is_variable());
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// connector.path = "/get/{{ field }}".to_string();
    /// assert_eq!(true, connector.is_variable());
    /// ```
    fn is_variable(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.path = "/status/200".to_string();
    ///     assert_eq!(true, connector.is_empty().await?);
    ///     connector.path = "/get".to_string();
    ///     assert_eq!(false, connector.is_empty().await?);
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&mut self) -> Result<bool> {
        Ok(0 == self.len().await?)
    }
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.path = "/status/200".to_string();
    ///     assert!(0 == connector.len().await?, "The remote document should have a length equal to zero");
    ///     connector.path = "/get".to_string();
    ///     assert!(0 != connector.len().await?, "The remote document should have a length different than zero");
    ///     Ok(())
    /// }
    /// ```
    async fn len(&mut self) -> Result<usize> {
        let client = surf::client();
        let url = Url::parse(format!("{}{}", self.endpoint, self.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let mut request_builder = surf::head(url);

        if let Some(ref mut authenticator_type) = self.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            authenticator.set_parameters(self.parameters.clone());
            request_builder = authenticator.authenticate(request_builder).await?;
        }

        if !self.metadata().content_type().is_empty() {
            request_builder = request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        if !self.headers.is_empty() {
            for (key, value) in self.headers.iter() {
                request_builder = request_builder.header(key.as_str(), value.as_str());
            }
        }

        let req = request_builder.build();
        
        let res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        if !res.status().is_success() {
            warn!(slog_scope::logger(), "Can't get the len of the remote document with method HEAD"; "connector" => format!("{:?}", self), "status" => res.status().to_string());

            return Ok(0);
        }

        let header_value = res
            .header(headers::CONTENT_LENGTH)
            .map(|ct_len| ct_len.as_str())
            .unwrap_or("0");

        let content_length = header_value
            .parse::<usize>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        Ok(content_length)
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use json_value_search::Search;
    /// use serde_json::Value;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Post;
    ///     connector.path = "/post".to_string();
    ///     
    ///     connector.write(r#"[{"column1":"value1"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let payload: Value = serde_json::from_str(std::str::from_utf8(connector.inner()).unwrap())?;
    ///     assert_eq!(r#"[{"column1":"value1"}]"#, payload.search("/data")?.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn send(&mut self, _position: Option<isize>) -> Result<()> {
        let client = surf::client();
        // initialize the position of the cursor
        self.inner.set_position(0);

        let url = Url::parse(format!("{}{}", self.endpoint, self.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let mut request_builder = surf::RequestBuilder::new(self.method, url);

        if let Some(ref mut authenticator_type) = self.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            authenticator.set_parameters(self.parameters.clone());
            request_builder = authenticator.authenticate(request_builder).await?;
        }

        if !self.metadata().content_type().is_empty() {
            request_builder = request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        if !self.headers.is_empty() {
            for (key, value) in self.headers.iter() {
                request_builder = request_builder.header(key.as_str(), value.as_str());
            }
        }

        let req = request_builder.body(self.inner.get_ref().to_vec()).build();
        let mut res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let data = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        self.clear();

        if !data.is_empty() {
            self.inner.write_all(&data)?;
            self.inner.set_position(0);
        }

        Ok(())
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.path = "/status/200".to_string();
    ///     connector.erase().await?;
    ///     assert_eq!(true, connector.is_empty().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn erase(&mut self) -> Result<()> {
        let client = surf::client();
        let url = Url::parse(format!("{}{}", self.endpoint, self.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let mut request_builder = surf::RequestBuilder::new(Method::Delete, url);

        if let Some(ref mut authenticator_type) = self.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            authenticator.set_parameters(self.parameters.clone());
            request_builder = authenticator.authenticate(request_builder).await?;
        }

        if !self.metadata().content_type().is_empty() {
            request_builder = request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        if !self.headers.is_empty() {
            for (key, value) in self.headers.iter() {
                request_builder = request_builder.header(key.as_str(), value.as_str());
            }
        }

        let req = request_builder.build();
        let mut res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    res.body_string()
                        .await
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        Ok(())
    }
    /// See [`Writer::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for Curl {
    /// See [`async_std::io::Read::poll_read`] for more details.
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Read::read(&mut self.inner, buf))
    }
}

#[async_trait]
impl async_std::io::Write for Curl {
    /// See [`async_std::io::Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`async_std::io::Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`async_std::io::Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct CurlPaginator {
    connector: Curl,
    skip: usize,
    has_next: bool,
}

impl CurlPaginator {
    pub fn new(connector: Curl) -> Self {
        CurlPaginator {
            connector: connector.clone(),
            skip: connector.skip,
            has_next: true,
        }
    }
}

#[async_trait]
impl Paginator for CurlPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example: Paginate through the remove document.
    /// ```rust
    /// use chewdata::connector::{curl::Curl, curl::PaginatorParameters, Connector};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/links/{{n}}/10".to_string();
    ///     connector.limit = 1;
    ///     connector.skip = 1;
    ///     let paginator_parameters = PaginatorParameters { skip_name: "n".to_string(), limit_name: "limit".to_string() };
    ///     connector.paginator_parameters = Some(paginator_parameters);
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     let mut reader = paginator.next_page().await?.unwrap();
    ///     assert_eq!("/links/1/10", reader.path().as_str());
    ///     let mut buffer1 = String::default();
    ///     let len1 = reader.read_to_string(&mut buffer1).await?;
    ///     println!("buffer1 {:?}", buffer1);
    ///     assert!(0 < len1, "Can't read the content of the file.");
    ///
    ///     let mut reader = paginator.next_page().await?.unwrap();
    ///     assert_eq!("/links/2/10", reader.path().as_str());  
    ///     let mut buffer2 = String::default();
    ///     let len2 = reader.read_to_string(&mut buffer2).await?;
    ///     println!("buffer2 {:?}", buffer2);
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files is not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Paginate one time on a remove document.
    /// ```rust
    /// use chewdata::connector::{curl::Curl, curl::PaginatorParameters, Connector};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/get".to_string();
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     let mut reader = paginator.next_page().await?;     
    ///     assert!(reader.is_some());
    ///
    ///     let mut reader = paginator.next_page().await?;
    ///     assert!(reader.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(
        &mut self
    ) -> Result<Option<Box<dyn Connector>>> {
        Ok(match self.has_next {
            true => {
                let mut new_connector = self.connector.clone();
                let mut new_parameters = Value::default();
                new_parameters.merge(self.connector.parameters.clone());

                if let Some(paginator_parameters) = self.connector.paginator_parameters.clone() {
                    new_parameters.merge(serde_json::from_str(
                        format!(
                            r#"{{"{}":"{}"}}"#,
                            paginator_parameters.limit_name, self.connector.limit
                        )
                        .as_str(),
                    )?);
                    new_parameters.merge(serde_json::from_str(
                        format!(r#"{{"{}":"{}"}}"#, paginator_parameters.skip_name, self.skip).as_str(),
                    )?);
                }

                if self.connector.paginator_parameters.clone().is_none() {
                    self.has_next = false;
                }

                new_connector.set_parameters(new_parameters);
                new_connector.fetch().await?;

                self.skip += self.connector.limit;

                Some(Box::new(new_connector))
            }
            false => None,
        })
    }
}
