use super::authenticator::AuthenticatorType;
use super::{Connector, Paginator};
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
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
    // The path of the resource
    pub path: String,
    // The http method.
    pub method: Method,
    // Add complementaries headers. This headers override the default headers.
    pub headers: Box<HashMap<String, String>>,
    #[serde(alias = "params")]
    pub parameters: Value,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
    #[serde(skip)]
    pub inner: Box<Cursor<Vec<u8>>>,
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
            paginator_type: PaginatorType::default(),
            counter_type: None,
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
            .field("paginator_type", &self.paginator_type)
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
            }
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
            trace!("The connector stay link to the same resource");
            return Ok(false);
        }

        let mut actuel_path = self.path.clone();
        actuel_path.replace_mustache(self.parameters.clone());

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if actuel_path == new_path {
            trace!("The connector stay link to the same resource");
            return Ok(false);
        }

        info!("The connector will use another resource, regarding the new parameters");
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
    #[instrument]
    async fn fetch(&mut self) -> Result<()> {
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
            request_builder =
                request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
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

        info!("The connector fetch data into the resource with success");
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        let paginator = match self.paginator_type {
            PaginatorType::Offset(ref offset_paginator) => {
                let mut offset_paginator = offset_paginator.clone();
                offset_paginator.set_connector(self.clone());

                Box::new(offset_paginator) as Box<dyn Paginator + Send>
            }
            PaginatorType::Cursor(ref cursor_paginator) => {
                let mut cursor_paginator = cursor_paginator.clone();
                cursor_paginator.set_connector(self.clone());

                Box::new(cursor_paginator) as Box<dyn Paginator + Send>
            }
        };

        Ok(Pin::new(paginator))
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
    #[instrument]
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
            request_builder =
                request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
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
            trace!(
                connector = format!("{:?}", self).as_str(),
                status = res.status().to_string().as_str(),
                "Can't get the len of the remote document with method HEAD"
            );

            return Ok(0);
        }

        let header_value = res
            .header(headers::CONTENT_LENGTH)
            .map(|ct_len| ct_len.as_str())
            .unwrap_or("0");

        let content_length = header_value
            .parse::<usize>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        info!(len = content_length, "The connector found data in the resource");
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
    #[instrument]
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
            request_builder =
                request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
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

        info!("The connector send data into the resource with success");
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
    #[instrument]
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
            request_builder =
                request_builder.header(headers::CONTENT_TYPE, self.metadata().content_type());
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

        info!("The connector erase data in the resource with success");
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "header")]
    Header(HeaderCounter),
    #[serde(rename = "body")]
    Body(BodyCounter),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Header(HeaderCounter::default())
    }
}

impl CounterType {
    pub async fn count(
        &self,
        connector: Curl,
        document: Option<Box<dyn Document>>,
    ) -> Result<Option<usize>> {
        match self {
            CounterType::Header(header_counter) => header_counter.count(connector).await,
            CounterType::Body(body_counter) => {
                let document = match document {
                    Some(document) => Ok(document),
                    None => Err(Error::new(
                        ErrorKind::InvalidInput,
                        "The counter type Body need a document type to work",
                    )),
                }?;
                body_counter.count(connector, document).await
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HeaderCounter {
    // Header Name
    pub name: String,
    // path of the count resource
    pub path: Option<String>,
}

impl Default for HeaderCounter {
    fn default() -> Self {
        HeaderCounter {
            name: "X-Total-Count".to_string(),
            path: None,
        }
    }
}

impl HeaderCounter {
    pub fn new(name: String, path: Option<String>) -> Self {
        HeaderCounter { name, path }
    }
    /// Get the number of items from the header
    ///
    /// # Example: Get the number
    /// ```rust
    /// use chewdata::connector::curl::{Curl, HeaderCounter};
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
    ///
    ///     let mut counter = HeaderCounter::default();
    ///     counter.name = "Content-Length".to_string();
    ///     assert_eq!(Some(194), counter.count(connector).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Not get the number
    /// ```rust
    /// use chewdata::connector::curl::{Curl, HeaderCounter};
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
    ///
    ///     let mut counter = HeaderCounter::default();
    ///     counter.name = "not_found".to_string();
    ///     assert_eq!(None, counter.count(connector).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    pub async fn count(&self, connector: Curl) -> Result<Option<usize>> {
        let client = surf::client();
        let mut connector = connector.clone();

        if let Some(path) = self.path.clone() {
            connector.path = path;
        }

        let url = Url::parse(format!("{}{}", connector.endpoint, connector.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let mut request_builder = surf::head(url);

        if let Some(ref mut authenticator_type) = connector.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            authenticator.set_parameters(connector.parameters.clone());
            request_builder = authenticator.authenticate(request_builder).await?;
        }

        if !connector.metadata().content_type().is_empty() {
            request_builder =
                request_builder.header(headers::CONTENT_TYPE, connector.metadata().content_type());
        }

        if !connector.headers.is_empty() {
            for (key, value) in connector.headers.iter() {
                request_builder = request_builder.header(key.as_str(), value.as_str());
            }
        }

        let req = request_builder.build();

        let res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        if !res.status().is_success() {
            warn!(
                status = res.status().to_string().as_str(),
                "Can't get the number of elements into the remote document with the method HEAD"
            );

            return Ok(None);
        }

        let header_value = res
            .header(self.name.as_str())
            .map(|value| value.as_str())
            .unwrap_or("0");

        if header_value == "0" {
            return Ok(None);
        }

        Ok(match header_value.to_string().parse::<usize>() {
            Ok(count) => {
                trace!(size = count, "The counter count elements in the resource with success");
                Some(count)
            },
            Err(_) => {
                trace!("The counter can't count elements in the resource");
                None
            },
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BodyCounter {
    // The entry path to catch the value in the body
    pub entry_path: String,
    // Path of the count resource
    pub path: Option<String>,
}

impl Default for BodyCounter {
    fn default() -> Self {
        BodyCounter {
            entry_path: "/count".to_string(),
            path: None,
        }
    }
}

impl BodyCounter {
    pub fn new(entry_path: String, path: Option<String>) -> Self {
        BodyCounter { entry_path, path }
    }
    /// Get the number of items from the response body
    ///
    /// # Example: Get the number
    /// ```rust
    /// use chewdata::connector::curl::{Curl, BodyCounter};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Post;
    ///     connector.path = "/anything?count=10".to_string();
    ///
    ///     let mut counter = BodyCounter::default();
    ///     counter.entry_path = "/args/count".to_string();
    ///     assert_eq!(Some(10), counter.count(connector, Box::new(Json::default())).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Don't find the count in the body
    /// ```rust
    /// use chewdata::connector::curl::{Curl, BodyCounter};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Post;
    ///     connector.path = "/anything?count=10".to_string();
    ///
    ///     let mut counter = BodyCounter::default();
    ///     counter.entry_path = "/args/test".to_string();
    ///     assert_eq!(None, counter.count(connector, Box::new(Json::default())).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    pub async fn count(
        &self,
        connector: Curl,
        document: Box<dyn Document>,
    ) -> Result<Option<usize>> {
        let mut connector = connector.clone();
        let mut document = document.clone();

        if let Some(path) = self.path.clone() {
            connector.path = path;
        }

        document.set_entry_path(self.entry_path.clone());

        connector.fetch().await?;

        let mut dataset = document
            .read_data(&mut (Box::new(connector) as Box<dyn Connector>))
            .await?;

        let data_opt = dataset.next().await;

        let value = match data_opt {
            Some(data) => data.to_value(),
            None => Value::Null,
        };

        let count = match value {
            Value::Number(_) => value.as_u64().map(|number| number as usize),
            Value::String(_) => match value.as_str() {
                Some(value) => match value.parse::<usize>() {
                    Ok(number) => Some(number),
                    Err(_) => None,
                },
                None => None,
            },
            _ => None,
        };

        trace!(size = count, "The counter count elements in the resource with success");
        Ok(count)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "offset")]
    Offset(OffsetPaginator),
    #[serde(rename = "cursor")]
    Cursor(CursorPaginator),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Offset(OffsetPaginator::default())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct OffsetPaginator {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Curl>>,
}

impl Default for OffsetPaginator {
    fn default() -> Self {
        OffsetPaginator {
            limit: 100,
            skip: 0,
            count: None,
            connector: None,
        }
    }
}

impl OffsetPaginator {
    fn set_connector(&mut self, connector: Curl) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for OffsetPaginator {
    /// See [`Paginator::count`] for more details.
    ///
    /// # Example: Paginate indefinitely with the offset paginator
    /// ```rust
    /// use chewdata::connector::{curl::{Curl, PaginatorType, OffsetPaginator, CounterType, HeaderCounter}, Connector};
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
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator::default());
    ///     connector.counter_type = Some(CounterType::Header(HeaderCounter::new("Content-Length".to_string(), None)));
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert_eq!(Some(194), paginator.count().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn count(&mut self) -> Result<Option<usize>> {
        let connector = match self.connector {
            Some(ref mut connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't count the number of element in the resource without a connector",
            )),
        }?;
        
        if let Some(counter_type) = connector.counter_type.clone() {
            self.count = counter_type.count(*connector.clone(), None).await?;

            info!(size = self.count, "The connector's counter count elements in the resource with success");
            return Ok(self.count);
        }

        trace!(size = self.count, "The connector's counter not exist or can't count the number of elements in the resource");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example: Paginate indefinitely with the offset paginator
    /// ```rust
    /// use chewdata::connector::{curl::{Curl, PaginatorType, OffsetPaginator}, Connector};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/links/{{ paginator.skip }}/10".to_string();
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
    ///         skip: 1,
    ///         limit: 1,
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;  
    ///     assert_eq!("/links/1/10", connector.path().as_str());
    ///     let mut buffer1 = String::default();
    ///     let len1 = connector.read_to_string(&mut buffer1).await?;
    ///     assert!(0 < len1, "Can't read the content of the file.");
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;  
    ///     assert_eq!("/links/2/10", connector.path().as_str());  
    ///     let mut buffer2 = String::default();
    ///     let len2 = connector.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files is not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Paginate one time with the offset paginator
    /// ```rust
    /// use chewdata::connector::{curl::Curl, Connector};
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
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Paginate three times with the offset paginator and the paginator can return multi connectors in parallel
    /// ```rust
    /// use chewdata::connector::{curl::{Curl, PaginatorType, OffsetPaginator}, Connector};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/links/{{ paginator.skip }}/10".to_string();
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
    ///         skip: 0,
    ///         limit: 1,
    ///         count: Some(3),
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;
        let count_opt = match self.count {
            Some(count) => Some(count),
            None => self.count().await?,
        };

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();
                new_parameters.merge_in("/paginator/limit", Value::String(limit.to_string()))?;
                new_parameters.merge_in("/paginator/skip", Value::String(skip.to_string()))?;

                new_connector.set_parameters(new_parameters);

                if let Some(count) = count_opt {
                    if count <= limit + skip {
                        has_next = false;
                    }
                }

                if connector.path() == new_connector.path() {
                    has_next = false;
                }

                skip += limit;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return the last new connector");
                yield Ok(new_connector as Box<dyn Connector>);
            }
            trace!("The stream stop to return new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&mut self) -> bool {
        self.count.is_some()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct CursorPaginator {
    pub limit: usize,
    // The entry path to catch the value in the body
    pub entry_path: String,
    #[serde(skip)]
    pub document: Option<Box<dyn Document>>,
    #[serde(skip)]
    pub connector: Option<Box<Curl>>,
    #[serde(skip)]
    pub next_token: Option<String>,
}

impl Default for CursorPaginator {
    fn default() -> Self {
        CursorPaginator {
            limit: 100,
            connector: None,
            document: None,
            next_token: None,
            entry_path: "/next".to_string(),
        }
    }
}

impl CursorPaginator {
    fn set_connector(&mut self, connector: Curl) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for CursorPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// See [`Paginator::set_document`] for more details.
    fn set_document(&mut self, document: Box<dyn Document>) {
        self.document = Some(document);
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example: Paginate to the next cursor
    /// ```rust
    /// use chewdata::connector::{curl::{Curl, PaginatorType, CursorPaginator}, Connector};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/uuid?next={{ paginator.next_token }}".to_string();
    ///     connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
    ///         limit: 1,
    ///         entry_path: "/uuid".to_string(),
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     paginator.set_document(Box::new(Json::default()));
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut document = match self.document.clone() {
            Some(document) => Ok(document),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a document",
            )),
        }?;

        let mut has_next = true;
        let limit = self.limit;
        let entry_path = self.entry_path.clone();
        let mut next_token_opt = self.next_token.clone();

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();

                if let Some(next_token) = next_token_opt {
                    new_parameters.merge_in("/paginator/next_token", Value::String(next_token))?;
                }

                new_parameters
                    .merge_in("/paginator/limit", Value::String(limit.to_string()))?;

                document.set_entry_path(entry_path.clone());
                new_connector.fetch().await?;

                let mut dataset = document
                    .read_data(&mut (new_connector.clone() as Box<dyn Connector>))
                    .await?;

                let data_opt = dataset.next().await;

                let value = match data_opt {
                    Some(data) => data.to_value(),
                    None => Value::Null,
                };

                next_token_opt = match value {
                    Value::Number(_) => Some(value.to_string()),
                    Value::String(string) => Some(string),
                    _ => None,
                };

                if next_token_opt.is_none() {
                    has_next = false;
                }

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                yield Ok(new_connector.clone() as Box<dyn Connector>);
            }
            trace!("The stream stop to return a new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&mut self) -> bool {
        false
    }
}
