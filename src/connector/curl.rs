//! Read and write data through http(s) connector.
//!
//! ### Configuration
//!
//! | key           | alias | Description                                              | Default Value | Possible Values                                                         |
//! | ------------- | ----- | -------------------------------------------------------- | ------------- | ----------------------------------------------------------------------- |
//! | type          | -     | Required in order to use this connector.                  | `curl`        | `curl`                                                                 |
//! | metadata      | meta  | Override metadata information.                            | `null`        | [`crate::Metadata`]                                                    |
//! | authenticator | auth  | Define the authentification that secure the http(s) call. | `null`        | [`crate::connector::authenticator::basic::Basic`] / [`crate::connector::authenticator::bearer::Bearer`] / [`crate::connector::authenticator::jwt::Jwt`] |
//! | endpoint      | -     | The http endpoint of the url like <http://my_site.com:80>.| `null`        | String                                                                 |
//! | path          | uri   | The path of the resource.                                 | `null`        | String                                                                 |
//! | method        | -     | The http method to use.                                   | `get`         | [HTTP methods](https://developer.mozilla.org/fr/docs/Web/HTTP/Methods) |
//! | headers       | -     | The http headers to override.                             | `null`        | List of key/value                                                      |
//! | timeout       | -     | Time in secound before to abort the call.                 | `5`           | Unsigned number                                                        |
//! | keepalive     | -     | Enable the TCP keepalive.                                 | `true`        | `true` / `false`                                                       |
//! | tcp_nodelay   | -     | Enable the TCP nodelay.                                   | `false`       | `true` / `false`                                                       |
//! | parameters    | -     | Parameters used in the `path` that can be override.       | `null`        | Object or Array of objects                                             |
//! | paginator_type | paginator | Paginator parameters.                                | [`crate::connector::paginator::curl::offset::Offset`]      | [`crate::connector::paginator::curl::offset::Offset`] / [`crate::connector::paginator::curl::cursor::Cursor`]        |
//! | counter_type  | count / counter | Use to find the total of elements in the resource.  | `null` | [`crate::connector::counter::curl::header::Header`] / [`crate::connector::counter::curl::body::Body`]                |
//! | redirection_limit    | - | Limit of redirection |    `5`    | Integer |
//! | version    | - | HTTP version|    `1`    | Integer |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "connector":{
//!             "type": "curl",
//!             "endpoint": "{{ CURL_ENDPOINT }}",
//!             "path": "/get?skip={{ paginator.skip }}&limit={{ paginator.limit }}&cache={{ cache }}",
//!             "method": "get",
//!             "authenticator": {
//!                 "type": "basic",
//!                 "username": "{{ BASIC_USERNAME }}",
//!                 "password": "{{ BASIC_PASSWORD }}",
//!             },
//!             "headers": {
//!                 "Accept": "application/json"
//!             },
//!             "parameters": [
//!                 { "cache": false }
//!             ],
//!             "paginator": {
//!                 "limit": 100,
//!                 "skip": 0
//!             },
//!             "version": "1"
//!         }
//!     }
//! ]
//! ```
//!
use super::authenticator::AuthenticatorType;
use super::counter::curl::CounterType;
use super::paginator::curl::PaginatorType;
use super::Connector;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{DataResult, DataSet, DataStream, Metadata};
use async_native_tls::TlsStream;
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncRead as AsyncReadIo;
use futures::AsyncWrite as AsyncWriteIo;
use futures::{AsyncWriteExt, Stream};
use http::{
    header, request::Builder, HeaderName, HeaderValue, Method, Request, StatusCode, Version,
};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest as SendRequestHttp1;
use json_value_merge::Merge;
use json_value_remove::Remove;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::{io, net::TcpStream};
use smol_hyper::rt::FuturesIo;
use smol_timeout::TimeoutExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

const REDIRECT_CODES: &[StatusCode] = &[
    StatusCode::MOVED_PERMANENTLY,
    StatusCode::FOUND,
    StatusCode::SEE_OTHER,
    StatusCode::TEMPORARY_REDIRECT,
    StatusCode::PERMANENT_REDIRECT,
];

const DEFAULT_TIMEOUT: u64 = 5;

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Curl {
    #[serde(skip)]
    document: Option<Box<dyn Document>>,
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "auth")]
    #[serde(rename = "authenticator")]
    pub authenticator_type: Option<Box<AuthenticatorType>>,
    pub endpoint: String,
    pub path: String,
    pub method: String,
    pub headers: Box<HashMap<String, String>>,
    pub timeout: Option<u64>,
    pub keepalive: bool,
    pub tcp_nodelay: bool,
    #[serde(alias = "params")]
    pub parameters: Value,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
    pub redirection_limit: usize,
    pub version: usize,
}

impl fmt::Debug for Curl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Curl")
            .field("document", &self.document)
            .field("metadata", &self.metadata)
            .field("authenticator_type", &self.authenticator_type)
            .field("endpoint", &self.endpoint)
            .field("path", &self.path)
            .field("method", &self.method)
            // Can contain sensitive data
            .field("headers", &self.headers.display_only_for_debugging())
            .field("timeout", &self.timeout)
            .field("keepalive", &self.keepalive)
            .field("tcp_nodelay", &self.tcp_nodelay)
            // Can contain sensitive data
            .field("parameters", &self.parameters.display_only_for_debugging())
            .field("paginator_type", &self.paginator_type)
            .field("counter_type", &self.counter_type)
            .field("redirection_limit", &self.redirection_limit)
            .field("version", &self.version)
            .finish()
    }
}

impl Default for Curl {
    fn default() -> Self {
        Curl {
            document: None,
            metadata: Metadata::default(),
            authenticator_type: None,
            endpoint: "".into(),
            path: "".into(),
            method: "GET".into(),
            headers: Box::<HashMap<String, String>>::default(),
            timeout: Some(DEFAULT_TIMEOUT),
            keepalive: true,
            tcp_nodelay: false,
            parameters: Value::Null,
            paginator_type: PaginatorType::default(),
            counter_type: None,
            redirection_limit: 5,
            version: 1,
        }
    }
}

/// A TCP or TCP+TLS connection.
enum SmolStream {
    /// A plain TCP connection.
    Plain(TcpStream),

    /// A TCP connection secured by TLS.
    Tls(TlsStream<TcpStream>),
}

impl AsyncReadIo for SmolStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            SmolStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            SmolStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWriteIo for SmolStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            SmolStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            SmolStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            SmolStream::Plain(stream) => Pin::new(stream).poll_close(cx),
            SmolStream::Tls(stream) => Pin::new(stream).poll_close(cx),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            SmolStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            SmolStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }
}

impl Curl {
    async fn http1(&mut self) -> std::io::Result<SendRequestHttp1<Pin<Box<Full<Bytes>>>>> {
        let uri = self
            .endpoint
            .parse::<hyper::Uri>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let host = uri.host().unwrap_or("0.0.0.0");

        let (sender, conn) = hyper::client::conn::http1::handshake(FuturesIo::new({
            match uri.scheme_str() {
                Some("http") => {
                    let stream = {
                        let port = uri.port_u16().unwrap_or(80);
                        TcpStream::connect((host, port))
                            .await
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                    };
                    SmolStream::Plain(stream)
                }
                Some("https") => {
                    // In case of HTTPS, establish a secure TLS connection first.
                    let stream = {
                        let port = uri.port_u16().unwrap_or(443);
                        TcpStream::connect((host, port))
                            .await
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                    };
                    let stream = async_native_tls::connect(host, stream)
                        .await
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    SmolStream::Tls(stream)
                }
                _ => return Err(Error::new(ErrorKind::InvalidData, "unsupported scheme")),
            }
        }))
        .await
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        smol::spawn(
            async move {
                if let Err(e) = conn.await {
                    println!("Connection failed: {:?}", e);
                }
            }
            .timeout(Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT))),
        )
        .detach();

        Ok(sender)
    }
    /// Get a new request builder base on what has been setup in the configuration.
    async fn request_builder(&mut self) -> std::io::Result<Builder> {
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let url = format!("{}{}", self.endpoint, path)
            .parse::<hyper::Uri>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut request_builder = Request::builder().uri(&url).method(
            Method::from_bytes(self.method.to_uppercase().as_bytes())
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
        );

        request_builder = match self.version {
            1 => Ok(request_builder
                .header(
                    header::HOST,
                    format!(
                        "{}:{}",
                        url.host().unwrap_or("localhost"),
                        url.port_u16().unwrap_or(80)
                    ),
                )
                .version(Version::HTTP_11)),
            2 => Ok(request_builder
                .header(
                    ":authority",
                    format!(
                        "{}:{}",
                        url.host().unwrap_or("localhost"),
                        url.port_u16().unwrap_or(80)
                    ),
                )
                .version(Version::HTTP_2)),
            3 => Ok(request_builder
                .header(
                    ":authority",
                    format!(
                        "{}:{}",
                        url.host().unwrap_or("localhost"),
                        url.port_u16().unwrap_or(80)
                    ),
                )
                .version(Version::HTTP_3)),
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This http version '{}' is not managed", self.version),
            )),
        }?;

        // Force the content type
        request_builder =
            request_builder.header(header::CONTENT_TYPE, self.metadata().content_type());

        if !self.metadata().content_type().is_empty() {
            request_builder = request_builder.header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(&self.metadata().content_type())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        // Force the headers
        for (key, value) in self.headers.iter() {
            let header_name = key.parse::<HeaderName>().map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Invalid header name '{}': {}", key, e),
                )
            })?;

            let header_value = HeaderValue::from_str(value).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Invalid header value '{}': {}", value, e),
                )
            })?;

            request_builder = request_builder.header(header_name, header_value);
        }

        if let Some(authenticator_type) = self.authenticator_type.clone() {
            let authenticator = authenticator_type.authenticator();

            let (auth_name, auth_value) = authenticator.authenticate().await?;
            request_builder = request_builder.header(
                HeaderName::from_bytes(&auth_name)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(&auth_value)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        Ok(request_builder)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use smol::stream::StreamExt;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = "Get".into();
    ///     connector.path = "/json".to_string();
    ///     connector.set_document(document);
    ///
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero."
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "curl::head")]
    pub async fn head(&mut self) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut request_builder = self.request_builder().await?;
        let mut client = self.http1().await?;

        let mut request = match self.method.to_uppercase().as_str() {
            "POST" | "PUT" | "PATCH" => {
                let mut buffer = Vec::default();
                let mut parameters_without_context = self.parameters_without_context()?;
                parameters_without_context.replace_mustache(self.parameters.clone());

                let dataset = vec![DataResult::Ok(parameters_without_context)];
                let mut document = self.document()?.clone_box();
                document.set_entry_path(String::default());
                buffer.write_all(&document.header(&dataset)?).await?;
                buffer.write_all(&document.write(&dataset)?).await?;
                buffer.write_all(&document.footer(&dataset)?).await?;

                if let Some(mime_subtype) = &document.metadata().mime_subtype {
                    if mime_subtype == "x-www-form-urlencoded" {
                        if buffer.starts_with(b"\"") {
                            buffer = buffer.drain(1..).collect();
                        }
                        if buffer.ends_with(b"\"") {
                            buffer.pop();
                        }
                    }
                }

                request_builder =
                    request_builder.header(header::CONTENT_LENGTH, buffer.len().to_string());

                let boxed_body = Box::pin(Full::new(Bytes::from(buffer.clone())));
                request_builder.body(boxed_body)
            }
            _ => {
                let boxed_body = Box::pin(Full::new(Bytes::new()));
                request_builder.body(boxed_body)
            }
        }
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut headers = Vec::default();
        let mut redirect_count: u8 = 0;

        while redirect_count <= self.redirection_limit as u8 {
            let res = client
                .send_request(request.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            if REDIRECT_CODES.contains(&res.status()) {
                if let Some(location) = &res.headers().get("location") {
                    match location.to_str().unwrap().parse::<hyper::Uri>() {
                        Ok(valid_url) => {
                            *request.uri_mut() = valid_url;
                        }
                        Err(e) => return Err(Error::new(ErrorKind::InvalidData, e)),
                    };
                    redirect_count += 1;
                    continue;
                }
            }

            if !res.status().is_success() {
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    format!(
                        "The http call on '{}' failed with status code '{}'",
                        request.uri().path_and_query().unwrap().as_str(),
                        res.status(),
                    ),
                ));
            }

            headers = res
                .headers()
                .iter()
                .map(|(key, value)| (key.to_string().clone(), value.as_bytes().to_vec()))
                .collect();

            break;
        }

        if redirect_count > self.redirection_limit as u8 {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "The number of HTTP redirections exceeds the maximum limit of '{}' calls",
                    self.redirection_limit
                ),
            ));
        }

        info!(url = self.path(), "Fetch data with success");

        Ok(headers)
    }
    /// Return parameter's values without context.
    fn parameters_without_context(&self) -> Result<Value> {
        let mut parameters_without_context = self.parameters.clone();
        parameters_without_context.remove("/steps")?;
        parameters_without_context.remove("/paginator")?;
        Ok(parameters_without_context)
    }
}

#[async_trait]
impl Connector for Curl {
    /// See [`Connector::set_document`] for more details.
    fn set_document(&mut self, document: Box<dyn Document>) -> Result<()> {
        self.document = Some(document.clone());

        Ok(())
    }
    /// See [`Connector::document`] for more details.
    fn document(&self) -> Result<&Box<dyn Document>> {
        match &self.document {
            Some(document) => Ok(document),
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "The document has not been set in the connector",
            )),
        }
    }
    /// See [`Connector::path`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
        let mut path = self.path.clone();

        match self.is_variable() {
            true => {
                let mut params = self.parameters.clone();
                let mut metadata = Map::default();

                metadata.insert("metadata".to_string(), self.metadata().into());
                params.merge(&Value::Object(metadata));

                path.replace_mustache(params.clone());
                path
            }
            false => path,
        }
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
            trace!("Stay link to the same resource");
            return Ok(false);
        }

        let mut metadata_kv = Map::default();
        metadata_kv.insert("metadata".to_string(), self.metadata().into());
        let metadata = Value::Object(metadata_kv);

        let mut new_parameters = new_parameters;
        new_parameters.merge(&metadata);
        let mut old_parameters = self.parameters.clone();
        old_parameters.merge(&metadata);

        let mut previous_path = self.path.clone();
        previous_path.replace_mustache(old_parameters);

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if previous_path == new_path {
            trace!(path = previous_path, "The path has not changed");
            return Ok(false);
        }

        info!(
            previous_path = previous_path,
            new_path = new_path,
            "Will use another resource based the new parameters"
        );
        Ok(true)
    }
    /// See [`Connector::is_variable`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
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
        self.path.has_mustache()
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::counter::curl::CounterType;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.path = "/status/200".to_string();
    ///     connector.counter_type = Some(CounterType::default());
    ///     assert!(0 == connector.len().await?, "The remote document should have a length equal to zero.");
    ///     connector.path = "/get".to_string();
    ///     assert!(0 != connector.len().await?, "The remote document should have a length different than zero.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "curl::len")]
    async fn len(&self) -> Result<usize> {
        let counter_type = match &self.counter_type {
            Some(counter_type) => counter_type,
            None => return Ok(0),
        };

        match counter_type.count(self).await {
            Ok(Some(count)) => Ok(count),
            Ok(None) => Ok(0),
            Err(e) => {
                warn!(
                    error = e.to_string(),
                    "Can't count the number of element, return 0"
                );

                Ok(0)
            }
        }
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use smol::stream::StreamExt;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = "Get".into();
    ///     connector.path = "/json".to_string();
    ///     connector.set_document(document);
    ///
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero."
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "curl::fetch")]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let mut request_builder = self.request_builder().await?;
        let mut client = self.http1().await?;

        let mut request = match self.method.to_uppercase().as_str() {
            "POST" => {
                let mut buffer = Vec::default();
                let mut parameters_without_context = self.parameters_without_context()?;
                parameters_without_context.replace_mustache(self.parameters.clone());

                let dataset = vec![DataResult::Ok(parameters_without_context)];
                let mut document = self.document()?.clone_box();
                document.set_entry_path(String::default());
                buffer.write_all(&document.header(&dataset)?).await?;
                buffer.write_all(&document.write(&dataset)?).await?;
                buffer.write_all(&document.footer(&dataset)?).await?;

                if let Some(mime_subtype) = &document.metadata().mime_subtype {
                    if mime_subtype == "x-www-form-urlencoded" {
                        if buffer.starts_with(b"\"") {
                            buffer = buffer.drain(1..).collect();
                        }
                        if buffer.ends_with(b"\"") {
                            buffer.pop();
                        }
                    }
                }

                request_builder = request_builder.header(header::CONTENT_LENGTH, buffer.len());

                request_builder.body(Box::pin(Full::new(buffer.into())))
            }
            _ => {
                let boxed_body = Box::pin(Full::new(Bytes::new()));
                request_builder.body(boxed_body)
            }
        }
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut data = Vec::default();
        let mut redirect_count: u8 = 0;

        while redirect_count <= self.redirection_limit as u8 {
            let res = client
                .send_request(request.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            if REDIRECT_CODES.contains(&res.status()) {
                if let Some(location) = res.headers().get("location") {
                    match location.to_str().unwrap().parse::<hyper::Uri>() {
                        Ok(valid_url) => {
                            *request.uri_mut() = valid_url;
                        }
                        Err(e) => return Err(Error::new(ErrorKind::InvalidData, e)),
                    };
                    redirect_count += 1;
                    continue;
                }
            }

            if !res.status().is_success() {
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    format!(
                        "The http call on '{}' failed with status code '{}'",
                        request.uri().path_and_query().unwrap().as_str(),
                        res.status(),
                    ),
                ));
            }

            data = res
                .collect()
                .await
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                .to_bytes()
                .to_vec();

            break;
        }

        if redirect_count > self.redirection_limit as u8 {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "The number of HTTP redirections exceeds the maximum limit of '{}' calls",
                    self.redirection_limit
                ),
            ));
        }

        info!(path = self.path(), "Fetch data with success");

        let document = self.document()?;

        if !document.has_data(&data)? {
            return Ok(None);
        }

        let dataset = document.read(&data)?;

        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use json_value_search::Search;
    /// use serde_json::Value;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = "Post".into();
    ///     connector.path = "/post".to_string();
    ///     connector.set_document(document);
    ///
    ///     let expected_result1 =
    ///        DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1];
    ///     let mut datastream = connector.send(&dataset).await.unwrap().unwrap();
    ///     let value = datastream.next().await.unwrap().to_value();
    ///     assert_eq!(
    ///        r#"[{"column1":"value1"}]"#,
    ///        value.search("/data").unwrap().unwrap()
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "curl::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let mut request_builder = self.request_builder().await?;
        let mut client = self.http1().await?;

        let mut request = match self.method.to_uppercase().as_str() {
            "POST" | "PUT" | "PATCH" => {
                let mut buffer = Vec::default();
                let mut document = self.document()?.clone_box();

                document.set_entry_path(String::default());
                buffer.write_all(&document.header(&dataset)?).await?;
                buffer.write_all(&document.write(&dataset)?).await?;
                buffer.write_all(&document.footer(&dataset)?).await?;

                if let Some(mime_subtype) = &document.metadata().mime_subtype {
                    if mime_subtype == "x-www-form-urlencoded" {
                        if buffer.starts_with(b"\"") {
                            buffer = buffer.drain(1..).collect();
                        }
                        if buffer.ends_with(b"\"") {
                            buffer.pop();
                        }
                    }
                }

                request_builder =
                    request_builder.header(header::CONTENT_LENGTH, buffer.len().to_string());

                let boxed_body = Box::pin(Full::new(Bytes::from(buffer.clone())));
                request_builder.body(boxed_body)
            }
            _ => {
                let boxed_body = Box::pin(Full::new(Bytes::new()));
                request_builder.body(boxed_body)
            }
        }
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut data = Vec::default();
        let mut redirect_count: u8 = 0;

        while redirect_count <= self.redirection_limit as u8 {
            let res = client
                .send_request(request.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            if REDIRECT_CODES.contains(&res.status()) {
                if let Some(location) = res.headers().get("location") {
                    match location.to_str().unwrap().parse::<hyper::Uri>() {
                        Ok(valid_url) => {
                            *request.uri_mut() = valid_url;
                        }
                        Err(e) => return Err(Error::new(ErrorKind::InvalidData, e)),
                    };
                    redirect_count += 1;
                    continue;
                }
            }

            if !res.status().is_success() {
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    format!(
                        "The http call on '{}' failed with status code '{}'",
                        request.uri().path_and_query().unwrap().as_str(),
                        res.status(),
                    ),
                ));
            }

            data = res
                .collect()
                .await
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                .to_bytes()
                .to_vec();

            break;
        }

        if redirect_count > self.redirection_limit as u8 {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "The number of HTTP redirections exceeds the maximum limit of '{}' calls",
                    self.redirection_limit
                ),
            ));
        }

        info!(path = self.path(), "Fetch data with success");

        let document = self.document()?;
        if !document.has_data(&data)? {
            return Ok(None);
        }

        let dataset = document.read(&data)?;

        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
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
    #[instrument(name = "curl::erase")]
    async fn erase(&mut self) -> Result<()> {
        let mut request_builder = self.request_builder().await?;
        let mut client = self.http1().await?;

        request_builder = request_builder.method(hyper::Method::DELETE);

        let mut request = request_builder
            .body(Box::pin(Full::new(Bytes::new())))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut redirect_count: u8 = 0;

        while redirect_count <= self.redirection_limit as u8 {
            let res = client
                .send_request(request.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            if REDIRECT_CODES.contains(&res.status()) {
                if let Some(location) = res.headers().get("location") {
                    match location.to_str().unwrap().parse::<hyper::Uri>() {
                        Ok(valid_url) => {
                            *request.uri_mut() = valid_url;
                        }
                        Err(e) => return Err(Error::new(ErrorKind::InvalidData, e)),
                    };
                    redirect_count += 1;
                    continue;
                }
            }

            if !res.status().is_success() {
                return Err(Error::new(
                    ErrorKind::Interrupted,
                    format!(
                        "The http call on '{}' failed with status code '{}'",
                        request.uri().path_and_query().unwrap().as_str(),
                        res.status()
                    ),
                ));
            }

            break;
        }

        if redirect_count > self.redirection_limit as u8 {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "The number of HTTP redirections exceeds the maximum limit of '{}' calls",
                    self.redirection_limit
                ),
            ));
        }

        info!(path = self.path(), "Erase data with success");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        self.paginator_type.paginate(self).await
    }
}

#[derive(Debug, Default)]
pub struct Logger {
    _priv: (),
}

impl Logger {
    pub fn new() -> Self {
        Logger { _priv: () }
    }
}

// #[async_trait::async_trait]
// impl Middleware for Logger {
//     async fn handle(
//         &self,
//         req: Request,
//         client: Client,
//         next: Next<'_>,
//     ) -> std::result::Result<Response, http_types::Error> {
//         let start_time = time::Instant::now();
//         let uri = format!("{}", req.url());
//         let method = format!("{}", req.method());
//         let id = COUNTER.fetch_add(1, Ordering::Relaxed);

//         trace!(id, uri, method, "sending request");

//         let res = next.run(req, client).await?;
//         let status = res.status();
//         let elapsed = start_time.elapsed();

//         trace!(
//             id,
//             uri,
//             method,
//             elapsed = &format!("{:?}", elapsed),
//             status = status.to_string().as_str(),
//             "request completed"
//         );

//         Ok(res)
//     }
// }

// #[derive(Debug)]
// pub struct Authenticator {
//     authenticator_type: Box<AuthenticatorType>,
// }

// impl Authenticator {
//     pub fn new(authenticator_type: Box<AuthenticatorType>) -> Self {
//         Authenticator { authenticator_type }
//     }
// }

// #[async_trait::async_trait]
// impl Middleware for Authenticator {
//     async fn handle(
//         &self,
//         req: Request,
//         client: Client,
//         next: Next<'_>,
//     ) -> std::result::Result<Response, http_types::Error> {
//         let authenticator = self.authenticator_type.authenticator();

//         let (auth_name, auth_value) = authenticator.authenticate().await?;
//         let mut req = req.clone();
//         req.set_header(
//             HeaderName::from_bytes(auth_name).map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
//             HeaderValue::from_bytes(auth_value)
//                 .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
//         );

//         let res = next.run(req, client).await?;

//         Ok(res)
//     }
// }

#[cfg(test)]
mod tests {
    use json_value_search::Search;

    use super::*;
    use crate::connector::authenticator::{basic::Basic, bearer::Bearer, AuthenticatorType};
    use crate::connector::counter::curl::CounterType;
    use crate::document::json::Json;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

    #[test]
    fn is_variable() {
        let mut connector = Curl::default();
        assert_eq!(false, connector.is_variable());
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        connector.path = "/get/{{ field }}".to_string();
        assert_eq!(true, connector.is_variable());
    }
    #[test]
    fn is_resource_will_change() {
        let mut connector = Curl::default();
        let params = serde_json::from_str(r#"{"field":"test"}"#).unwrap();
        assert_eq!(
            false,
            connector.is_resource_will_change(Value::Null).unwrap()
        );
        connector.path = "/dir/static.ext".to_string();
        assert_eq!(
            false,
            connector.is_resource_will_change(Value::Null).unwrap()
        );
        connector.path = "/dir/dynamic_{{ field }}.ext".to_string();
        assert_eq!(true, connector.is_resource_will_change(params).unwrap());
    }
    #[test]
    fn path() {
        let mut connector = Curl::default();
        connector.path = "/resource/{{ field }}".to_string();
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        assert_eq!("/resource/value", connector.path());
    }
    #[apply(test!)]
    async fn len() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
        connector.counter_type = Some(CounterType::default());
        assert!(
            0 == connector.len().await.unwrap(),
            "The remote document should have a length equal to zero."
        );
        connector.path = "/get".to_string();
        assert!(
            0 != connector.len().await.unwrap(),
            "The remote document should have a length different than zero."
        );
    }
    #[apply(test!)]
    async fn is_empty() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
        connector.counter_type = Some(CounterType::default());
        assert_eq!(true, connector.is_empty().await.unwrap());
        connector.path = "/get".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn fetch() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/json".to_string();
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn fetch_with_basic() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/basic-auth/my-username/my-password".to_string();
        connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
            "my-username",
            "my-password",
        ))));
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn fetch_with_bearer() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/bearer".to_string();
        connector.authenticator_type =
            Some(Box::new(AuthenticatorType::Bearer(Bearer::new("abcd1234"))));
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn send() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "POST".into();
        connector.path = "/post".to_string();
        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        connector.set_document(Box::new(document)).unwrap();

        let dataset = vec![expected_result1];
        let mut datastream = connector.send(&dataset).await.unwrap().unwrap();
        let value = datastream.next().await.unwrap().to_value();
        assert_eq!(
            r#"[{"column1":"value1"}]"#,
            value.search("/data").unwrap().unwrap()
        );
    }
    #[apply(test!)]
    async fn erase() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
        connector.erase().await.unwrap();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn test_redirection_with_fetch() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/redirect/1".to_string();
        connector.redirection_limit = 1;
        connector.set_document(Box::new(document)).unwrap();

        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );

        connector.path = "/redirect/2".to_string();
        connector.redirection_limit = 1;

        let result = connector.fetch().await;
        assert!(
            result.is_err(),
            "The inner connector should raise an error."
        );
    }
    #[apply(test!)]
    async fn test_redirection_with_send() {
        let document = Json::default();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1];

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/redirect/1".to_string();
        connector.redirection_limit = 1;
        connector.set_document(Box::new(document)).unwrap();

        let datastream = connector.send(&dataset).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );

        connector.path = "/redirect/2".to_string();
        connector.redirection_limit = 1;

        let result = connector.send(&dataset).await;
        assert!(
            result.is_err(),
            "The inner connector should raise an error."
        );
    }
    // httpbin return 500 code error.
    // #[apply(test!)]
    // async fn test_redirection_with_erase() {
    //     let mut connector = Curl::default();
    //     connector.endpoint = "http://localhost:8080".to_string();
    //     connector.path = "/redirect/1".to_string();
    //     connector.redirection_limit = 1;
    //
    //     let result = connector.erase().await;
    //     assert!(
    //         result.is_ok(),
    //         "The inner connector shouldn't raise an error."
    //     );
    //
    //     connector.path = "/redirect/2".to_string();
    //     connector.redirection_limit = 1;
    //
    //     let result = connector.erase().await;
    //     assert!(
    //         result.is_err(),
    //         "The inner connector should raise an error."
    //     );
    // }
}
