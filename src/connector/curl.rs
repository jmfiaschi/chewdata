//! # HTTP(S) Connector (Curl)
//!
//! This connector executes HTTP requests and maps responses into datasets
//! using a [`Document`] for serialization/deserialization.
//!
//! ## Request lifecycle
//! 1. Build request from `endpoint`, `path`, `method`, headers, and parameters
//! 2. Apply authentication (optional)
//! 3. Send request (with redirection handling)
//! 4. Deserialize response body using the configured `Document`
//! 5. Optionally cache the response
//!
//! ## HTTP method behavior
//!
//! | Method        | Request body | Response body |
//! |---------------|--------------|---------------|
//! | GET / HEAD    | ❌           | ✔️ (HEAD ignored) |
//! | POST / PUT    | ✔️           | ✔️ |
//! | PATCH         | ✔️           | ✔️ |
//! | DELETE        | ❌           | ✔️ (optional) |
//!
//! ## Cache behavior
//!
//! * Cache key: full request URI
//! * Cache policy: HTTP semantics (`Cache-Control`, `Expires`, etc.)
//! * Storage: OS temp directory (`cache/http`)
//! * Cache is bypassed if response is stale
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
//! | parameters    | -     | Parameters used in the `path` that can be override.       | `null`        | Object or Array of objects                                             |
//! | paginator_type | paginator | Paginator parameters.                                | [`crate::connector::paginator::curl::offset::Offset`]      | [`crate::connector::paginator::curl::offset::Offset`] / [`crate::connector::paginator::curl::cursor::Cursor`]        |
//! | counter_type  | count / counter | Use to find the total of elements in the resource.  | `null` | [`crate::connector::counter::curl::header::Header`] / [`crate::connector::counter::curl::body::Body`]                |
//! | redirection_limit    | - | Limit of redirection |    `5`    | Integer |
//! | version    | - | HTTP version|    `1`    | Integer |
//! | is_cached  | cache | Enable the cache management. |    `false`    | `true` / `false` |
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
//!            "is_cached": false,
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
use anyhow::Context as AnyContext;
use async_native_tls::TlsStream;
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncRead as AsyncReadIo;
use futures::AsyncWrite as AsyncWriteIo;
use futures::{AsyncWriteExt, Stream};
use http::{
    header, request::Builder, HeaderName, HeaderValue, Method, Request, Response, StatusCode,
    Version,
};
use http::{HeaderMap, Uri};
use http_body_util::{BodyExt, Full};
use http_cache_semantics::{BeforeRequest, CachePolicy};
use hyper::body::Incoming;
use hyper::client::conn::http1::{Connection as ConnectionHttp1, SendRequest as SendRequestHttp1};
use hyper::client::conn::http2::SendRequest as SendRequestHttp2;
use json_value_merge::Merge;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::Timer;
use smol::{io, net::TcpStream};
use smol_hyper::rt::FuturesIo;
use smol_timeout::TimeoutExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

const REDIRECT_CODES: &[StatusCode; 5] = &[
    StatusCode::MOVED_PERMANENTLY,
    StatusCode::FOUND,
    StatusCode::SEE_OTHER,
    StatusCode::TEMPORARY_REDIRECT,
    StatusCode::PERMANENT_REDIRECT,
];
const DEFAULT_TIMEOUT: u64 = 5;
const DEFAULT_CACHE_DIR: &str = "cache/http";

#[derive(Deserialize, Serialize)]
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
    #[serde(alias = "params")]
    pub parameters: Value,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
    pub redirection_limit: usize,
    pub version: usize,
    #[serde(alias = "cache")]
    #[serde(alias = "cache_enabled")]
    pub is_cached: bool,
    #[serde(skip)]
    #[serde(default)]
    client: Option<ClientType>,
}

pub enum ClientType {
    Http1(SendRequestHttp1<Pin<Box<Full<Bytes>>>>),
    Http2(SendRequestHttp2<Pin<Box<Full<Bytes>>>>),
}

impl Clone for Curl {
    fn clone(&self) -> Self {
        Self {
            document: self.document.clone(),
            metadata: self.metadata.clone(),
            authenticator_type: self.authenticator_type.clone(),
            endpoint: self.endpoint.clone(),
            path: self.path.clone(),
            method: self.method.clone(),
            headers: self.headers.clone(),
            timeout: self.timeout,
            parameters: self.parameters.clone(),
            paginator_type: self.paginator_type.clone(),
            counter_type: self.counter_type.clone(),
            redirection_limit: self.redirection_limit,
            version: self.version,
            is_cached: self.is_cached,
            client: None,
        }
    }
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
            // Can contain sensitive data
            .field("parameters", &self.parameters.display_only_for_debugging())
            .field("paginator_type", &self.paginator_type)
            .field("counter_type", &self.counter_type)
            .field("redirection_limit", &self.redirection_limit)
            .field("version", &self.version)
            .field("is_cached", &self.is_cached)
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
            parameters: Value::Null,
            paginator_type: PaginatorType::default(),
            counter_type: None,
            redirection_limit: 5,
            version: 1,
            is_cached: false,
            client: None,
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

#[derive(Clone, Copy)]
pub struct SmolExecutor;

impl<F> hyper::rt::Executor<F> for SmolExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        smol::spawn(fut).detach();
    }
}

#[derive(Clone, Copy)]
struct RetryPolicy {
    max_attempts: usize,
    delay: Duration,
    retry_on_status: &'static [StatusCode],
    retry_on_method: &'static [Method],
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: Duration::from_millis(200),
            retry_on_status: &[
                StatusCode::REQUEST_TIMEOUT,
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::BAD_GATEWAY,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::GATEWAY_TIMEOUT,
            ],
            retry_on_method: &[
                Method::GET,
                Method::HEAD,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ],
        }
    }
}

impl RetryPolicy {
    fn is_retryable_status(self, status: &StatusCode) -> bool {
        self.retry_on_status.contains(&status)
    }
    fn is_retryable_method(self, method: &Method) -> bool {
        self.retry_on_method.contains(&method)
    }
}

async fn backoff(attempt: usize, base_delay: Duration) {
    let max_delay = Duration::from_secs(30);

    let delay = base_delay
        .checked_mul(2u32.saturating_pow(attempt as u32))
        .unwrap_or(max_delay)
        .min(max_delay);

    Timer::after(delay).await;
}

impl Curl {
    async fn get_or_create_client(&mut self) -> io::Result<&mut ClientType> {
        if self.client.is_none() {
            self.client = Some(match self.version {
                1 => ClientType::Http1(self.http1().await?),
                2 => ClientType::Http2(self.http2().await?),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Unsupported HTTP version",
                    ))
                }
            });
        }
        Ok(self.client.as_mut().unwrap())
    }
    async fn http1(&self) -> io::Result<SendRequestHttp1<Pin<Box<Full<Bytes>>>>> {
        use hyper::client::conn::http1;

        let uri: hyper::Uri = self
            .endpoint
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;

        let scheme = uri
            .scheme_str()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing scheme"))?;

        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing host"))?;

        let port = uri.port_u16().unwrap_or(match scheme {
            "http" => 80,
            "https" => 443,
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "unsupported scheme",
                ))
            }
        });

        let tcp = TcpStream::connect((host, port))
            .timeout(Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT)))
            .await
            .ok_or_else(|| io::Error::new(ErrorKind::TimedOut, "connect timeout"))??;

        tcp.set_nodelay(true)?;

        let stream = match scheme {
            "http" => SmolStream::Plain(tcp),
            "https" => {
                let tls: TlsStream<TcpStream> = async_native_tls::connect(host, tcp)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                SmolStream::Tls(tls)
            }
            _ => unreachable!(),
        };

        let (sender, conn): (
            SendRequestHttp1<Pin<Box<Full<Bytes>>>>,
            ConnectionHttp1<FuturesIo<SmolStream>, Pin<Box<Full<Bytes>>>>,
        ) = http1::Builder::new()
            .title_case_headers(true)
            .handshake(FuturesIo::new(stream))
            .await
            .map_err(|e| io::Error::new(ErrorKind::ConnectionAborted, e))?;

        smol::spawn(async move {
            if let Err(e) = conn.await {
                warn!(error = %e, "HTTP/1 connection closed");
            }
        })
        .detach();

        Ok(sender)
    }
    async fn http2(&mut self) -> io::Result<SendRequestHttp2<Pin<Box<Full<Bytes>>>>> {
        let uri = self
            .endpoint
            .parse::<hyper::Uri>()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
        let host = uri.host().unwrap_or("0.0.0.0");
        let port = match uri.port_u16() {
            Some(p) => p,
            None => match uri.scheme_str() {
                Some("http") => 80,
                Some("https") => 443,
                _ => {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidInput,
                        "Unsupported scheme",
                    ))
                }
            },
        };

        // Connect with timeout
        let tcp = TcpStream::connect((host, port))
            .timeout(Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT)))
            .await
            .ok_or_else(|| io::Error::new(ErrorKind::TimedOut, "connect timeout"))??;

        // Wrap TLS if needed
        let stream = match uri.scheme_str() {
            Some("http") => SmolStream::Plain(tcp),
            Some("https") => {
                let tls: TlsStream<TcpStream> = async_native_tls::connect(host, tcp)
                    .await
                    .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
                SmolStream::Tls(tls)
            }
            _ => unreachable!(),
        };

        let io = FuturesIo::new(stream);
        let executor = SmolExecutor;

        let (sender, connection) = hyper::client::conn::http2::Builder::new(executor)
            .handshake(io)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e))?;

        // Spawn connection task
        smol::spawn(async move {
            if let Err(e) = connection.await {
                warn!(error = e.to_string(), "HTTP/2 connection failed");
            }
        })
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

        let uri = format!("{}{}", self.endpoint, path)
            .parse::<hyper::Uri>()
            .with_context(|| format!("failed to parse URI: {}{}", self.endpoint, path))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut request_builder = Request::builder().uri(&uri).method(
            Method::from_bytes(self.method.to_uppercase().as_bytes())
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
        );

        let host = match uri.port_u16() {
            Some(port) => format!("{}:{}", uri.host().unwrap_or("localhost"), port),
            None => uri.host().unwrap_or("localhost").to_string(),
        };

        request_builder = match self.version {
            1 => request_builder
                .header(header::HOST, host)
                .version(Version::HTTP_11),
            2 => request_builder
                .header(":authority", host)
                .version(Version::HTTP_2),
            3 => request_builder
                .header(":authority", host)
                .version(Version::HTTP_3),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("This http version '{}' is not managed", self.version),
                ))
            }
        };

        // Force the content type
        let content_type = self.metadata().content_type();
        if !content_type.is_empty() {
            request_builder = request_builder.header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
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
    /// Retrieve headers from the remote resource.
    pub async fn headers(&mut self) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut request_builder = self.request_builder().await?;
        let path = self.path();

        let mut parameters_without_context = self.parameters_without_context()?;
        parameters_without_context.replace_mustache(self.parameters.clone());
        let dataset = vec![DataResult::Ok(parameters_without_context)];

        let (body, body_size) = self.get_request_body(&dataset).await?;

        request_builder = request_builder.header(header::CONTENT_LENGTH, body_size.to_string());

        let request = request_builder
            .body(Box::pin(body))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if self.is_cached {
            if let Ok(Some(cache_entry)) = CachedEntry::get(&request).await {
                return Ok(cache_entry
                    .resp_headers
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
                    .collect());
            }
        }

        println!("request {:?}", request);

        let (_, final_response) = self.follow_redirects(request).await?;

        info!(path, "✅ Fetch headers with success");

        Ok(final_response
            .headers()
            .iter()
            .map(|(key, value)| (key.to_string().clone(), value.as_bytes().to_vec()))
            .collect())
    }
    /// Return parameter's values without context.
    fn parameters_without_context(&self) -> Result<Value> {
        Ok(match self.parameters.clone().search("/input")? {
            Some(input) => input,
            None => self.parameters.clone(),
        })
    }
    async fn get_request_body(&self, dataset: &Vec<DataResult>) -> Result<(Full<Bytes>, usize)> {
        match self.method.to_uppercase().as_str() {
            "POST" | "PUT" | "PATCH" => {
                let mut buffer = Vec::default();
                let mut document = self.document()?.clone_box();
                document.set_entry_path(String::default());
                buffer.write_all(&document.header(dataset)?).await?;
                buffer.write_all(&document.write(dataset)?).await?;
                buffer.write_all(&document.footer(dataset)?).await?;

                // Specific clean for x-www-form-urlencoded
                if document.metadata().mime_subtype.as_deref() == Some("x-www-form-urlencoded") {
                    if buffer.starts_with(b"\"") {
                        buffer.drain(0..1);
                    }
                    if buffer.ends_with(b"\"") {
                        buffer.pop();
                    }
                }

                let buffer_len = buffer.len();

                Ok((Full::new(Bytes::from(buffer)), buffer_len))
            }
            _ => Ok((Full::new(Bytes::new()), 0)),
        }
    }
    async fn follow_redirects(
        &mut self,
        mut request: Request<Pin<Box<Full<Bytes>>>>,
    ) -> io::Result<(Request<Pin<Box<Full<Bytes>>>>, Response<Incoming>)> {
        let base_uri = request.uri().clone();

        for _ in 0..=self.redirection_limit {
            let response = self.send_with_retry(request.clone()).await?;

            if !REDIRECT_CODES.contains(&response.status()) {
                return Ok((request, response));
            }

            let location = response
                .headers()
                .get(header::LOCATION)
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing Location header"))?
                .to_str()
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            let new_uri = if location.starts_with("http://") || location.starts_with("https://") {
                println!("starts_with");
                location
                    .parse()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            } else {
                println!("relative path");
                let mut parts = base_uri.clone().into_parts();
                parts.path_and_query = Some(
                    location
                        .parse()
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                );
                Uri::from_parts(parts).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            };

            let status = response.status();

            trace!(
                base_uri = %base_uri.clone(),
                status = %response.status(),
                location,
                method = %request.method(),
                new_uri = %new_uri,
                "following redirect"
            );

            // Update request URI
            *request.uri_mut() = new_uri;

            // Method rewrite rules (RFC + de facto)
            match status {
                StatusCode::SEE_OTHER | StatusCode::FOUND | StatusCode::MOVED_PERMANENTLY => {
                    *request.method_mut() = Method::GET;
                    *request.body_mut() = Box::pin(Full::default()); // clear body for GET
                }
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => {
                    // preserve method + body
                }
                _ => {}
            }
        }

        Err(Error::new(ErrorKind::InvalidInput, "too many redirects"))
    }
    async fn send_with_retry(
        &mut self,
        mut request: Request<Pin<Box<Full<Bytes>>>>,
    ) -> io::Result<Response<hyper::body::Incoming>> {
        let policy = RetryPolicy::default();
        let method = request.method().clone();
        let original_request = request.clone();

        for attempt in 1..=policy.max_attempts {
            let client = self.get_or_create_client().await?;

            let result = match client {
                ClientType::Http1(sender) => sender.try_send_request(request.clone()).await,
                ClientType::Http2(sender) => sender.try_send_request(request.clone()).await,
            };

            match result {
                Ok(response) => {
                    if policy.is_retryable_status(&response.status())
                        && policy.is_retryable_method(&method)
                        && attempt < policy.max_attempts
                    {
                        backoff(attempt, policy.delay).await;
                        request = original_request.clone();
                        continue;
                    }
                    return Ok(response);
                }

                Err(e) => {
                    // Determine retryable transport errors
                    let retryable = e.error().is_closed()
                        || e.error().is_incomplete_message()
                        || e.error().is_timeout();

                    if retryable
                        && attempt < policy.max_attempts
                        && policy.is_retryable_method(&method)
                    {
                        warn!(
                            attempt,
                            "Retrying request after transport error: {}",
                            e.into_error()
                        );
                        self.client = None; // force reconnect
                        backoff(attempt, policy.delay).await;
                        request = original_request.clone();
                        continue;
                    }

                    return Err(Error::new(ErrorKind::Interrupted, e.into_error()));
                }
            }
        }

        Err(Error::new(ErrorKind::TimedOut, "retry limit exceeded"))
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
    fn document(&self) -> Result<&dyn Document> {
        self.document.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "The document has not been set in the connector",
            )
        })
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
        let path = self.path();

        let mut parameters_without_context = self.parameters_without_context()?;
        parameters_without_context.replace_mustache(self.parameters.clone());
        let dataset = vec![DataResult::Ok(parameters_without_context)];

        let (body, body_size) = self.get_request_body(&dataset).await?;

        request_builder = request_builder.header(header::CONTENT_LENGTH, body_size.to_string());
        println!("request_builder {:?}", request_builder);
        let request = request_builder
            .body(Box::pin(body))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        println!("request {:?}", request);

        if self.is_cached {
            info!(path, "✅ Fetch data from cache with success");

            if let Ok(Some(cache_entry)) = CachedEntry::get(&request).await {
                let document = self.document()?;
                let dataset = document.read(&cache_entry.data)?;

                return Ok(Some(Box::pin(stream! {
                    for data in dataset {
                        yield data;
                    }
                })));
            }
        }

        let (final_request, final_response) = self.follow_redirects(request).await?;
        let status = final_response.status().as_u16();
        let request_headers = final_request.headers();
        let response_headers = final_response.headers().clone();

        let headers_to_map = |headers: &HeaderMap| {
            headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
                .collect()
        };

        let data = final_response
            .collect()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            .to_bytes()
            .to_vec();

        if self.is_cached {
            CachedEntry::new(
                status,
                headers_to_map(request_headers),
                headers_to_map(&response_headers),
                data.clone(),
            )
            .save(&final_request.uri().to_string())
            .await?;
        }

        info!(path, "✅ Fetch data with success");

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
    #[instrument(
        name = "curl::send",
        skip(self, dataset),
        fields(
            method = %self.method,
            path = %self.path()
        )
    )]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let mut request_builder = self.request_builder().await?;
        let path = self.path();

        let (body, body_size) = self.get_request_body(dataset).await?;

        request_builder = request_builder.header(header::CONTENT_LENGTH, body_size.to_string());

        let request = request_builder
            .body(Box::pin(body))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        println!("request {:?}", request);

        let (_, final_response) = self.follow_redirects(request).await?;
        let data = final_response
            .collect()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            .to_bytes()
            .to_vec();

        info!(path, "✅ Send data with success");

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
        let path = self.path();

        request_builder = request_builder.method(hyper::Method::DELETE);

        let request = request_builder
            .body(Box::pin(Full::new(Bytes::new())))
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let (final_request, _) = self.follow_redirects(request).await?;

        if self.is_cached {
            CachedEntry::remove(&final_request.uri().to_string()).await?;
        }

        info!(path, "✅ Erase data with success");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        self.paginator_type.paginate(self).await
    }
}

#[derive(Serialize, Deserialize)]
struct CachedEntry {
    status: u16,
    req_headers: HashMap<String, String>,
    resp_headers: HashMap<String, String>,
    data: Vec<u8>,
}

impl CachedEntry {
    fn new(
        status: u16,
        req_headers: HashMap<String, String>,
        resp_headers: HashMap<String, String>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            status,
            req_headers,
            resp_headers,
            data,
        }
    }
    /// Save the entry in the cache.
    async fn save(&self, uri: &str) -> Result<()> {
        let json = serde_json::to_vec(&self)?;

        cacache::write(
            std::env::temp_dir().join(self::DEFAULT_CACHE_DIR),
            &uri,
            json,
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        Ok(())
    }
    /// Get the entry from the cache.
    async fn get(
        request: &Request<Pin<Box<http_body_util::Full<bytes::Bytes>>>>,
    ) -> Result<Option<Self>> {
        let uri = request.uri().to_string();
        let data =
            match cacache::read(std::env::temp_dir().join(self::DEFAULT_CACHE_DIR), &uri).await {
                Ok(data) => data,
                Err(e) => {
                    trace!(uri, "{}", e);
                    return Ok(None);
                }
            };

        let cached: Self = serde_json::from_slice(&data)?;

        let method = request.method().clone();

        let mut cache_req_builder = Request::builder().method(method).uri(&uri);

        for (k, v) in cached.req_headers.iter() {
            cache_req_builder = cache_req_builder.header(k, v);
        }

        let cache_req = cache_req_builder
            .body(())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut cache_resp_builder = Response::builder().status(cached.status);

        for (k, v) in cached.resp_headers.iter() {
            cache_resp_builder = cache_resp_builder.header(k, v);
        }

        let cache_resp = cache_resp_builder
            .body(())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let policy = CachePolicy::new(&cache_req, &cache_resp);

        match policy.before_request(request, SystemTime::now()) {
            BeforeRequest::Fresh(_) => {
                trace!(uri, "🔁 Data retrieved from cache");
                Ok(Some(cached))
            }
            BeforeRequest::Stale { .. } => {
                trace!(uri, "♻️ Cached data is stale");
                Ok(None)
            }
        }
    }
    /// Remove the entry from the cache.
    async fn remove(uri: &str) -> Result<()> {
        cacache::remove(std::env::temp_dir().join(self::DEFAULT_CACHE_DIR), &uri)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::authenticator::{basic::Basic, bearer::Bearer, AuthenticatorType};
    use crate::connector::counter::curl::CounterType;
    use crate::document::json::Json;
    use json_value_search::Search;
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
    async fn fetch_http1() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/json".to_string();
        connector.version = 1;
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn fetch_http2() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/json".to_string();
        connector.version = 2;
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn fetch_head() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "HEAD".into();
        connector.path = "/get".to_string();
        connector.is_cached = false;
        connector.set_document(Box::new(document)).unwrap();

        assert!(
            connector.fetch().await.unwrap().is_none(),
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
    #[apply(test!)]
    async fn test_redirection_with_erase() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/redirect-to?url=/delete".to_string();
        connector.redirection_limit = 1;

        let result = connector.erase().await;
        assert!(
            result.is_ok(),
            "The inner connector shouldn't raise this error: {:?}",
            result
        );
    }
}
