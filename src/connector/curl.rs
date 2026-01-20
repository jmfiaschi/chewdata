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
//! | version    | - | HTTP version|    `1`    | `1` / `2` |
//! | is_cached  | cache | Enable the cache management. |    `false`    | `true` / `false` |
//! | certificate | crt | Path to a local certificate file used to trust the HTTPS connection. | `null` | Local path of a .crt file |
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
//!             "version": "1",
//!             "crt": "./crt/my_certificate.crt"
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
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncRead as AsyncReadIo;
use futures::AsyncWrite as AsyncWriteIo;
use futures::{AsyncWriteExt, Stream};
use futures_rustls::TlsConnector;
use futures_rustls::TlsStream as RustlsTlsStream;
use http::uri::{Authority, Scheme};
use http::HeaderMap;
use http::{
    header, request::Builder, HeaderName, HeaderValue, Method, Request, Response, StatusCode,
    Version,
};
use http_body_util::{BodyExt, Empty, Full};
use http_cache_semantics::{BeforeRequest, CachePolicy};
use hyper::body::Body;
use hyper::client::conn::http1::{Connection as ConnectionHttp1, SendRequest as SendRequestHttp1};
use hyper::client::conn::http2::SendRequest as SendRequestHttp2;
use json_value_merge::Merge;
use json_value_search::Search;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::io;
use smol::net::TcpStream;
use smol::{Executor, Timer};
use smol_hyper::rt::{FuturesIo, SmolExecutor};
use smol_timeout::TimeoutExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};
use webpki_roots::TLS_SERVER_ROOTS;

type DynBody = Pin<Box<dyn Body<Data = Bytes, Error = io::Error> + Send + Sync>>;

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
    #[serde(with = "method_uppercase")]
    pub method: Method,
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
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
    #[serde(alias = "crt")]
    pub certificate: Option<String>,
    #[serde(skip)]
    #[serde(default)]
    client: Option<ClientType>,
}

mod method_uppercase {
    use http::Method;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Method, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let upper = s.to_ascii_uppercase();

        Method::from_bytes(upper.as_bytes()).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(method: &Method, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(method.as_str())
    }
}

pub enum ClientType {
    Http1(SendRequestHttp1<DynBody>),
    Http2(SendRequestHttp2<DynBody>),
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
            certificate: None,
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
            .field("certificate", &self.certificate)
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
            method: Method::GET,
            headers: HeaderMap::new(),
            timeout: Some(DEFAULT_TIMEOUT),
            parameters: Value::Null,
            paginator_type: PaginatorType::default(),
            counter_type: None,
            redirection_limit: 5,
            version: 1,
            is_cached: false,
            certificate: None,
            client: None,
        }
    }
}

/// A TCP or TCP+TLS connection.
enum SmolStream {
    /// A plain TCP connection.
    Plain(TcpStream),

    /// A TCP connection secured by TLS.
    Tls(Box<RustlsTlsStream<TcpStream>>),
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
        self.retry_on_status.contains(status)
    }
    fn is_retryable_method(self, method: &Method) -> bool {
        self.retry_on_method.contains(method)
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

fn build_request(request_builder: Builder, body: &Bytes) -> io::Result<Request<DynBody>> {
    Ok(match body.len() {
        0 => request_builder
            .body(
                Box::pin(Empty::new().map_err(|e| Error::new(ErrorKind::InvalidData, e)))
                    as DynBody,
            )
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
        _ => request_builder
            .body(Box::pin(
                Full::from(body.clone()).map_err(|e| Error::new(ErrorKind::InvalidData, e)),
            ) as DynBody)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
    })
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
    #[instrument(name = "curl::http1")]
    async fn http1(&self) -> io::Result<SendRequestHttp1<DynBody>> {
        use hyper::client::conn::http1;

        let base = self
            .endpoint
            .parse::<hyper::Uri>()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

        let scheme: Scheme = base
            .scheme_str()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing scheme"))?
            .try_into()
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "unsupported scheme"))?;

        let host: String = base
            .host()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing host"))?
            .to_owned();

        let port = base.port_u16().unwrap_or(if scheme == Scheme::HTTP {
            80
        } else if scheme == Scheme::HTTPS {
            443
        } else {
            return Err(io::Error::new(ErrorKind::InvalidInput, "unsupported port"));
        });

        let tcp = match TcpStream::connect((host.clone(), port))
            .timeout(Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT)))
            .await
        {
            None => return Err(io::Error::new(ErrorKind::TimedOut, "connect timeout")),
            Some(Err(e)) => return Err(e),
            Some(Ok(tcp)) => tcp,
        };

        tcp.set_nodelay(true)?;

        let stream = if scheme == Scheme::HTTP {
            SmolStream::Plain(tcp)
        } else if scheme == Scheme::HTTPS {
            let mut roots = RootCertStore::empty();
            roots.extend(TLS_SERVER_ROOTS.iter().cloned());

            if let Some(certificate_path) = &self.certificate {
                let iter = CertificateDer::pem_file_iter(certificate_path)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

                let certs: Vec<CertificateDer<'_>> = iter.filter_map(|res| res.ok()).collect();

                roots.add_parsable_certificates(certs.into_iter());
            }

            let mut config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();

            config.alpn_protocols.clear();

            let connector = TlsConnector::from(Arc::new(config));
            let server_name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid DNS name"))?;

            let tls = connector.connect(server_name, tcp).await?;
            SmolStream::Tls(Box::new(futures_rustls::TlsStream::Client(tls)))
        } else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "unsupported scheme",
            ));
        };

        let (sender, connection): (
            SendRequestHttp1<DynBody>,
            ConnectionHttp1<FuturesIo<SmolStream>, DynBody>,
        ) = http1::Builder::new()
            .title_case_headers(false)
            .handshake(FuturesIo::new(stream))
            .await
            .map_err(|e| io::Error::new(ErrorKind::ConnectionAborted, e))?;

        smol::spawn(async move {
            debug!("HTTP/1 connection task started");
            if let Err(e) = connection.await {
                warn!(error = %e, "HTTP/1 connection closed");
            }
        })
        .detach();

        Ok(sender)
    }
    #[instrument(name = "curl::http2")]
    async fn http2(&mut self) -> io::Result<SendRequestHttp2<DynBody>> {
        let base = self
            .endpoint
            .parse::<hyper::Uri>()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

        let scheme: Scheme = base
            .scheme_str()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing scheme"))?
            .try_into()
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "unsupported scheme"))?;

        let host: String = base
            .host()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing host"))?
            .to_owned();

        let port = base.port_u16().unwrap_or(if scheme == Scheme::HTTP {
            80
        } else if scheme == Scheme::HTTPS {
            443
        } else {
            return Err(io::Error::new(ErrorKind::InvalidInput, "unsupported port"));
        });

        let tcp = match TcpStream::connect((host.clone(), port))
            .timeout(Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT)))
            .await
        {
            None => return Err(io::Error::new(ErrorKind::TimedOut, "connect timeout")),
            Some(Err(e)) => return Err(e),
            Some(Ok(tcp)) => tcp,
        };

        // ---- TLS (rustls + ALPN h2) ----
        let mut roots = RootCertStore::empty();
        roots.extend(TLS_SERVER_ROOTS.iter().cloned());

        if let Some(certificate_path) = &self.certificate {
            let iter = CertificateDer::pem_file_iter(certificate_path)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            let certs: Vec<CertificateDer<'_>> = iter.filter_map(|res| res.ok()).collect();

            roots.add_parsable_certificates(certs.into_iter());
        }

        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        config.alpn_protocols = vec![b"h2".to_vec()];

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "invalid DNS name"))?;

        let tls = connector.connect(server_name, tcp).await?;

        debug_assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));

        // ---- Hyper HTTP/2 ----
        let io = FuturesIo::new(tls);
        let exec = Arc::new(Executor::new());
        smol::spawn({
            let exec = exec.clone();
            async move {
                exec.run(futures::future::pending::<()>()).await;
            }
        })
        .detach();
        let executor = SmolExecutor::new(exec);

        let (sender, connection) = hyper::client::conn::http2::Builder::new(executor)
            .handshake(io)
            .await
            .map_err(|e| io::Error::new(ErrorKind::ConnectionAborted, e))?;

        smol::spawn(async move {
            debug!("HTTP/2 connection task started");
            if let Err(e) = connection.await {
                warn!(error = %e, "HTTP/2 connection closed");
            }
        })
        .detach();

        Ok(sender)
    }

    /// Get a new request builder base on what has been setup in the configuration.
    async fn request_builder(
        &mut self,
        override_uri: Option<&str>,
        override_method: Option<&Method>,
        body: Option<&Bytes>,
    ) -> std::io::Result<Builder> {
        let path = self.path();
        let mut request_builder = Request::builder();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let uri = if let Some(uri) = override_uri {
            uri.parse::<hyper::Uri>()
                .with_context(|| format!("failed to parse URI: {}", uri))
        } else {
            format!("{}{}", self.endpoint, path)
                .parse::<hyper::Uri>()
                .with_context(|| format!("failed to parse URI: {}{}", self.endpoint, path))
        }
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let host = match uri.port_u16() {
            Some(port) => format!("{}:{}", uri.host().unwrap_or("localhost"), port),
            None => uri.host().unwrap_or("localhost").to_string(),
        };

        let method = if let Some(method) = override_method {
            method.clone()
        } else {
            self.method.clone()
        };

        request_builder = request_builder.uri(uri).method(method);

        request_builder = match self.version {
            1 => request_builder
                .header(header::HOST, host)
                .version(Version::HTTP_11),
            2 => request_builder
                .version(Version::HTTP_2)
                .header(header::HOST, host),
            3 => request_builder.version(Version::HTTP_3),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("This http version '{}' is not managed", self.version),
                ))
            }
        };

        // Force the content type
        let content_type = self.metadata().content_type();
        let body_length = if let Some(bytes) = body {
            bytes.len()
        } else {
            0
        };

        if !content_type.is_empty() && body_length > 0 {
            request_builder = request_builder.header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );

            request_builder = request_builder.header(header::CONTENT_LENGTH, body_length);
        }

        // Force the headers
        for (header_name, header_value) in self.headers.iter() {
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
    #[instrument(name = "curl::headers")]
    pub async fn headers(&mut self) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut parameters_without_context = self.parameters_without_context()?;
        parameters_without_context.replace_mustache(self.parameters.clone());
        let dataset = vec![DataResult::Ok(parameters_without_context)];
        let body = self.body(&dataset).await?;
        let request_builder = self.request_builder(None, None, Some(&body)).await?;
        let request = build_request(request_builder, &body)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if self.is_cached {
            if let Ok(Some(cache_entry)) = CachedEntry::get(&request).await {
                info!("Fetch headers from cache with success");

                return Ok(cache_entry
                    .resp_headers
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
                    .collect());
            }
        }

        let request_builder = self.request_builder(None, None, Some(&body)).await?;
        let entry_to_cache = self.follow_redirects(request_builder, &body).await?;

        info!("Fetch headers with success");

        Ok(entry_to_cache
            .resp_headers
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
    async fn body(&self, dataset: &Vec<DataResult>) -> Result<Bytes> {
        match self.method {
            Method::POST | Method::PUT | Method::PATCH => {
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

                Ok(Bytes::from(buffer))
            }
            _ => Ok(Bytes::new()),
        }
    }
    #[instrument(name = "curl::follow_redirects")]
    async fn follow_redirects(
        &mut self,
        request_builder: Builder,
        original_bytes: &Bytes,
    ) -> io::Result<CachedEntry> {
        let request_builder = request_builder;

        let (endpoint, mut current_uri) = match request_builder.uri_ref() {
            Some(uri) => (
                format!(
                    "{}://{}",
                    uri.scheme().unwrap_or(&Scheme::HTTP),
                    uri.authority()
                        .unwrap_or(&Authority::from_static("localhost"))
                ),
                uri.to_string(),
            ),
            None => return Err(Error::new(ErrorKind::InvalidInput, "Uri is required")),
        };

        let mut current_method = match request_builder.method_ref() {
            Some(method) => method.clone(),
            None => return Err(Error::new(ErrorKind::InvalidInput, "Method is required")),
        };

        let mut bytes = original_bytes.clone();

        for _ in 0..=self.redirection_limit {
            let entry_to_cache = self
                .send_with_retry(&current_uri, &current_method, &bytes)
                .await?;

            if !REDIRECT_CODES.contains(
                &StatusCode::from_u16(entry_to_cache.status)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            ) {
                return Ok(entry_to_cache);
            }

            let location = entry_to_cache
                .resp_headers
                .get(header::LOCATION.as_str())
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Missing Location header"))?;

            current_uri = if location.to_string().starts_with("/") {
                format!("{}{}", endpoint, location)
            } else {
                location.to_string()
            };

            info!(%current_uri, %location, "Redirecting");

            // Apply redirect rules for method/body
            match StatusCode::from_u16(entry_to_cache.status)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            {
                hyper::StatusCode::SEE_OTHER => {
                    current_method = Method::GET;
                    bytes = Bytes::new(); // drop body
                }
                hyper::StatusCode::MOVED_PERMANENTLY | hyper::StatusCode::FOUND => {
                    if current_method == Method::POST {
                        current_method = Method::GET;
                        bytes = Bytes::new();
                    }
                }
                hyper::StatusCode::TEMPORARY_REDIRECT | hyper::StatusCode::PERMANENT_REDIRECT => {
                    // keep method + body
                }
                _ => {}
            }
        }

        Err(Error::new(ErrorKind::InvalidInput, "too many redirects"))
    }
    async fn send_with_retry(
        &mut self,
        uri: &str,
        method: &Method,
        body: &Bytes,
    ) -> io::Result<CachedEntry> {
        let policy = RetryPolicy::default();

        for attempt in 1..=policy.max_attempts {
            let request_builder = self
                .request_builder(Some(uri), Some(method), Some(body))
                .await?;

            let client = self.get_or_create_client().await?;

            let req_headers = match request_builder.headers_ref() {
                Some(headers) => headers_to_map(headers),
                None => HashMap::default(),
            };

            let result = match client {
                ClientType::Http1(sender) => {
                    sender
                        .send_request(build_request(request_builder, body)?)
                        .await
                }
                ClientType::Http2(sender) => {
                    sender
                        .send_request(build_request(request_builder, body)?)
                        .await
                }
            };

            match result {
                Ok(response) => {
                    if policy.is_retryable_status(&response.status())
                        && policy.is_retryable_method(method)
                        && attempt < policy.max_attempts
                    {
                        backoff(attempt, policy.delay).await;
                        continue;
                    }

                    let resp_status = response.status().as_u16();
                    let resp_headers = headers_to_map(response.headers());
                    let data = response
                        .collect()
                        .await
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                        .to_bytes()
                        .to_vec();

                    let request_to_cache = CachedEntry::new(
                        resp_status,
                        method.to_string(),
                        uri.to_string(),
                        req_headers,
                        resp_headers,
                        data,
                    );

                    return Ok(request_to_cache);
                }

                Err(e) => {
                    // Determine retryable transport errors
                    let retryable = e.is_closed() || e.is_incomplete_message() || e.is_timeout();

                    if retryable
                        && attempt < policy.max_attempts
                        && policy.is_retryable_method(method)
                    {
                        warn!(attempt, "Retrying request after transport error: {}", e);
                        self.client = None; // force reconnect
                        backoff(attempt, policy.delay).await;
                        continue;
                    }

                    return Err(Error::new(ErrorKind::Interrupted, e));
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
    /// ```
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
    /// ```
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
    /// ```
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
    /// ```
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use smol::stream::StreamExt;
    /// use std::io;
    /// use http::Method;
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
    ///     connector.method = Method::GET;
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
    #[instrument(name = "curl::fetch", skip(self))]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let mut parameters_without_context = self.parameters_without_context()?;
        parameters_without_context.replace_mustache(self.parameters.clone());
        let dataset = vec![DataResult::Ok(parameters_without_context)];
        let body = self.body(&dataset).await?;
        let request_builder = self.request_builder(None, None, Some(&body)).await?;
        let request = build_request(request_builder, &body)?;

        if self.is_cached {
            if let Ok(Some(cache_entry)) = CachedEntry::get(&request).await {
                let document = self.document()?;
                let dataset = document.read(&cache_entry.data)?;

                info!("Fetch data from cache with success");

                return Ok(Some(Box::pin(stream! {
                    for data in dataset {
                        yield data;
                    }
                })));
            }
        }

        let request_builder = self.request_builder(None, None, Some(&body)).await?;
        let mut entry_to_cache = self.follow_redirects(request_builder, &body).await?;

        entry_to_cache.method = self.method.to_string();

        if self.is_cached {
            entry_to_cache.save().await?;
        }

        let data = entry_to_cache.data;

        info!("Fetch data with success");

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
    /// ```
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use json_value_search::Search;
    /// use serde_json::Value;
    /// use http::Method;
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
    ///     connector.method = Method::POST;
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
    #[instrument(name = "curl::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let body = self.body(dataset).await?;

        let request_builder = self.request_builder(None, None, Some(&body)).await?;
        let entry_to_cache = self.follow_redirects(request_builder, &body).await?;

        let data = entry_to_cache.data;

        info!("Send data with success");

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
    /// ```
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
        let path = self.path();
        let body = self.body(&Vec::default()).await?;

        let request_builder = self
            .request_builder(None, Some(&Method::DELETE), Some(&body))
            .await?;
        let entry_to_cache = self.follow_redirects(request_builder, &body).await?;

        if self.is_cached {
            entry_to_cache.remove().await?;

            info!("Erase cache entry with success");
        }

        info!(path, "Erase data with success");
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
    pub status: u16,
    pub method: String,
    pub uri: String,
    pub req_headers: HashMap<String, String>,
    pub resp_headers: HashMap<String, String>,
    pub data: Vec<u8>,
}

impl fmt::Debug for CachedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedEntry")
            .field("status", &self.status)
            .field("method", &self.method)
            .field("uri", &self.uri)
            .field("req_headers", &self.req_headers)
            .field("resp_headers", &self.resp_headers)
            .field("data", &self.data.display_only_for_debugging())
            .finish()
    }
}

impl CachedEntry {
    fn new(
        status: u16,
        method: String,
        uri: String,
        req_headers: HashMap<String, String>,
        resp_headers: HashMap<String, String>,
        data: Vec<u8>,
    ) -> Self {
        Self {
            status,
            method,
            uri,
            req_headers,
            resp_headers,
            data,
        }
    }
    /// Persist the cache entry on disk using the request URI as the cache key.
    #[instrument(name = "curl::cache_entry::save")]
    async fn save(&self) -> Result<()> {
        let cache_dir = std::env::temp_dir().join(self::DEFAULT_CACHE_DIR);
        let payload = serde_json::to_vec(&self)?;

        cacache::write(cache_dir, &self.uri, payload)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        trace!(uri = self.uri, "cache saved");
        Ok(())
    }
    /// Attempt to retrieve a valid cached entry for the given request.
    ///
    /// Cache freshness is evaluated using HTTP cache headers
    /// via `http_cache_semantics::CachePolicy`.
    #[instrument(name = "curl::cache_entry::get", skip(request))]
    async fn get(request: &Request<DynBody>) -> Result<Option<Self>> {
        let uri = request.uri().to_string();
        let cache_dir = std::env::temp_dir().join(self::DEFAULT_CACHE_DIR);

        let data = match cacache::read(cache_dir, &uri).await {
            Ok(data) => data,
            Err(e) => {
                trace!(uri, "cache miss: {}", e);
                return Ok(None);
            }
        };

        let cached: Self = serde_json::from_slice(&data)?;

        // Reconstruct request used to compute cache policy
        let mut cache_req_builder = Request::builder().method(cached.method.as_str()).uri(&uri);

        for (k, v) in cached.req_headers.iter() {
            cache_req_builder = cache_req_builder.header(k, v);
        }

        let cache_req = cache_req_builder
            .body(())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        // Reconstruct response used to compute cache policy
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
                trace!(uri, entry = format!("{:?}", cached), "cache hit");
                Ok(Some(cached))
            }
            BeforeRequest::Stale { .. } => {
                trace!(uri, "cache stale");
                Ok(None)
            }
        }
    }
    /// Remove this entry from the on-disk cache.
    #[instrument(name = "curl::cache_entry::remove")]
    async fn remove(&self) -> Result<()> {
        let cache_dir = std::env::temp_dir().join(self::DEFAULT_CACHE_DIR);

        cacache::remove(cache_dir, &self.uri)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        trace!(uri = self.uri, "cache removed");
        Ok(())
    }
}

/// Convert an HTTP `HeaderMap` into a `HashMap<String, String>`,
/// discarding headers with non-UTF-8 values.
fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_string())))
        .collect()
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
    async fn http1_fetch() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::GET;
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
    async fn http1_fetch_through_https() {
        crate::init_tls().await.unwrap();

        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "https://localhost:8084".to_string();
        connector.method = Method::GET;
        connector.path = "/json".to_string();
        connector.certificate = Some("./.config/my-ca.crt".to_string());
        connector.version = 1;
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn http2_fetch_through_https() {
        crate::init_tls().await.unwrap();

        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "https://localhost:8084".to_string();
        connector.method = Method::GET;
        connector.path = "/json".to_string();
        connector.certificate = Some("./.config/my-ca.crt".to_string());
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
        connector.method = Method::HEAD;
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
        connector.method = Method::GET;
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
        connector.method = Method::GET;
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
        connector.method = Method::POST;
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
