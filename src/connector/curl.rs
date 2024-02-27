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
//! | cache_mode    | cache | Enable the backend cache management and define the cache strategy. See the details here <https://github.com/06chaynes/http-cache/blob/main/http-cache/src/lib.rs#L265-L295> |    `null`    | `default` / `no_store` / `reload` / `no_cache` / `force_cache` / `if_cached` / `ignore_rules` |
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
//!             "cache_mode": "default"
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
use async_std::sync::Arc;
use async_std::sync::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use http_cache_surf::{CACacheManager, Cache, CacheMode, HttpCache, HttpCacheOptions};
use http_types::headers::HeaderName;
use http_types::headers::HeaderValue;
use json_value_merge::Merge;
use json_value_remove::Remove;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time;
use std::time::Duration;
use std::{
    fmt,
    io::{Error, ErrorKind, Result, Write},
};
use surf::middleware::{Middleware, Next};
use surf::{
    http::{headers, Method, Url},
    Client,
};
use surf::{Request, Response};

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static CLIENTS: OnceLock<Arc<Mutex<HashMap<String, Client>>>> = OnceLock::new();

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Curl {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "auth")]
    #[serde(rename = "authenticator")]
    pub authenticator_type: Option<Box<AuthenticatorType>>,
    pub endpoint: String,
    pub path: String,
    pub method: Method,
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
    #[serde(alias = "cache")]
    pub cache_mode: Option<String>,
    pub redirection_limit: usize,
}

impl fmt::Debug for Curl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Curl")
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
            .field("cache_mode", &self.cache_mode)
            .field("redirection_limit", &self.redirection_limit)
            .finish()
    }
}

impl Default for Curl {
    fn default() -> Self {
        Curl {
            metadata: Metadata::default(),
            authenticator_type: None,
            endpoint: "".into(),
            path: "".into(),
            method: Method::Get,
            headers: Box::<HashMap<String, String>>::default(),
            timeout: Some(5),
            keepalive: true,
            tcp_nodelay: false,
            parameters: Value::Null,
            paginator_type: PaginatorType::default(),
            counter_type: None,
            cache_mode: None,
            redirection_limit: 5,
        }
    }
}

impl Curl {
    /// Generate a key used to store and retreive the connector's client.
    fn client_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let client_key = format!("{}:{}", self.endpoint, self.path);
        client_key.hash(&mut hasher);
        hasher.finish().to_string()
    }
    /// Create a new client for the connector.
    async fn create_client(&mut self) -> std::io::Result<Client> {
        let mut config = surf::Config::new()
            .set_base_url(
                Url::parse(self.endpoint.as_str())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            )
            .set_timeout(self.timeout.map(Duration::from_secs))
            .set_http_keep_alive(self.keepalive)
            .set_tcp_no_delay(self.tcp_nodelay);

        if !self.metadata().content_type().is_empty() {
            config = config
                .add_header(
                    HeaderName::from_bytes(headers::CONTENT_TYPE.to_string().into_bytes())
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                    HeaderValue::from_bytes(self.metadata().content_type().into_bytes())
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        }

        if !self.headers.is_empty() {
            for (key, value) in self.headers.iter() {
                config = config
                    .add_header(
                        HeaderName::from_bytes(key.clone().into_bytes())
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                        HeaderValue::from_bytes(value.clone().into_bytes())
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                    )
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            }
        }

        let mut client: Client = config
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        client = client
            .with(Logger::new())
            .with(surf::middleware::Redirect::new(
                self.redirection_limit as u8,
            ));

        if let Some(authenticator_type) = self.authenticator_type.clone() {
            client = client.with(Authenticator::new(authenticator_type));
        }
        if let Some(cache_mode) = &self.cache_mode {
            let cache_path = std::env::temp_dir().join("http-cacache");
            trace!(
                cache_dir = format!("{:?}", cache_path).as_str(),
                "Enabling the cache..."
            );

            client = client.with(Cache(HttpCache {
                mode: match cache_mode.to_lowercase().as_str() {
                    "no_store" | "nostore" => CacheMode::NoStore,
                    "force_cache" | "forcecache" => CacheMode::ForceCache,
                    "ignore_rules" | "ignorerules" => CacheMode::IgnoreRules,
                    "no_cache" | "nocache" => CacheMode::NoCache,
                    "only_if_cached" | "onlyifcached" | "if_cached" => CacheMode::OnlyIfCached,
                    "reload" => CacheMode::Reload,
                    _ => CacheMode::Default,
                },
                manager: CACacheManager { path: cache_path },
                options: HttpCacheOptions::default(),
            }));
        }
        Ok(client)
    }
    pub async fn client(&mut self) -> std::io::Result<Client> {
        let clients = CLIENTS.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let client_key = self.client_key();
        if let Some(client) = clients.lock().await.get(&self.client_key()) {
            trace!(client_key, "Retrieve the previous client");
            return Ok(client.clone());
        }

        trace!(client_key, "Create a new client");
        let mut map = clients.lock_arc().await;
        let client = self.create_client().await?;
        map.insert(client_key, client.clone());

        Ok(client)
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
        self.path.has_mustache()
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::counter::curl::CounterType;
    /// use std::io;
    ///
    /// #[async_std::main]
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
    /// use surf::http::Method;
    /// use futures::StreamExt;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/json".to_string();
    ///     let datastream = connector.fetch(&document).await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero."
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "curl::fetch")]
    async fn fetch(&mut self, document: &dyn Document) -> std::io::Result<Option<DataStream>> {
        let client = self.client().await?;
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, &url);

        match self.method {
            Method::Post | Method::Put | Method::Patch => {
                let mut buffer = Vec::default();
                let mut parameters_without_context = self.parameters_without_context()?;
                parameters_without_context.replace_mustache(self.parameters.clone());

                let dataset = vec![DataResult::Ok(parameters_without_context)];
                let mut document = document.clone_box();
                document.set_entry_path(String::default());
                buffer.write_all(&document.header(&dataset)?)?;
                buffer.write_all(&document.write(&dataset)?)?;
                buffer.write_all(&document.footer(&dataset)?)?;

                if let Some(mime_subtype) = document.metadata().mime_subtype {
                    if mime_subtype == "x-www-form-urlencoded" {
                        if buffer.starts_with(&[b'"']) {
                            buffer = buffer.drain(1..).collect();
                        }
                        if buffer.ends_with(&[b'"']) {
                            buffer.pop();
                        }
                    }
                }

                req = req.body(buffer.clone());
                req = req.header(headers::CONTENT_LENGTH, buffer.len().to_string());
            }
            _ => (),
        };

        // Force to replace the `application/octet-stream` by the connector content type.
        if !self.metadata().content_type().is_empty() {
            req = req.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        info!(
            url = url.as_str(),
            "Ready to retrieve data from the resource"
        );

        let mut res = client
            .send(req.build())
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
                    "Curl failed with status code '{}' and response's body: {}",
                    res.status(),
                    String::from_utf8(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        info!(url = url.as_str(), "Fetch data with success");

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
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use json_value_search::Search;
    /// use serde_json::Value;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Post;
    ///     connector.path = "/post".to_string();
    ///     let expected_result1 =
    ///        DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1];
    ///     let mut datastream = connector.send(&document, &dataset).await.unwrap().unwrap();
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
    async fn send(
        &mut self,
        document: &dyn Document,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let client = self.client().await?;
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, &url);

        match self.method {
            Method::Post | Method::Put | Method::Patch => {
                let mut buffer = Vec::default();

                buffer.append(&mut document.header(dataset)?);
                buffer.append(&mut document.write(dataset)?);
                buffer.append(&mut document.footer(dataset)?);

                if let Some(mime_subtype) = document.metadata().mime_subtype {
                    if mime_subtype == "x-www-form-urlencoded" {
                        if buffer.starts_with(&[b'"']) {
                            buffer = buffer.drain(1..).collect();
                        }
                        if buffer.ends_with(&[b'"']) {
                            buffer.pop();
                        }
                    }
                }

                req = req.body(buffer.clone());
                req = req.header(headers::CONTENT_LENGTH, buffer.len().to_string());
            }
            _ => (),
        };

        // Force to replace the `application/octet-stream` by the connector content type.
        if !self.metadata().content_type().is_empty() {
            req = req.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        info!(
            url = url.as_str(),
            "Ready to retrieve data into the resource"
        );

        let mut res = client
            .send(req.build())
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
                    "Curl failed with status code '{}' and response's body: {}",
                    res.status(),
                    String::from_utf8(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        if !data.is_empty() {
            let dataset = document.read(&data)?;

            return Ok(Some(Box::pin(stream! {
                for data in dataset {
                    yield data;
                }
            })));
        }

        info!(url = url.as_str(), "Send data with success");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    #[instrument(name = "curl::erase")]
    async fn erase(&mut self) -> Result<()> {
        let client = self.client().await?;
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, &url);

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        info!(url = url.as_str(), "Ready to erase data in the resource");

        let mut res = client
            .send(req.build())
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response's body: {}",
                    res.status(),
                    res.body_string()
                        .await
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        info!(url = url.as_str(), "Erase data with success");
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

#[async_trait::async_trait]
impl Middleware for Logger {
    async fn handle(
        &self,
        req: Request,
        client: Client,
        next: Next<'_>,
    ) -> std::result::Result<Response, http_types::Error> {
        let start_time = time::Instant::now();
        let uri = format!("{}", req.url());
        let method = format!("{}", req.method());
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);

        trace!(id, uri, method, "sending request");

        let res = next.run(req, client).await?;
        let status = res.status();
        let elapsed = start_time.elapsed();

        trace!(
            id,
            uri,
            method,
            elapsed = &format!("{:?}", elapsed),
            status = status.to_string().as_str(),
            "request completed"
        );

        Ok(res)
    }
}

#[derive(Debug)]
pub struct Authenticator {
    authenticator_type: Box<AuthenticatorType>,
}

impl Authenticator {
    pub fn new(authenticator_type: Box<AuthenticatorType>) -> Self {
        Authenticator { authenticator_type }
    }
}

#[async_trait::async_trait]
impl Middleware for Authenticator {
    async fn handle(
        &self,
        req: Request,
        client: Client,
        next: Next<'_>,
    ) -> std::result::Result<Response, http_types::Error> {
        let authenticator = self.authenticator_type.authenticator();

        let (auth_name, auth_value) = authenticator.authenticate().await?;
        let mut req = req.clone();
        req.set_header(
            HeaderName::from_bytes(auth_name).map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            HeaderValue::from_bytes(auth_value)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
        );

        let res = next.run(req, client).await?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use json_value_search::Search;

    use super::*;
    use crate::connector::authenticator::{basic::Basic, bearer::Bearer, AuthenticatorType};
    use crate::connector::counter::curl::CounterType;
    use crate::document::json::Json;
    use futures::StreamExt;

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
    #[async_std::test]
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
    #[async_std::test]
    async fn is_empty() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
        connector.counter_type = Some(CounterType::default());
        assert_eq!(true, connector.is_empty().await.unwrap());
        connector.path = "/get".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
    async fn fetch() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/json".to_string();
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[async_std::test]
    async fn fetch_with_basic() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/basic-auth/my-username/my-password".to_string();
        connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
            "my-username",
            "my-password",
        ))));
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[async_std::test]
    async fn fetch_with_bearer() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/bearer".to_string();
        connector.authenticator_type =
            Some(Box::new(AuthenticatorType::Bearer(Bearer::new("abcd1234"))));
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[async_std::test]
    async fn send() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Post;
        connector.path = "/post".to_string();
        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1];
        let mut datastream = connector.send(&document, &dataset).await.unwrap().unwrap();
        let value = datastream.next().await.unwrap().to_value();
        assert_eq!(
            r#"[{"column1":"value1"}]"#,
            value.search("/data").unwrap().unwrap()
        );
    }
    #[async_std::test]
    async fn erase() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
        connector.erase().await.unwrap();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
}
