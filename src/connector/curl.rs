use super::authenticator::AuthenticatorType;
use super::{Connector, Paginator};
use crate::document::{Document, DocumentType};
use crate::helper::mustache::Mustache;
use crate::{DataSet, DataStream, Metadata};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use http_types::headers::HeaderName;
use http_types::headers::HeaderValue;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::convert::TryInto;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::time::Duration;
use std::{collections::HashMap, fmt};
use surf::{
    http::{headers, Method, Url},
    Client,
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
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
            timeout: Some(5),
            keepalive: true,
            tcp_nodelay: false,
            parameters: Value::Null,
            paginator_type: PaginatorType::default(),
            counter_type: None,
        }
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
            .field("timeout", &self.timeout)
            .field("keepalive", &self.keepalive)
            .field("tcp_nodelay", &self.tcp_nodelay)
            .field("parameters", &self.parameters)
            .field("paginator_type", &self.paginator_type)
            .finish()
    }
}

impl Curl {
    async fn client(&mut self) -> std::io::Result<Client> {
        let mut config = surf::Config::new()
            .set_base_url(
                Url::parse(self.endpoint.as_str())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            )
            .set_timeout(self.timeout.map(Duration::from_secs))
            .set_http_keep_alive(self.keepalive)
            .set_tcp_no_delay(self.tcp_nodelay);

        if let Some(ref mut authenticator_type) = self.authenticator_type {
            let authenticator = authenticator_type.authenticator_mut();
            let (auth_name, auth_value) =
                authenticator.authenticate(self.parameters.clone()).await?;
            config = config
                .add_header(
                    HeaderName::from_bytes(auth_name)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                    HeaderValue::from_bytes(auth_value)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        }

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

        let client: Client = config
            .try_into()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        Ok(client)
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
        let mut params = self.parameters.clone();
        let mut metadata = Map::default();

        match self.is_variable() {
            true => {
                metadata.insert("metadata".to_string(), self.metadata().into());
                params.merge(Value::Object(metadata));

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
            trace!("The connector stay link to the same resource");
            return Ok(false);
        }

        let mut metadata_kv = Map::default();
        metadata_kv.insert("metadata".to_string(), self.metadata().into());
        let metadata = Value::Object(metadata_kv);

        let mut new_parameters = new_parameters;
        new_parameters.merge(metadata.clone());
        let mut old_parameters = self.parameters.clone();
        old_parameters.merge(metadata);

        let mut previous_path = self.path.clone();
        previous_path.replace_mustache(old_parameters);

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if previous_path == new_path {
            trace!(path = previous_path, "The connector path didn't change");
            return Ok(false);
        }

        info!(
            previous_path = previous_path,
            new_path = new_path,
            "The connector will use another resource regarding the new parameters"
        );
        Ok(true)
    }
    /// See [`Connector::is_variable_path`] for more details.
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
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn len(&mut self) -> Result<usize> {
        let client = self.client().await?;
        let url = Url::parse(format!("{}{}", self.endpoint, self.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, url);

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        let res = client
            .send(req.build())
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

        info!(
            len = content_length,
            "The connector found data in the resource"
        );
        Ok(content_length)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use async_std::stream::StreamExt;
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
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn fetch(&mut self, document: &dyn Document) -> std::io::Result<Option<DataStream>> {
        let client = self.client().await?;
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, url);

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

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
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8(data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        info!(
            path = path,
            "The connector fetch data into the resource with success"
        );

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
    #[instrument(skip(dataset))]
    async fn send(
        &mut self,
        document: &dyn Document,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let client = self.client().await?;
        let mut buffer = Vec::default();
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        buffer.append(&mut document.header(dataset)?);
        buffer.append(&mut document.write(dataset)?);
        buffer.append(&mut document.footer(dataset)?);

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, url).body(buffer.clone());

        // Force to replace the `application/octet-stream` by the connector content type.
        if !self.metadata().content_type().is_empty() {
            req = req.header(headers::CONTENT_TYPE, self.metadata().content_type());
        }

        req = req.header(headers::CONTENT_LENGTH, buffer.len().to_string());

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

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
                    "Curl failed with status code '{}' and response body: {}",
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

        info!(
            path = path,
            "The connector send data into the resource with success"
        );
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
    #[instrument]
    async fn erase(&mut self) -> Result<()> {
        let client = self.client().await?;
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        let url = Url::parse(format!("{}{}", self.endpoint, path).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut req = client.request(self.method, url);

        // Force the headers
        for (key, value) in self.headers.iter() {
            req = req.header(
                HeaderName::from_bytes(key.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                HeaderValue::from_bytes(value.clone().into_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        let mut res = client
            .send(req.build())
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

        info!(
            path = path,
            "The connector erase data in the resource with success"
        );
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        let paginator = match self.paginator_type {
            PaginatorType::Offset(ref offset_paginator) => {
                let mut offset_paginator = offset_paginator.clone();
                offset_paginator.set_connector(self.clone());

                Box::new(offset_paginator) as Box<dyn Paginator + Send + Sync>
            }
            PaginatorType::Cursor(ref cursor_paginator) => {
                let mut cursor_paginator = cursor_paginator.clone();
                cursor_paginator.set_connector(self.clone());

                Box::new(cursor_paginator) as Box<dyn Paginator + Send + Sync>
            }
        };

        Ok(Pin::new(paginator))
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
    /// Get the number of items from the header. Return None if the counter can't count.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    #[instrument]
    pub async fn count(&self, connector: Curl) -> Result<Option<usize>> {
        let mut connector = connector.clone();
        let client = connector.client().await?;

        if let Some(path) = self.path.clone() {
            connector.path = path;
        }

        let url = Url::parse(format!("{}{}", connector.endpoint, connector.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let res = client
            .head(url)
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
                trace!(
                    size = count,
                    "The counter count elements in the resource with success"
                );
                Some(count)
            }
            Err(_) => {
                trace!("The counter can't count elements in the resource");
                None
            }
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
    /// Get the number of items from the response body. Return None if the counter can't count.
    ///
    /// # Examples
    ///
    /// ```no_run
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

        let mut dataset = match connector.fetch(&*document).await? {
            Some(dataset) => dataset,
            None => {
                trace!("No data found");
                return Ok(None);
            }
        };

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

        trace!(
            size = count,
            "The counter count elements in the resource with success"
        );
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
#[serde(default, deny_unknown_fields)]
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
    /// # Examples
    ///
    /// ```no_run
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

            info!(
                size = self.count,
                "The connector's counter count elements in the resource with success"
            );
            return Ok(self.count);
        }

        trace!(size = self.count, "The connector's counter not exist or can't count the number of elements in the resource");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    ///
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let mut paginator = self.clone();
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = paginator.limit;
        let mut skip = paginator.skip;

        let count_opt = match paginator.count {
            Some(count) => Some(count),
            None => paginator.count().await?,
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
    fn is_parallelizable(&self) -> bool {
        self.count.is_some()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct CursorPaginator {
    pub limit: usize,
    // The entry path to catch the value in the body
    pub entry_path: String,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    pub document_type: DocumentType,
    #[serde(skip)]
    pub connector: Option<Box<Curl>>,
    #[serde(rename = "next")]
    pub next_token: Option<String>,
}

impl Default for CursorPaginator {
    fn default() -> Self {
        CursorPaginator {
            limit: 100,
            connector: None,
            document_type: DocumentType::default(),
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
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::{Curl, PaginatorType, CursorPaginator}, Connector};
    /// use chewdata::document::{DocumentType, json::Json};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/uuid?next={{ paginator.next }}".to_string();
    ///     connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
    ///         limit: 1,
    ///         entry_path: "/uuid".to_string(),
    ///         document_type: DocumentType::default(),
    ///         ..Default::default()
    ///     });
    ///     let paginator = connector.paginator().await?;
    ///     let mut stream = paginator.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut document = self.document_type.clone().boxed_inner();
        let mut has_next = true;
        let limit = self.limit;
        let entry_path = self.entry_path.clone();
        let mut next_token_opt = self.next_token.clone();

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();

                if let Some(next_token) = next_token_opt {
                    new_parameters.merge_in("/paginator/next", Value::String(next_token))?;
                }

                new_parameters
                    .merge_in("/paginator/limit", Value::String(limit.to_string()))?;

                document.set_entry_path(entry_path.clone());

                let mut dataset = match new_connector.fetch(&*document).await? {
                    Some(dataset) => dataset,
                    None => break
                };

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
    fn is_parallelizable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::authenticator::{basic::Basic, bearer::Bearer, AuthenticatorType};
    use crate::document::json::Json;
    #[cfg(feature = "xml")]
    use crate::document::xml::Xml;
    use crate::DataResult;
    use json_value_search::Search;

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
        assert!(
            0 == connector.len().await.unwrap(),
            "The remote document should have a length equal to zero"
        );
        connector.path = "/get".to_string();
        assert!(
            0 != connector.len().await.unwrap(),
            "The remote document should have a length different than zero"
        );
    }
    #[async_std::test]
    async fn is_empty() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.path = "/status/200".to_string();
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
            "The inner connector should have a size upper than zero"
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
            "The inner connector should have a size upper than zero"
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
            "The inner connector should have a size upper than zero"
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
    #[async_std::test]
    async fn paginator_header_counter_count() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        let mut counter = HeaderCounter::default();
        counter.name = "Content-Length".to_string();
        assert_eq!(Some(194), counter.count(connector).await.unwrap());
    }
    #[async_std::test]
    async fn paginator_header_counter_count_none() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        let mut counter = HeaderCounter::default();
        counter.name = "not_found".to_string();
        assert_eq!(None, counter.count(connector).await.unwrap());
    }
    #[async_std::test]
    async fn paginator_body_counter_count() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Post;
        connector.path = "/anything?count=10".to_string();
        let mut counter = BodyCounter::default();
        counter.entry_path = "/args/test".to_string();
        assert_eq!(
            None,
            counter
                .count(connector, Box::new(Json::default()))
                .await
                .unwrap()
        );
    }
    #[async_std::test]
    async fn paginator_offset_count() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        connector.paginator_type = PaginatorType::Offset(OffsetPaginator::default());
        connector.counter_type = Some(CounterType::Header(HeaderCounter::new(
            "Content-Length".to_string(),
            None,
        )));
        let mut paginator = connector.paginator().await.unwrap();
        assert_eq!(Some(194), paginator.count().await.unwrap());
    }
    #[cfg(feature = "xml")]
    #[async_std::test]
    async fn paginator_offset_stream() {
        let mut document = Xml::default();
        document.entry_path = "/html/body/*/a".to_string();

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();
        connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
            skip: 1,
            limit: 1,
            ..Default::default()
        });
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();

        let mut connector = stream.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/1/10", connector.path().as_str());
        let len1 = connector
            .fetch(&document)
            .await
            .unwrap()
            .unwrap()
            .count()
            .await;
        assert!(0 < len1, "Can't read the content of the file.");

        let mut connector = stream.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/2/10", connector.path().as_str());
        let len2 = connector
            .fetch(&document)
            .await
            .unwrap()
            .unwrap()
            .count()
            .await;
        assert!(0 < len2, "Can't read the content of the file.");

        assert!(
            len1 != len2,
            "The content of this two files is not different."
        );
    }
    #[async_std::test]
    async fn paginator_offset_stream_one_time() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
    #[async_std::test]
    async fn paginator_offset_stream_tree_times_and_parallize() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();
        connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
            skip: 0,
            limit: 1,
            count: Some(3),
            ..Default::default()
        });
        let paginator = connector.paginator().await.unwrap();
        assert!(paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
    #[async_std::test]
    async fn paginator_cursor_stream() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/uuid?next={{ paginator.next }}".to_string();
        connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
            limit: 1,
            entry_path: "/uuid".to_string(),
            document_type: DocumentType::default(),
            ..Default::default()
        });

        let document = Json::default();

        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let mut datastream = connector.unwrap().fetch(&document).await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();

        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let mut datastream = connector.unwrap().fetch(&document).await.unwrap().unwrap();
        let data_2 = datastream.next().await.unwrap();

        assert!(
            data_1 != data_2,
            "The content of this two stream are not different."
        );
    }
}
