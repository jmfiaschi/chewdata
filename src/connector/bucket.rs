use super::Paginator;
use crate::connector::Connector;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use regex::Regex;
use rusoto_core::credential::DefaultCredentialsProvider;
use rusoto_core::{credential::StaticProvider, Region, RusotoError};
use rusoto_s3::ListObjectsV2Request;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, PutObjectRequest, S3Client, S3 as RusotoS3};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Result, Seek, SeekFrom, Write},
};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

const DEFAULT_TAG_SERVICE_WRITER_NAME: (&str, &str) = ("service:writer:name", "chewdata");

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Bucket {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    #[serde(alias = "key")]
    pub path: String,
    #[serde(alias = "params")]
    pub parameters: Box<Value>,
    pub limit: Option<usize>,
    pub skip: usize,
    pub version: Option<String>,
    pub tags: HashMap<String, String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for Bucket {
    fn default() -> Self {
        let mut tags = HashMap::default();
        tags.insert(
            DEFAULT_TAG_SERVICE_WRITER_NAME.0.to_string(),
            DEFAULT_TAG_SERVICE_WRITER_NAME.1.to_string(),
        );

        Bucket {
            metadata: Metadata::default(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            region: rusoto_core::Region::default().name().to_string(),
            bucket: String::default(),
            path: String::default(),
            parameters: Box::new(Value::default()),
            inner: Cursor::default(),
            limit: None,
            skip: 0,
            version: None,
            tags,
            cache_control: None,
            expires: None,
        }
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut secret_access_key = self.secret_access_key.clone().unwrap_or_default();
        secret_access_key.replace_range(
            0..(secret_access_key.len() / 2),
            (0..(secret_access_key.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );
        f.debug_struct("Bucket")
            .field("metadata", &self.metadata)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &secret_access_key)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("parameters", &self.parameters)
            .field("limit", &self.limit)
            .field("skip", &self.skip)
            .field("version", &self.version)
            .field("tags", &self.tags)
            .field("cache_control", &self.cache_control)
            .field("expires", &self.expires)
            .finish()
    }
}

impl Bucket {
    fn s3_client(&self) -> Result<S3Client> {
        Ok(
            match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
                (Some(access_key_id), Some(secret_access_key)) => S3Client::new_with(
                    rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                    StaticProvider::new_minimal(
                        access_key_id.to_owned(),
                        secret_access_key.to_owned(),
                    ),
                    Region::Custom {
                        name: self.region.to_owned(),
                        endpoint: match self.endpoint.to_owned() {
                            Some(endpoint) => endpoint,
                            None => format!("https://s3-{}.amazonaws.com", self.region),
                        },
                    },
                ),
                (_, _) => S3Client::new_with(
                    rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                    DefaultCredentialsProvider::new()
                        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?,
                    Region::Custom {
                        name: self.region.to_owned(),
                        endpoint: match self.endpoint.to_owned() {
                            Some(endpoint) => endpoint,
                            None => format!("https://s3-{}.amazonaws.com", self.region),
                        },
                    },
                ),
            },
        )
    }
    fn tagging(&self) -> String {
        let mut tagging = String::default();
        let mut tags = Bucket::default().tags;
        tags.extend(self.tags.clone());

        for (k, v) in tags {
            if !tagging.is_empty() {
                tagging += &"&".to_string();
            }
            tagging += &format!("{}={}", k, v).to_string();
        }
        tagging
    }
}

#[async_trait]
impl Connector for Bucket {
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = Box::new(parameters);
    }
    /// See [`Connector::is_variable_path`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
    /// assert_eq!(false, connector.is_variable());
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_variable());
    /// ```
    fn is_variable(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{bucket::Bucket, Connector};
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
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
        actuel_path.replace_mustache(*self.parameters.clone());

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if actuel_path == new_path {
            trace!("The connector stay link to the same resource");
            return Ok(false);
        }

        info!("The connector will use another resource, regarding the new parameters");
        Ok(true)
    }
    /// See [`Connector::path`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// assert_eq!("/dir/filename_value.ext", connector.path());
    /// ```
    fn path(&self) -> String {
        match (self.is_variable(), *self.parameters.clone()) {
            (true, params) => {
                let mut path = self.path.clone();
                path.replace_mustache(params);
                path
            }
            _ => self.path.clone(),
        }
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "data/not-found-file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn len(&mut self) -> Result<usize> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::NotFound,
                "len() method not available for wildcard path.",
            ));
        }

        let s3_client = self.s3_client()?;
        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: self.path(),
            version_id: self.version.clone(),
            ..Default::default()
        };

        //TODO: When rusoto will use last version of tokio we should remove the block_on.
        let len = Runtime::new()?.block_on(async {
            match s3_client.head_object(request).await {
                Ok(response) => match response.content_length {
                    Some(len) => Ok(len as usize),
                    None => Ok(0_usize),
                },
                Err(e) => {
                    let error = format!("{:?}", e);
                    match e {
                        RusotoError::Unknown(http_response) => {
                            match http_response.status.as_u16() {
                                404 => Ok(0),
                                _ => Err(Error::new(ErrorKind::Interrupted, error)),
                            }
                        }
                        _ => Err(Error::new(ErrorKind::Interrupted, e)),
                    }
                }
            }
        })?;

        info!(len = len, "The connector found data in the resource");
        Ok(len)
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     assert_eq!(false, connector.is_empty().await?);
    ///     connector.path = "data/not_found.json".to_string();
    ///     assert_eq!(true, connector.is_empty().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&mut self) -> Result<bool> {
        Ok(0 == self.len().await?)
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{bucket::Bucket, Connector};
    /// use surf::http::Method;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.fetch().await?;
    ///     assert!(0 < connector.inner().len(), "The inner connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn fetch(&mut self) -> Result<()> {
        // Avoid to fetch two times the same data in the same connector
        if !self.inner.get_ref().is_empty() {
            return Ok(());
        }

        let connector = self.clone();
        let s3_client = connector.s3_client()?;
        let request = GetObjectRequest {
            bucket: connector.bucket.clone(),
            key: connector.path(),
            version_id: connector.version,
            ..Default::default()
        };

        //TODO: When rusoto will use last version of tokio we should remove the block_on.
        let result: Result<String> = Runtime::new()?.block_on(async {
            let response = s3_client
                .get_object(request)
                .await
                .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

            match response.body {
                Some(body) => {
                    let mut buffer = String::new();
                    let mut async_read = body.into_async_read();
                    async_read.read_to_string(&mut buffer).await?;
                    Ok(buffer)
                }
                None => Ok(String::default()),
            }
        });

        self.inner = Cursor::new(result?.as_bytes().to_vec());

        info!("The connector fetch data into the resource with success");
        Ok(())
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Example:
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use serde_json::{from_str, Value};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/out/test_bucket_send".to_string();
    ///     connector.erase().await?;
    ///
    ///     connector.write(r#"[{"column1":"value1"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///     connector_read.clear();
    ///
    ///     connector.write(r#",{"column1":"value2"}]"#.as_bytes()).await?;
    ///     connector.send(Some(-1)).await?;
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn send(&mut self, position: Option<isize>) -> Result<()> {
        if self.is_variable() && *self.parameters == Value::Null && self.inner.get_ref().is_empty()
        {
            warn!(
                path = self.path.clone().as_str(),
                parameters = self.parameters.to_string().as_str(),
                "Can't flush with variable path and without parameters"
            );
            return Ok(());
        }

        let mut content_file = Vec::default();
        let path_resolved = self.path();

        if !self.is_empty().await? {
            info!(
                path = path_resolved.to_string().as_str(),
                "Fetch existing data into S3"
            );
            {
                let mut connector_clone = self.clone();
                connector_clone.clear();
                connector_clone.fetch().await?;
                connector_clone.read_to_end(&mut content_file).await?;
            }
        }

        let mut cursor = Cursor::new(content_file.clone());

        match position {
            Some(pos) => match content_file.len() as isize + pos {
                start if start > 0 => cursor.seek(SeekFrom::Start(start as u64)),
                _ => cursor.seek(SeekFrom::Start(0)),
            },
            None => cursor.seek(SeekFrom::End(0)),
        }?;

        cursor.write_all(self.inner.get_ref())?;

        let s3_client = self.s3_client()?;
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved,
            body: Some(cursor.into_inner().into()),
            tagging: Some(self.tagging()),
            content_type: Some(self.metadata().content_type()),
            metadata: Some(self.metadata().to_hashmap()),
            cache_control: self.cache_control.to_owned(),
            content_language: match self.metadata().content_language().is_empty() {
                true => None,
                false => Some(self.metadata().content_language()),
            },
            expires: self.expires.to_owned(),
            ..Default::default()
        };

        //TODO: When rusoto will use last version of tokio we should remove the block_on.
        Runtime::new()?.block_on(async {
            match s3_client.put_object(put_request).await {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::new(ErrorKind::NotFound, e)),
            }
        })?;

        self.clear();

        info!("The connector send data into the resource with success");
        Ok(())
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::erase`] for more details.
    #[instrument]
    async fn erase(&mut self) -> Result<()> {
        let path_resolved = self.path();
        let s3_client = self.s3_client()?;
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved,
            body: Some(Vec::default().into()),
            ..Default::default()
        };

        //TODO: When rusoto will use last version of tokio we should remove the block_on.
        Runtime::new()?.block_on(async {
            match s3_client.put_object(put_request).await {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::new(ErrorKind::NotFound, e)),
            }
        })?;

        info!("The connector erase data in the resource with success");
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(BucketPaginator::new(self.clone())?))
    }
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for Bucket {
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
impl async_std::io::Write for Bucket {
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
pub struct BucketPaginator {
    pub connector: Bucket,
    pub paths: IntoIter<String>,
    pub skip: usize,
}

impl BucketPaginator {
    pub fn new(connector: Bucket) -> Result<Self> {
        let mut paths = Vec::default();

        let reg_path_contain_wildcard =
            Regex::new("[*]").map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let path = connector.path();

        match reg_path_contain_wildcard.is_match(path.as_str()) {
            true => {
                let delimiter = "/";

                let directories: Vec<&str> = path.split_terminator(delimiter).collect();
                let prefix_keys: Vec<&str> = directories
                    .clone()
                    .into_iter()
                    .take_while(|item| !item.contains('*'))
                    .collect();
                let postfix_keys: Vec<&str> = directories
                    .clone()
                    .into_iter()
                    .filter(|item| !prefix_keys.contains(item))
                    .collect();

                let key_pattern = postfix_keys
                    .join(delimiter)
                    .replace(".", "\\.")
                    .replace("*", ".*");
                let reg_key = Regex::new(key_pattern.as_str())
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

                let mut is_truncated = true;
                let mut next_token: Option<String> = None;
                while is_truncated {
                    let s3_client = connector.s3_client()?;
                    let request = ListObjectsV2Request {
                        bucket: connector.bucket.clone(),
                        delimiter: Some(delimiter.to_string()),
                        prefix: Some(format!("{}/", prefix_keys.join("/"))),
                        continuation_token: next_token,
                        ..Default::default()
                    };
                    //TODO: When rusoto will use last version of tokio we should remove the block_on.
                    let (mut paths_tmp, is_truncated_tmp, next_token_tmp) = Runtime::new()?
                        .block_on(async {
                            match s3_client.list_objects_v2(request).await {
                                Ok(response) => (
                                    response
                                        .contents
                                        .unwrap_or_default()
                                        .into_iter()
                                        .filter(|object| match object.key {
                                            Some(ref path) => reg_key.is_match(path.as_str()),
                                            None => false,
                                        })
                                        .map(|object| object.key.unwrap())
                                        .collect(),
                                    response.is_truncated.unwrap_or(false),
                                    response.next_continuation_token,
                                ),
                                Err(e) => {
                                    warn!(
                                        error = e.to_string().as_str(),
                                        "Can't fetch the list of keys"
                                    );
                                    (Vec::default(), false, None)
                                }
                            }
                        });

                    is_truncated = is_truncated_tmp;
                    next_token = next_token_tmp;
                    paths.append(&mut paths_tmp);
                }

                if let Some(limit) = connector.limit {
                    let paths_range_start = if paths.len() < connector.skip {
                        paths.len()
                    } else {
                        connector.skip
                    };
                    let paths_range_end = if paths.len() < connector.skip + limit {
                        paths.len()
                    } else {
                        connector.skip + limit
                    };

                    paths = paths[paths_range_start..paths_range_end].to_vec();
                }
            }
            false => {
                paths.append(&mut vec![path]);
            }
        }

        Ok(BucketPaginator {
            skip: connector.skip,
            paths: paths.into_iter(),
            connector,
        })
    }
}

#[async_trait]
impl Paginator for BucketPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(Some(self.paths.clone().count()))
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_none(), "Can't paginate more than one time.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: With wildcard. List results are always returned in UTF-8 binary order
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/*.json*".to_string();
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: With wildcard, limit and skip. List results are always returned in UTF-8 binary order
    /// ```rust
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/*.json*".to_string();
    ///     connector.limit = Some(5);
    ///     connector.skip = 2;
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert_eq!("data/multi_lines.jsonl".to_string(), stream.next().await.transpose()?.unwrap().path());
    ///     assert_eq!("data/one_line.json".to_string(), stream.next().await.transpose()?.unwrap().path());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = self.connector.clone();
        let mut paths = self.paths.clone();

        let stream = Box::pin(stream! {
            while let Some(path) = paths.next() {
                trace!(next_path = path.as_str(), "Next path");

                let mut new_connector = connector.clone();
                new_connector.path = path;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return the last new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("The stream stop to return new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&mut self) -> bool {
        true
    }
}
