use super::Paginator;
use crate::connector::Connector;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::{DataSet, DataStream, Metadata};
use async_compat::Compat;
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::types::DateTime;
use aws_sdk_s3::{Client, Endpoint, Region};
use json_value_merge::Merge;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::env;
use std::pin::Pin;
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Result, Seek, SeekFrom, Write},
};

const DEFAULT_TAG_SERVICE_WRITER_NAME: (&str, &str) = ("service:writer:name", "chewdata");

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Bucket {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub endpoint: String,
    pub profile: String,
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
    // in seconds since the Unix epoch
    pub expires: Option<i64>,
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
            endpoint: "http://localhost:9000".to_string(),
            profile: "default".to_string(),
            region: "us-west-2".to_string(),
            bucket: String::default(),
            path: String::default(),
            parameters: Box::new(Value::default()),
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
        futures::executor::block_on(async {
            let get_object = Compat::new(
                self.client()
                    .await
                    .unwrap()
                    .get_object()
                    .bucket(self.bucket.clone())
                    .key(self.path())
                    .set_version_id(self.version.clone())
                    .send(),
            )
            .await
            .unwrap();

            let buffer = get_object
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .to_vec();

            write!(f, "{}", String::from_utf8(buffer).unwrap())
        })
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("metadata", &self.metadata)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("profile", &self.profile)
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
    async fn client(&self) -> Result<Client> {
        if let Ok(key) = env::var("BUCKET_ACCESS_KEY_ID") {
            env::set_var("AWS_ACCESS_KEY_ID", key);
        }
        if let Ok(secret) = env::var("BUCKET_SECRET_ACCESS_KEY") {
            env::set_var("AWS_SECRET_ACCESS_KEY", secret);
        }
        let provider = CredentialsProviderChain::default_provider().await;
        let endpoint = Endpoint::immutable(
            self.endpoint
                .parse()
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
        );
        let config = aws_config::from_env()
            .endpoint_resolver(endpoint)
            .region(Region::new(self.region.clone()))
            .credentials_provider(provider)
            .load()
            .await;

        Ok(Client::new(&config))
    }
    fn tagging(&self) -> String {
        let mut tagging = String::default();
        let mut tags = Bucket::default().tags;
        tags.extend(self.tags.clone());

        for (k, v) in tags {
            if !tagging.is_empty() {
                tagging += "&";
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
    /// # Examples
    ///
    /// ```no_run
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
        self.path.has_mustache()
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Example
    ///
    /// ```no_run
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

        let mut metadata_kv = Map::default();
        metadata_kv.insert("metadata".to_string(), self.metadata().into());
        let metadata = Value::Object(metadata_kv);

        let mut new_parameters = new_parameters;
        new_parameters.merge(metadata.clone());
        let mut old_parameters = *self.parameters.clone();
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
    /// See [`Connector::path`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
        let mut path = self.path.clone();
        let mut params = *self.parameters.clone();
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
    /// See [`Connector::len`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = "http://localhost:9000".to_string();
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
        Compat::new(async {
            let reg = Regex::new("[*]").unwrap();
            if reg.is_match(self.path.as_ref()) {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "len() method not available for wildcard path.",
                ));
            }

            let len = match self
                .client()
                .await?
                .head_object()
                .key(self.path())
                .bucket(self.bucket.clone())
                .set_version_id(self.version.clone())
                .send()
                .await
            {
                Ok(res) => res.content_length() as usize,
                Err(e) => {
                    warn!(
                        error = format!("{:?}", e.to_string()).as_str(),
                        "The connector can't find the length of the document"
                    );
                    0_usize
                }
            };

            info!(len = len, "The connector found data in the resource");
            Ok(len)
        })
        .await
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{bucket::Bucket, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use async_std::stream::StreamExt;
    /// use surf::http::Method;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///     let mut connector = Bucket::default();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     let datastream = connector.fetch(document).await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn fetch(&mut self, document: Box<dyn Document>) -> Result<Option<DataStream>> {
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        let get_object = Compat::new(
            self.client()
                .await?
                .get_object()
                .bucket(self.bucket.clone())
                .key(path.clone())
                .set_version_id(self.version.clone())
                .send(),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let buffer = get_object
            .body
            .collect()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
            .into_bytes()
            .to_vec();

        info!(path = path, "The connector fetch data into the resource with success");

        if !document.has_data(&buffer)? {
            return Ok(None);
        }

        let dataset = document.read(&buffer)?;

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
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use serde_json::{from_str, Value};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/out/test_bucket_send".to_string();
    ///     connector.erase().await.unwrap();
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"[{"column1":"value1"}]"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     connector
    ///         .send(document.clone(), &dataset)
    ///         .await
    ///         .unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read
    ///         .fetch(document.clone())
    ///         .await
    ///         .unwrap()
    ///         .unwrap();
    ///     assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());
    ///     
    ///     let expected_result2 =
    ///         DataResult::Ok(serde_json::from_str(r#"[{"column1":"value2"}]"#).unwrap());
    ///     let dataset = vec![expected_result2.clone()];
    ///     connector.send(document.clone(), &dataset).await.unwrap();
    ///     
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch(document).await.unwrap().unwrap();
    ///     assert_eq!(expected_result1, datastream.next().await.unwrap());
    ///     assert_eq!(expected_result2, datastream.next().await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset))]
    async fn send(
        &mut self,
        mut document: Box<dyn Document>,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let mut content_file = Vec::default();
        let path_resolved = self.path();
        let terminator = document.terminator()?;
        let footer = document.footer(dataset)?;
        let header = document.header(dataset)?;
        let body = document.write(dataset)?;

        if path_resolved.has_mustache() {
            warn!(path = path_resolved, "This path is not fully resolved");
        }

        let position = match document.can_append() {
            true => Some(-(footer.len() as isize)),
            false => None,
        };

        if !self.is_empty().await? {
            info!(
                path = path_resolved.to_string().as_str(),
                "Fetch existing data into S3"
            );
            {
                let get_object = Compat::new(
                    self.client()
                        .await?
                        .get_object()
                        .bucket(self.bucket.clone())
                        .key(self.path())
                        .set_version_id(self.version.clone())
                        .send(),
                )
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                content_file = get_object
                    .body
                    .collect()
                    .await
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                    .into_bytes()
                    .to_vec();
            }
        }

        let file_len = content_file.len();
        let mut cursor = Cursor::new(content_file.clone());

        match position {
            Some(pos) => match file_len as isize + pos {
                start if start > 0 => cursor.seek(SeekFrom::Start(start as u64)),
                _ => cursor.seek(SeekFrom::Start(0)),
            },
            None => cursor.seek(SeekFrom::Start(0)),
        }?;

        if 0 == file_len {
            cursor.write_all(&header)?;
        }
        if 0 < file_len && file_len > (header.len() as usize + footer.len() as usize) {
            cursor.write_all(&terminator)?;
        }
        cursor.write_all(&body)?;
        cursor.write_all(&footer)?;

        let buffer = cursor.into_inner();

        Compat::new(
            self.client()
                .await?
                .put_object()
                .bucket(self.bucket.clone())
                .key(path_resolved.clone())
                .tagging(self.tagging())
                .content_type(self.metadata().content_type())
                .set_metadata(Some(self.metadata().to_hashmap()))
                .set_cache_control(self.cache_control.to_owned())
                .set_content_language(match self.metadata().content_language().is_empty() {
                    true => None,
                    false => Some(self.metadata().content_language()),
                })
                .content_length(buffer.len() as i64)
                .set_expires(self.expires.map(DateTime::from_secs))
                .body(buffer.into())
                .send(),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!(path = path_resolved, "The connector send data into the resource with success");
        Ok(None)
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
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        Compat::new(async {
            self.client()
                .await?
                .put_object()
                .bucket(self.bucket.clone())
                .key(path)
                .body(Vec::default().into())
                .send()
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            info!("The connector erase data in the resource with success");
            Ok(())
        })
        .await
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        Ok(Box::pin(BucketPaginator::new(self.clone()).await?))
    }
}

#[derive(Debug)]
pub struct BucketPaginator {
    pub connector: Bucket,
    pub paths: IntoIter<String>,
    pub skip: usize,
}

impl BucketPaginator {
    pub async fn new(connector: Bucket) -> Result<Self> {
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
                    .replace('.', "\\.")
                    .replace('*', ".*");
                let reg_key = Regex::new(key_pattern.as_str())
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

                let mut is_truncated = true;
                let mut next_token: Option<String> = None;
                while is_truncated {
                    let mut list_object_v2 = connector
                        .client()
                        .await?
                        .list_objects_v2()
                        .bucket(connector.bucket.clone())
                        .delimiter(delimiter.to_string())
                        .prefix(format!("{}/", prefix_keys.join("/")));

                    if let Some(next_token) = next_token {
                        list_object_v2 = list_object_v2.continuation_token(next_token);
                    }

                    let (mut paths_tmp, is_truncated_tmp, next_token_tmp) =
                        match Compat::new(list_object_v2.send()).await {
                            Ok(response) => (
                                response
                                    .contents()
                                    .unwrap_or_default()
                                    .iter()
                                    .filter(|object| match object.key {
                                        Some(ref path) => reg_key.is_match(path.as_str()),
                                        None => false,
                                    })
                                    .map(|object| object.key.clone().unwrap())
                                    .collect(),
                                response.is_truncated(),
                                response.next_continuation_token().map(|t| t.to_string()),
                            ),
                            Err(e) => {
                                warn!(
                                    error = e.to_string().as_str(),
                                    "Can't fetch the list of keys"
                                );
                                (Vec::default(), false, None)
                            }
                        };

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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
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
    fn is_parallelizable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::json::Json;
    use crate::DataResult;

    #[test]
    fn is_variable() {
        let mut connector = Bucket::default();
        assert_eq!(false, connector.is_variable());
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        assert_eq!(true, connector.is_variable());
    }
    #[test]
    fn is_resource_will_change() {
        let mut connector = Bucket::default();
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
        let mut connector = Bucket::default();
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        assert_eq!("/dir/filename_value.ext", connector.path());
    }
    #[async_std::test]
    async fn len() {
        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };
        assert!(
            0 < connector.len().await.unwrap(),
            "The length of the document is not greather than 0"
        );
        connector.path = "data/not-found-file".to_string();
        assert_eq!(0, connector.len().await.unwrap());
    }
    #[async_std::test]
    async fn is_empty() {
        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "data/not_found.json".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
    async fn fetch() {
        let document = Box::new(Json::default());
        let mut connector = Bucket::default();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };
        connector.path = "data/one_line.json".to_string();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero"
        );
    }
    #[async_std::test]
    async fn send() {
        let document = Box::new(Json::default());

        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/out/test_bucket_send".to_string();
        connector.erase().await.unwrap();
        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"[{"column1":"value1"}]"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        connector.send(document.clone(), &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read
            .fetch(document.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"[{"column1":"value2"}]"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(document.clone(), &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch(document).await.unwrap().unwrap();
        assert_eq!(expected_result1, datastream.next().await.unwrap());
        assert_eq!(expected_result2, datastream.next().await.unwrap());
    }
    #[async_std::test]
    async fn paginator_stream() {
        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        let mut paginator = connector.paginator().await.unwrap();
        assert!(paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            stream.next().await.transpose().unwrap().is_none(),
            "Can't paginate more than one time."
        );
    }
    #[async_std::test]
    async fn paginator_stream_with_wildcard() {
        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json*".to_string();
        let mut paginator = connector.paginator().await.unwrap();
        assert!(paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the second reader."
        );
    }
    #[async_std::test]
    async fn paginator_stream_with_wildcard_limit_skip() {
        let mut connector = Bucket::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json*".to_string();
        connector.limit = Some(5);
        connector.skip = 2;
        let mut paginator = connector.paginator().await.unwrap();
        assert!(paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert_eq!(
            "data/multi_lines.jsonl".to_string(),
            stream.next().await.transpose().unwrap().unwrap().path()
        );
        assert_eq!(
            "data/one_line.json".to_string(),
            stream.next().await.transpose().unwrap().unwrap().path()
        );
    }
}
