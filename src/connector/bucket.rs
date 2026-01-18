//! Read and write data into S3/Minio bucket.
//!
//! ### Configuration
//!
//! | key               | alias  | Description                                                            | Default Value                    | Possible Values            |
//! | ----------------- | ------ | ---------------------------------------------------------------------- | -------------------------------- | -------------------------- |
//! | type              | -      | Required in order to use this connector                                | `bucket`                         | `bucket`                   |
//! | metadata          | meta   | Override metadata information                                          | `null`                           | [`crate::Metadata`]      |
//! | endpoint          | -      | Endpoint of the connector                                              | `null`                           | String                     |
//! | access_key_id     | -      | The access key used for the authentification                           | `null`                           | String                     |
//! | secret_access_key | -      | The secret access key used for the authentification                    | `null`                           | String                     |
//! | region            | -      | The bucket's region                                                    | `us-east-1`                      | String                     |
//! | bucket            | -      | The bucket name                                                        | `null`                           | String                     |
//! | path              | key    | The path of the resource. Can use `*` in order to read multiple files  | `null`                           | String                     |
//! | parameters        | params | The parameters used to remplace variables in the path                  | `null`                           | Object or Array of objects |
//! | limit             | -      | Limit the number of files to read.                                     | `null`                           | Unsigned number            |
//! | skip              | -      | Skip N files before to start to read the next files                    | `null`                           | Unsigned number            |
//! | version           | -      | Read a specific version of a file                                      | `null`                           | String                     |
//! | tags              | -      | List of tags to apply on the file. Used to give more context to a file | `(service:writer:name,chewdata)` | List of Key/Value          |
//! | cache_control     | -      | Override the file cache controle                                       | `null`                           | String                     |
//! | expires           | -      | Override the file expire date. In seconds since the Unix epoch                                          | `null`                           | String                     |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "r",
//!         "connector": {
//!             "type": "bucket",
//!             "bucket": "my-bucket",
//!             "path": "data/*.json*",
//!             "endpoint":"{{ BUCKET_ENDPOINT }}",
//!             "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
//!             "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
//!             "region": "{{ BUCKET_REGION }}",
//!             "limit": 10,
//!             "skip": 0,
//!             "tags": {
//!                 "service:writer": "my_service",
//!                 "service:writer:owner": "my_team_name",
//!                 "service:writer:env": "dev",
//!                 "service:writer:context": "example"
//!             }
//!         }
//!     },
//! ]
//! ```
use crate::connector::Connector;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{ConnectorStream, DataSet, DataStream, Metadata};
use async_compat::CompatExt;
use async_lock::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::Client;
use json_value_merge::Merge;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Result, Seek, SeekFrom, Write},
};

static CLIENTS: OnceLock<Arc<Mutex<HashMap<String, Client>>>> = OnceLock::new();

const DEFAULT_TAG_SERVICE_WRITER_NAME: (&str, &str) = ("service:writer:name", "chewdata");
const DEFAULT_REGION: &str = "us-west-2";
const DEFAULT_ENDPOINT: &str = "http://localhost:9000";

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Bucket {
    #[serde(skip)]
    document: Option<Box<dyn Document>>,
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub endpoint: Option<String>,
    pub profile: String,
    pub region: Option<String>,
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
    pub expires: Option<i64>,
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("document", &self.document)
            .field("metadata", &self.metadata)
            .field("endpoint", &self.endpoint)
            .field("profile", &self.profile)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("parameters", &self.parameters.display_only_for_debugging())
            .field("limit", &self.limit)
            .field("skip", &self.skip)
            .field("version", &self.version)
            .field("tags", &self.tags)
            .field("cache_control", &self.cache_control)
            .field("expires", &self.expires)
            .finish()
    }
}

impl Default for Bucket {
    fn default() -> Self {
        let mut tags = HashMap::default();
        tags.insert(
            DEFAULT_TAG_SERVICE_WRITER_NAME.0.to_string(),
            DEFAULT_TAG_SERVICE_WRITER_NAME.1.to_string(),
        );

        Bucket {
            document: None,
            metadata: Metadata::default(),
            endpoint: None,
            profile: "default".to_string(),
            region: None,
            bucket: String::default(),
            path: String::default(),
            parameters: Box::<Value>::default(),
            limit: None,
            skip: 0,
            version: None,
            tags,
            cache_control: None,
            expires: None,
        }
    }
}

impl Bucket {
    fn region(&self) -> String {
        match (
            self.region.clone(),
            env::var("BUCKET_ACCESS_KEY_ID"),
            env::var("AWS_DEFAULT_REGION"),
        ) {
            (Some(region), _, _) => region,
            (None, Ok(region), _) => region,
            (None, Err(_), Ok(region)) => region,
            (None, Err(_), Err(_)) => DEFAULT_REGION.to_string(),
        }
    }
    fn endpoint(&self) -> String {
        match (
            self.endpoint.clone(),
            env::var("BUCKET_ENDPOINT"),
            env::var("AWS_ENDPOINT_URL_S3"),
        ) {
            (Some(endpoint), _, _) => endpoint,
            (None, Ok(endpoint), _) => endpoint,
            (None, Err(_), Ok(endpoint)) => endpoint,
            (None, Err(_), Err(_)) => DEFAULT_ENDPOINT.to_string(),
        }
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
    fn client_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let client_key = format!("{}:{}", self.endpoint(), self.region());
        client_key.hash(&mut hasher);
        hasher.finish().to_string()
    }
    /// Get the current client
    pub async fn client(&self) -> Result<Client> {
        let clients = CLIENTS.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let client_key = self.client_key();
        if let Some(client) = clients.lock().await.get(&self.client_key()) {
            trace!(client_key, "Retrieve the previous client");
            return Ok(client.clone());
        }

        trace!(client_key, "Create a new client");

        if let Ok(key) = env::var("BUCKET_ACCESS_KEY_ID") {
            env::set_var("AWS_ACCESS_KEY_ID", key);
        }
        if let Ok(secret) = env::var("BUCKET_SECRET_ACCESS_KEY") {
            env::set_var("AWS_SECRET_ACCESS_KEY", secret);
        }

        let provider = CredentialsProviderChain::default_provider().await;
        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(self.endpoint())
            .region(Region::new(self.region()))
            .credentials_provider(provider)
            .force_path_style(true);

        let mut map = clients.lock_arc().await;
        let client = Client::from_conf(config.build());
        map.insert(client_key, client.clone());

        Ok(client)
    }
}

#[async_trait]
impl Connector for Bucket {
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
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        *self.parameters = parameters
    }
    /// See [`Connector::is_variable`] for more details.
    ///
    /// # Examples
    ///
    /// ```
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
    /// ```
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
            trace!("Stay link to the same resource");
            return Ok(false);
        }

        let mut metadata_kv = Map::default();
        metadata_kv.insert("metadata".to_string(), self.metadata().into());
        let metadata = Value::Object(metadata_kv);

        let mut new_parameters = new_parameters;
        new_parameters.merge(&metadata);
        let mut old_parameters = *self.parameters.clone();
        old_parameters.merge(&metadata);

        let mut previous_path = self.path.clone();
        previous_path.replace_mustache(old_parameters);

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if previous_path == new_path {
            trace!(path = previous_path, "Path didn't change");
            return Ok(false);
        }

        info!(
            previous_path = previous_path,
            new_path = new_path,
            "Will use another resource regarding the new parameters"
        );
        Ok(true)
    }
    /// See [`Connector::path`] for more details.
    ///
    /// # Examples
    ///
    /// ```
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

        match self.is_variable() {
            true => {
                let mut params = *self.parameters.clone();
                let mut metadata = Map::default();

                metadata.insert("metadata".to_string(), self.metadata().into());
                params.merge(&Value::Object(metadata));

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
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "data/not-found-file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket::len")]
    async fn len(&self) -> Result<usize> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::NotFound,
                "len() method not available for wildcard path",
            ));
        }

        let len = match self
            .client()
            .compat()
            .await?
            .head_object()
            .key(self.path())
            .bucket(&self.bucket)
            .set_version_id(self.version.clone())
            .send()
            .compat()
            .await
        {
            Ok(res) => match res.content_length() {
                Some(content_length) => content_length as usize,
                None => 0_usize,
            },
            Err(e) => {
                warn!(
                    error = format!("{:?}", e.to_string()).as_str(),
                    "Can't find the length of the resource"
                );
                0_usize
            }
        };

        info!(len, "Find length of the resource");

        Ok(len)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::{bucket::Bucket, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use smol::stream::StreamExt;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///     let mut connector = Bucket::default();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.set_document(document);
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket::fetch")]
    async fn fetch(&mut self) -> Result<Option<DataStream>> {
        let document = self.document()?;
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let get_object = self
            .client()
            .compat()
            .await?
            .get_object()
            .bucket(&self.bucket)
            .key(&path)
            .set_version_id(self.version.clone())
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let buffer = get_object
            .body
            .collect()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?
            .into_bytes()
            .to_vec();

        info!(path = path, "Fetch data with success");

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
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use serde_json::{from_str, Value};
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Bucket::default();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/out/test_bucket_send".to_string();
    ///     connector.erase().await.unwrap();
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     connector.set_document(Box::new(document)).unwrap();
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());
    ///
    ///     let expected_result2 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
    ///     let dataset = vec![expected_result2.clone()];
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1, datastream.next().await.unwrap());
    ///     assert_eq!(expected_result2, datastream.next().await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "bucket::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let document = self.document()?;
        let mut content_file = Vec::default();
        let path = self.path();
        let terminator = document.terminator()?;
        let footer = document.footer(dataset)?;
        let header = document.header(dataset)?;
        let body = document.write(dataset)?;

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let position = match document.can_append() {
            true => Some(-(footer.len() as isize)),
            false => None,
        };

        if !self.is_empty().await? {
            info!(path = path.to_string().as_str(), "Fetch existing data");
            {
                let get_object = self
                    .client()
                    .compat()
                    .await?
                    .get_object()
                    .bucket(&self.bucket)
                    .key(self.path())
                    .set_version_id(self.version.clone())
                    .send()
                    .compat()
                    .await
                    .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                content_file = get_object
                    .body
                    .collect()
                    .compat()
                    .await
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                    .into_bytes()
                    .to_vec();
            }
        }

        let file_len = content_file.len();
        let mut cursor = Cursor::new(content_file);

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
        if 0 < file_len && file_len > (header.len() + footer.len()) {
            cursor.write_all(&terminator)?;
        }
        cursor.write_all(&body)?;
        cursor.write_all(&footer)?;

        let buffer = cursor.into_inner();

        self.client()
            .compat()
            .await?
            .put_object()
            .bucket(&self.bucket)
            .key(&path)
            .tagging(self.tagging())
            .content_type(self.metadata().content_type())
            .set_metadata(Some(
                self.metadata()
                    .to_hashmap()
                    .into_iter()
                    .map(|(key, value)| (key, value.replace('\n', "\\n")))
                    .collect(),
            ))
            .set_cache_control(self.cache_control.to_owned())
            .set_content_language(match self.metadata().content_language().is_empty() {
                true => None,
                false => Some(self.metadata().content_language()),
            })
            .content_length(buffer.len() as i64)
            .set_expires(self.expires.map(DateTime::from_secs))
            .body(buffer.into())
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!(path = path, "Send data with success");
        Ok(None)
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
    }
    /// See [`Connector::erase`] for more details.
    #[instrument(name = "bucket::erase")]
    async fn erase(&mut self) -> Result<()> {
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        self.client()
            .compat()
            .await?
            .put_object()
            .bucket(self.bucket.clone())
            .key(path)
            .body(Vec::default().into())
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!("Erase data with success");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        BucketPaginator::new(self).await?.paginate(self).await
    }
}

#[derive(Debug)]
pub struct BucketPaginator {
    pub paths: IntoIter<String>,
    pub skip: usize,
}

impl BucketPaginator {
    pub async fn new(connector: &Bucket) -> Result<Self> {
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
                        .compat()
                        .await?
                        .list_objects_v2()
                        .bucket(&connector.bucket)
                        .delimiter(delimiter.to_string())
                        .prefix(format!("{}/", prefix_keys.join("/")));

                    if let Some(next_token) = next_token {
                        list_object_v2 = list_object_v2.continuation_token(next_token);
                    }

                    let (mut paths_tmp, is_truncated_tmp, next_token_tmp) =
                        match list_object_v2.send().compat().await {
                            Ok(response) => (
                                response
                                    .contents()
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
                                (Vec::default(), Some(false), None)
                            }
                        };

                    is_truncated = is_truncated_tmp.unwrap_or(false);
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
        })
    }
}

impl BucketPaginator {
    /// Paginate through the bucket folder.
    /// Wildcard is allowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::bucket::{Bucket, BucketPaginator};
    /// use chewdata::connector::Connector;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Bucket::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///
    ///     let paginator = BucketPaginator::new(&connector).await?;
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(paging.next().await.transpose()?.is_none(), "Should not have more readers.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket::paginate")]
    pub async fn paginate(&self, connector: &Bucket) -> Result<ConnectorStream> {
        let mut paths = self.paths.clone();
        let connector = connector.clone();

        Ok(Box::pin(stream! {
            for path in &mut paths {
                trace!(next_path = path.as_str(), "Next path");

                let mut new_connector = connector.clone();
                new_connector.path = path;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream yields a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("The stream stops yielding new connectors");
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::json::Json;
    use crate::DataResult;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

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
    #[apply(test!)]
    async fn len() {
        let mut connector = Bucket::default();
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
    #[apply(test!)]
    async fn is_empty() {
        let mut connector = Bucket::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "data/not_found.json".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn fetch() {
        let document = Json::default();
        let mut connector = Bucket::default();
        connector.path = "data/one_line.json".to_string();
        connector.bucket = "my-bucket".to_string();
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

        let mut connector = Bucket::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/out/test_bucket_send".to_string();
        connector.erase().await.unwrap();
        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        connector.set_document(Box::new(document)).unwrap();
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1, datastream.next().await.unwrap());
        assert_eq!(expected_result2, datastream.next().await.unwrap());
    }
    #[apply(test!)]
    async fn paginator_paginate() {
        let document = Json::default();
        let mut connector = Bucket::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let mut paging = connector.paginate().await.unwrap();

        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            paging.next().await.transpose().unwrap().is_none(),
            "Can't paginate more than one time."
        );
    }
    #[apply(test!)]
    async fn paginator_paginate_with_wildcard() {
        let document = Json::default();
        let mut connector = Bucket::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json*".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let mut paging = connector.paginate().await.unwrap();

        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the second reader."
        );
    }
    #[apply(test!)]
    async fn paginator_paginate_with_wildcard_limit_skip() {
        let document = Json::default();
        let mut connector = Bucket::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json*".to_string();
        connector.limit = Some(5);
        connector.skip = 2;
        connector.set_document(Box::new(document)).unwrap();

        let mut paging = connector.paginate().await.unwrap();

        assert_eq!(
            "data/multi_lines.jsonl".to_string(),
            paging.next().await.transpose().unwrap().unwrap().path()
        );
        assert_eq!(
            "data/one_line.json".to_string(),
            paging.next().await.transpose().unwrap().unwrap().path()
        );
    }
}
