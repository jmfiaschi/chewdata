//! Filter data file with S3 select queries and read data into AWS/Minio bucket.
//! Use Bucket connector in order to write into the bucket.
//!
//! ### Configuration
//!
//! | key               | alias  | Description                                                                                      | Default Value            | Possible Values                                                                                                        |
//! | ----------------- | ------ | ------------------------------------------------------------------------------------------------ | ------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
//! | type              | -      | Required in order to use this connector                                                          | `bucket`                 | `bucket`                                                                                                               |
//! | metadata          | meta   | Override metadata information                                                                    | `null`                   | [`crate::Metadata`]                                                                                                  |
//! | endpoint          | -      | Endpoint of the connector                                                                        | `null`                   | String                                                                                                                 |
//! | access_key_id     | -      | The access key used for the authentification                                                     | `null`                   | String                                                                                                                 |
//! | secret_access_key | -      | The secret access key used for the authentification                                              | `null`                   | String                                                                                                                 |
//! | region            | -      | The bucket's region                                                                              | `us-east-1`              | String                                                                                                                 |
//! | bucket            | -      | The bucket name                                                                                  | `null`                   | String                                                                                                                 |
//! | path              | key    | The path of the resource. Can use `*` in order to read multiple files with the same content type | `null`                   | String                                                                                                                 |
//! | parameters        | params | The parameters used to remplace variables in the path                                            | `null`                   | Object or Array of objects                                                                                             |
//! | query             | -      | S3 select query                                                                                  | `select * from s3object` | See [AWS S3 select](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-select.html) |
//! | limit             | -      | Limit the number of files to read with the wildcard mode in the path                             | `null`                   | Unsigned number                                                                                                        |
//! | skip              | -      | Skip N files before to start to read the next files with the wildcard mode in the path           | `null`                   | Unsigned number                                                                                                        |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "r",
//!         "connector": {
//!             "type": "bucket_select",
//!             "bucket": "my-bucket",
//!             "path": "data/my_file.jsonl",
//!             "endpoint": "{{ BUCKET_ENDPOINT }}",
//!             "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
//!             "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
//!             "region": "{{ BUCKET_REGION }}",
//!             "query": "select * from s3object[*].results[*] r where r.number = 20"
//!         },
//!         "document" : {
//!             "type": "jsonl"
//!         }
//!     }
//! ]
//! ```
use super::bucket::{Bucket, BucketPaginator};
use super::Paginator;
use crate::connector::Connector;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::{DataSet, DataStream, Metadata};
use async_compat::CompatExt;
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::select_object_content::builders::SelectObjectContentFluentBuilder;
use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    JsonInput, JsonOutput, JsonType, OutputSerialization, ParquetInput,
    SelectObjectContentEventStream,
};
use aws_sdk_s3::Client;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::env;
use std::pin::Pin;
use std::time::Duration;
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct BucketSelect {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub endpoint: String,
    pub profile: String,
    pub region: String,
    pub bucket: String,
    #[serde(alias = "key")]
    pub path: String,
    pub query: String,
    #[serde(alias = "params")]
    pub parameters: Box<Value>,
    pub limit: Option<usize>,
    pub skip: usize,
    pub timeout: Option<Duration>,
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            metadata: Metadata::default(),
            query: "select * from s3object".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            profile: "default".to_string(),
            region: "us-west-2".to_string(),
            bucket: String::default(),
            path: String::default(),
            parameters: Box::<Value>::default(),
            timeout: None,
            limit: None,
            skip: 0,
        }
    }
}

impl fmt::Display for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        futures::executor::block_on(async {
            let get_object = self
                .client()
                .compat()
                .await
                .unwrap()
                .get_object()
                .bucket(self.bucket.clone())
                .key(self.path())
                .send()
                .compat()
                .await
                .unwrap();

            let buffer = get_object
                .body
                .collect()
                .compat()
                .await
                .unwrap()
                .into_bytes()
                .to_vec();

            write!(f, "{}", String::from_utf8(buffer).unwrap())
        })
    }
}

impl fmt::Debug for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketSelect")
            .field("metadata", &self.metadata)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("profile", &self.profile)
            .field("path", &self.path)
            .field("parameters", &self.parameters)
            .field("limit", &self.limit)
            .field("skip", &self.skip)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl BucketSelect {
    async fn client(&self) -> Result<Client> {
        if let Ok(key) = env::var("BUCKET_ACCESS_KEY_ID") {
            env::set_var("AWS_ACCESS_KEY_ID", key);
        }
        if let Ok(secret) = env::var("BUCKET_SECRET_ACCESS_KEY") {
            env::set_var("AWS_SECRET_ACCESS_KEY", secret);
        }
        let provider = CredentialsProviderChain::default_provider().await;
        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(&self.endpoint)
            .region(Region::new(self.region.clone()))
            .credentials_provider(provider)
            .force_path_style(true)
            .build();

        Ok(Client::from_conf(config))
    }
    /// Get a Select object Content Request object with a BucketSelect connector.
    pub async fn select_object_content(&self) -> Result<SelectObjectContentFluentBuilder> {
        let metadata = self.metadata();
        let path = self.path();

        if path.has_mustache() {
            warn!(path = path, "This path is not fully resolved");
        }

        let input_serialization = match metadata.mime_subtype.as_deref() {
            Some("csv") => InputSerialization::builder().csv(
                CsvInput::builder()
                    .set_field_delimiter(metadata.clone().delimiter)
                    .file_header_info(match metadata.has_headers {
                        Some(true) => FileHeaderInfo::Use,
                        Some(false) => FileHeaderInfo::None,
                        _ => FileHeaderInfo::Use,
                    })
                    .set_quote_character(metadata.clone().quote)
                    .set_quote_escape_character(metadata.clone().escape)
                    .build(),
            ),
            Some("octet-stream") | Some("parquet") => {
                InputSerialization::builder().parquet(ParquetInput::builder().build())
            }
            Some("json") => InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Document).build()),
            Some("x-ndjson") => InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build()),
            _ => InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build()),
        }
        .compression_type(CompressionType::from(
            metadata
                .compression
                .unwrap_or_else(|| "NONE".to_string())
                .as_str(),
        ))
        .build();

        let output_serialization = match metadata.mime_subtype.as_deref() {
            Some("csv") => OutputSerialization::builder().csv(
                CsvOutput::builder()
                    .set_field_delimiter(metadata.delimiter)
                    .set_quote_character(metadata.quote)
                    .set_quote_escape_character(metadata.escape)
                    .record_delimiter(
                        match metadata
                            .terminator
                            .unwrap_or_else(|| "\n".to_string())
                            .as_str()
                        {
                            "CRLF" => "\n\r".to_string(),
                            "CR" => "\n".to_string(),
                            terminal => terminal.to_string(),
                        },
                    )
                    .build(),
            ),
            _ => OutputSerialization::builder().json(
                JsonOutput::builder()
                    .set_record_delimiter(metadata.delimiter)
                    .build(),
            ),
        }
        .build();

        Ok(self
            .client()
            .await?
            .select_object_content()
            .bucket(self.bucket.clone())
            .key(path)
            .expression(self.query.clone())
            .expression_type(ExpressionType::Sql)
            .input_serialization(input_serialization)
            .output_serialization(output_serialization))
    }
    async fn fetch_data(&mut self) -> Result<Vec<u8>> {
        let mut event_stream = self
            .select_object_content()
            .compat()
            .await?
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;

        let mut buffer = Vec::default();

        while let Some(event) = event_stream
            .payload
            .recv()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?
        {
            match event {
                SelectObjectContentEventStream::Records(records) => {
                    trace!("records Event");
                    if let Some(bytes) = records.payload() {
                        buffer.append(&mut bytes.clone().into_inner());
                    };
                }
                SelectObjectContentEventStream::Stats(stats) => {
                    trace!(
                        stats = format!("{:?}", stats.details()).as_str(),
                        "Stats Event"
                    );
                }
                SelectObjectContentEventStream::End(_) => {
                    trace!("End Event");
                    break;
                }
                SelectObjectContentEventStream::Progress(progress) => {
                    trace!(
                        details = format!("{:?}", progress.details()).as_str(),
                        "Progress Event"
                    );
                }
                SelectObjectContentEventStream::Cont(_) => {
                    trace!("Continuation Event");
                }
                otherwise => {
                    return Err(Error::new(
                        ErrorKind::Interrupted,
                        format!("{:?}", otherwise),
                    ))
                }
            }
        }

        Ok(buffer)
    }
    async fn fetch_length(&mut self) -> Result<usize> {
        let mut event_stream = self
            .select_object_content()
            .compat()
            .await?
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;

        let mut buffer: usize = 0;

        while let Some(event) = event_stream
            .payload
            .recv()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?
        {
            match event {
                SelectObjectContentEventStream::Records(_) => {
                    trace!("records Event");
                }
                SelectObjectContentEventStream::Stats(stats) => {
                    trace!(
                        stats = format!("{:?}", stats.details()).as_str(),
                        "Stats Event"
                    );
                    if let Some(stats) = stats.details {
                        buffer += stats.bytes_scanned() as usize;
                    };
                }
                SelectObjectContentEventStream::End(_) => {
                    trace!("End Event");
                    break;
                }
                SelectObjectContentEventStream::Progress(progress) => {
                    trace!(
                        details = format!("{:?}", progress.details()).as_str(),
                        "Progress Event"
                    );
                }
                SelectObjectContentEventStream::Cont(_) => {
                    trace!("Continuation Event");
                }
                otherwise => {
                    return Err(Error::new(
                        ErrorKind::Interrupted,
                        format!("{:?}", otherwise),
                    ))
                }
            }
        }

        Ok(buffer)
    }
}

#[async_trait]
impl Connector for BucketSelect {
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = Box::new(parameters);
    }
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::is_variable`] for more details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = BucketSelect::default();
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
    /// use chewdata::connector::{bucket_select::BucketSelect, Connector};
    /// use serde_json::Value;
    ///
    /// let mut connector = BucketSelect::default();
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
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = BucketSelect::default();
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
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "data/not-found-file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select::len")]
    async fn len(&mut self) -> Result<usize> {
        let mut connector = self.clone();
        connector.query = format!(
            "{} {}",
            self.query
                .clone()
                .to_lowercase()
                .split("where")
                .next()
                .unwrap(),
            "limit 1"
        );

        let len = connector.fetch_length().await.unwrap_or_default();

        info!(len, "The connector found data in the resource");

        Ok(len)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{bucket_select::BucketSelect, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use futures::StreamExt;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = BucketSelect::default();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///     connector.path = "/data/one_line.json".to_string();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket/".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     let datastream = connector.fetch(&document).await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select::fetch")]
    async fn fetch(&mut self, document: &dyn Document) -> Result<Option<DataStream>> {
        let mut buffer = Vec::default();
        let path = self.path();

        if let (Some(true), Some("csv")) = (
            self.metadata().has_headers,
            self.metadata().mime_subtype.as_deref(),
        ) {
            let mut connector = self.clone();

            let mut metadata = connector.metadata();
            metadata.has_headers = Some(false);

            connector.set_metadata(metadata);
            connector.query = format!(
                "{} {}",
                self.query
                    .clone()
                    .to_lowercase()
                    .split("where")
                    .next()
                    .unwrap(),
                "limit 1"
            );

            buffer.append(&mut connector.fetch_data().await?);
        }

        buffer.append(&mut self.fetch_data().await?);
        info!(
            path = path,
            "The connector fetch data into the resource with success"
        );

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
    #[instrument(skip(_dataset), name = "bucket_select::send")]
    async fn send(
        &mut self,
        _document: &dyn Document,
        _dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        unimplemented!("Can't send data to the remote document. Use the bucket connector instead of this connector")
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "Can't erase the document. Use the bucket connector instead of this connector"
        )
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        Ok(Box::pin(BucketSelectPaginator::new(self.clone()).await?))
    }
}

#[derive(Debug)]
pub struct BucketSelectPaginator {
    connector: BucketSelect,
    paths: IntoIter<String>,
}

impl BucketSelectPaginator {
    pub async fn new(connector: BucketSelect) -> Result<Self> {
        let mut bucket = Bucket::default();
        bucket.endpoint = connector.endpoint.clone();
        bucket.region = connector.region.clone();
        bucket.bucket = connector.bucket.clone();
        bucket.path = connector.path.clone();
        bucket.parameters = connector.parameters.clone();
        bucket.limit = connector.limit;
        bucket.skip = connector.skip;

        let bucket_paginator = BucketPaginator::new(bucket).await?;

        Ok(BucketSelectPaginator {
            paths: bucket_paginator.paths,
            connector,
        })
    }
}

#[async_trait]
impl Paginator for BucketSelectPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(Some(self.paths.clone().count()))
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///     connector.path = "data/multi_lines.json".to_string();
    ///     connector.endpoint = "http://localhost:9000".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.is_some(), "Can't get the first reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select_paginator::stream")]
    async fn stream(
        &self,
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
    use crate::document::{json::Json, jsonl::Jsonl};
    #[cfg(feature = "csv")]
    use crate::document::csv::Csv;
    use futures::StreamExt;

    #[test]
    fn is_variable() {
        let mut connector = BucketSelect::default();
        assert_eq!(false, connector.is_variable());
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        assert_eq!(true, connector.is_variable());
    }
    #[test]
    fn is_resource_will_change() {
        let mut connector = BucketSelect::default();
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
        let mut connector = BucketSelect::default();
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        assert_eq!("/dir/filename_value.ext", connector.path());
    }
    #[async_std::test]
    async fn len() {
        let mut connector = BucketSelect::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.query = "select * from s3object".to_string();
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
        let mut connector = BucketSelect::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.query = "select * from s3object".to_string();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "data/not_found.json".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
    async fn fetch() {
        let document = Json::default();

        let mut connector = BucketSelect::default();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };
        connector.path = "data/one_line.json".to_string();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.query = "select * from s3object".to_string();
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero"
        );
    }
    #[async_std::test]
    async fn json_document() {
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "my-key".to_string();
        connector.query = "my-query".to_string();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };

        let select_object_content_expected = connector
            .client()
            .compat()
            .await
            .unwrap()
            .select_object_content()
            .bucket(connector.bucket.clone())
            .key(connector.path())
            .expression(connector.query.clone())
            .expression_type(ExpressionType::Sql)
            .input_serialization(
                InputSerialization::builder()
                    .json(JsonInput::builder().r#type(JsonType::Document).build())
                    .compression_type(CompressionType::from("NONE"))
                    .build(),
            )
            .output_serialization(
                OutputSerialization::builder()
                    .json(JsonOutput::builder().build())
                    .build(),
            );

        assert_eq!(
            format!("{:?}", select_object_content_expected),
            format!(
                "{:?}",
                connector.select_object_content().compat().await.unwrap()
            )
        );
    }
    #[async_std::test]
    async fn json_lines() {
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "my-key".to_string();
        connector.query = "my-query".to_string();
        connector.metadata = Metadata {
            ..Jsonl::default().metadata
        };

        let select_object_content_expected = connector
            .client()
            .compat()
            .await
            .unwrap()
            .select_object_content()
            .bucket(connector.bucket.clone())
            .key(connector.path())
            .expression(connector.query.clone())
            .expression_type(ExpressionType::Sql)
            .input_serialization(
                InputSerialization::builder()
                    .json(JsonInput::builder().r#type(JsonType::Lines).build())
                    .compression_type(CompressionType::from("NONE"))
                    .build(),
            )
            .output_serialization(
                OutputSerialization::builder()
                    .json(JsonOutput::builder().build())
                    .build(),
            );

        assert_eq!(
            format!("{:?}", select_object_content_expected),
            format!(
                "{:?}",
                connector.select_object_content().compat().await.unwrap()
            )
        );
    }
    #[cfg(feature = "csv")]
    #[async_std::test]
    async fn csv_with_header() {
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "my-key".to_string();
        connector.query = "my-query".to_string();
        connector.metadata = Metadata {
            ..Csv::default().metadata
        };

        let select_object_content_expected = connector
            .client()
            .compat()
            .await
            .unwrap()
            .select_object_content()
            .bucket(connector.bucket.clone())
            .key(connector.path())
            .expression(connector.query.clone())
            .expression_type(ExpressionType::Sql)
            .input_serialization(
                InputSerialization::builder()
                    .csv(
                        CsvInput::builder()
                            .set_field_delimiter(connector.metadata.clone().delimiter)
                            .file_header_info(FileHeaderInfo::Use)
                            .set_quote_character(connector.metadata.clone().quote)
                            .set_quote_escape_character(connector.metadata.clone().escape)
                            .build(),
                    )
                    .compression_type(CompressionType::from("NONE"))
                    .build(),
            )
            .output_serialization(
                OutputSerialization::builder()
                    .csv(
                        CsvOutput::builder()
                            .set_field_delimiter(connector.metadata.clone().delimiter)
                            .set_quote_character(connector.metadata.clone().quote)
                            .set_quote_escape_character(connector.metadata.clone().escape)
                            .record_delimiter("\n".to_string())
                            .build(),
                    )
                    .build(),
            );

        assert_eq!(
            format!("{:?}", select_object_content_expected),
            format!(
                "{:?}",
                connector.select_object_content().compat().await.unwrap()
            )
        );
    }
    #[cfg(feature = "csv")]
    #[async_std::test]
    async fn csv_without_header() {
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "my-key".to_string();
        connector.query = "my-query".to_string();
        connector.metadata = Metadata {
            has_headers: Some(false),
            ..Csv::default().metadata
        };

        let select_object_content_expected = connector
            .client()
            .compat()
            .await
            .unwrap()
            .select_object_content()
            .bucket(connector.bucket.clone())
            .key(connector.path())
            .expression(connector.query.clone())
            .expression_type(ExpressionType::Sql)
            .input_serialization(
                InputSerialization::builder()
                    .csv(
                        CsvInput::builder()
                            .set_field_delimiter(connector.metadata.clone().delimiter)
                            .file_header_info(FileHeaderInfo::None)
                            .set_quote_character(connector.metadata.clone().quote)
                            .set_quote_escape_character(connector.metadata.clone().escape)
                            .build(),
                    )
                    .compression_type(CompressionType::from("NONE"))
                    .build(),
            )
            .output_serialization(
                OutputSerialization::builder()
                    .csv(
                        CsvOutput::builder()
                            .set_field_delimiter(connector.metadata.clone().delimiter)
                            .set_quote_character(connector.metadata.clone().quote)
                            .set_quote_escape_character(connector.metadata.clone().escape)
                            .record_delimiter("\n".to_string())
                            .build(),
                    )
                    .build(),
            );

        assert_eq!(
            format!("{:?}", select_object_content_expected),
            format!(
                "{:?}",
                connector.select_object_content().compat().await.unwrap()
            )
        );
    }
    #[async_std::test]
    async fn paginator_stream() {
        let mut connector = BucketSelect::default();
        connector.path = "data/multi_lines.json".to_string();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.query = "select * from s3object".to_string();
        connector.metadata = Metadata {
            ..Json::default().metadata
        };

        let paginator = connector.paginator().await.unwrap();
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
        let mut connector = BucketSelect::default();
        connector.endpoint = "http://localhost:9000".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json$".to_string();
        connector.query = "select * from s3object".to_string();
        connector.limit = Some(5);
        connector.skip = 1;
        connector.metadata = Metadata {
            ..Json::default().metadata
        };

        let paginator = connector.paginator().await.unwrap();
        assert!(paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();

        assert_eq!(
            "data/multi_lines.json".to_string(),
            stream.next().await.transpose().unwrap().unwrap().path()
        );
        assert_eq!(
            "data/one_line.json".to_string(),
            stream.next().await.transpose().unwrap().unwrap().path()
        );
    }
}
