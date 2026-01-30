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
use crate::connector::Connector;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{ConnectorStream, DataSet, DataStream, Metadata};
use async_compat::CompatExt;
use async_lock::OnceCell;
use async_stream::stream;
use async_trait::async_trait;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput;
use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    JsonInput, JsonOutput, JsonType, OutputSerialization, ParquetInput,
    SelectObjectContentEventStream,
};
use aws_sdk_s3::Client;
use dashmap::DashMap;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::prelude::*;
use std::env;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

type SharedClients = DashMap<String, Arc<OnceCell<Client>>>;
static CLIENTS: OnceLock<SharedClients> = OnceLock::new();

const DEFAULT_REGION: &str = "us-west-2";
const DEFAULT_ENDPOINT: &str = "http://localhost:9000";

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct BucketSelect {
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
    pub query: String,
    #[serde(alias = "params")]
    pub parameters: Box<Value>,
    pub limit: Option<usize>,
    pub skip: usize,
    pub timeout: Option<Duration>,
    #[serde(skip)]
    #[serde(default)]
    client: Option<Client>,
}

impl fmt::Debug for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketSelect")
            .field("document", &self.document.display_only_for_debugging())
            .field("metadata", &self.metadata.display_only_for_debugging())
            .field("endpoint", &self.endpoint)
            .field("profile", &self.profile)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("parameters", &self.parameters.display_only_for_debugging())
            .field("limit", &self.limit)
            .field("skip", &self.skip)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            document: None,
            metadata: Metadata::default(),
            query: "select * from s3object".to_string(),
            endpoint: None,
            profile: "default".to_string(),
            region: None,
            bucket: String::default(),
            path: String::default(),
            parameters: Box::<Value>::default(),
            timeout: None,
            limit: None,
            skip: 0,
            client: None,
        }
    }
}

impl BucketSelect {
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
    /// Get client and updating the connector if the client hasn't been initialized.
    #[instrument(name = "bucket_select::client_mut")]
    async fn client_mut(&mut self) -> Result<Client> {
        if let None = self.client {
            let client = get_or_create_client(self.endpoint(), self.region()).await?;

            trace!("initialize the client in the connector");
            self.client = Some(client);
        }

        Ok(self.client.clone().unwrap())
    }
    /// Get client without updating the connecter.
    #[instrument(name = "bucket_select::client")]
    async fn client(&self) -> Result<Client> {
        if let None = self.client {
            return get_or_create_client(self.endpoint(), self.region()).await;
        }

        Ok(self.client.clone().unwrap())
    }
}

/// Get a Select object Content Request object with a BucketSelect connector.
pub async fn input_serialization(document: &dyn Document) -> Result<InputSerialization> {
    let metadata = document.metadata();

    let input_serialization =
        match metadata.mime_subtype.as_deref() {
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

    Ok(input_serialization)
}
pub async fn output_serialization(document: &dyn Document) -> Result<OutputSerialization> {
    let metadata = document.metadata();

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

    Ok(output_serialization)
}

async fn read_event_stream(
    event_stream: &mut SelectObjectContentOutput,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    loop {
        let event_opt = event_stream.payload.recv().compat().await;

        let event = match event_opt {
            Ok(Some(ev)) => ev,
            Ok(None) => break,
            Err(e) => {
                warn!("S3 Select failed: {:#?}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
        };

        match event {
            SelectObjectContentEventStream::Records(records) => {
                if let Some(bytes) = records.payload() {
                    buffer.extend_from_slice(bytes.as_ref());
                }
            }
            SelectObjectContentEventStream::Stats(stats) => {
                trace!(?stats, "Stats Event");
            }
            SelectObjectContentEventStream::Progress(progress) => {
                trace!(?progress, "Progress Event");
            }
            SelectObjectContentEventStream::End(_) => {
                trace!("End Event");
                break;
            }
            SelectObjectContentEventStream::Cont(_) => {
                trace!("Continuation Event");
            }
            other => trace!(event = ?other, "Ignoring unknown event"),
        }
    }

    Ok(())
}

async fn read_event_stream_length(
    event_stream: &mut SelectObjectContentOutput,
    length: &mut usize,
) -> Result<()> {
    let mut scanned = 0usize;

    while let Some(event) = event_stream
        .payload
        .recv()
        .compat()
        .await
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
    {
        match event {
            SelectObjectContentEventStream::Stats(stats) => {
                if let Some(bytes) = stats.details.and_then(|d| d.bytes_scanned()) {
                    scanned = bytes as usize;
                }
            }
            SelectObjectContentEventStream::End(_) => break,
            other => trace!(event = ?other, "Ignoring unknown event"),
        }
    }

    *length = scanned;
    Ok(())
}

async fn get_or_create_client(endpoint: String, region: String) -> Result<Client> {
    let clients = CLIENTS.get_or_init(DashMap::new);
    let key = format!("{}:{}", endpoint, region);

    let cell = clients
        .entry(key.clone())
        .or_insert_with(|| Arc::new(OnceCell::new()))
        .clone();

    let client = cell
        .get_or_try_init(|| async {
            trace!(key = ?key, "storing client in shared container");

            if let Ok(key) = env::var("BUCKET_ACCESS_KEY_ID") {
                env::set_var("AWS_ACCESS_KEY_ID", key);
            }
            if let Ok(secret) = env::var("BUCKET_SECRET_ACCESS_KEY") {
                env::set_var("AWS_SECRET_ACCESS_KEY", secret);
            }

            let provider = CredentialsProviderChain::default_provider().await;
            let config = aws_sdk_s3::Config::builder()
                .endpoint_url(endpoint)
                .region(Region::new(region))
                .credentials_provider(provider)
                .behavior_version_latest()
                .force_path_style(true)
                .build();

            let client = Client::from_conf(config);

            Ok::<Client, anyhow::Error>(client)
        })
        .await
        .unwrap();

    Ok(client.clone())
}

#[async_trait]
impl Connector for BucketSelect {
    /// See [`Connector::set_document`] for more details.
    fn set_document(&mut self, document: Box<dyn Document>) -> Result<()> {
        self.document = Some(document);

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
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
    }
    /// See [`Connector::is_variable`] for more details.
    ///
    /// # Example
    ///
    /// ```
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
    /// ```
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
            trace!("It stays link to the same resource");
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
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///     let mut connector = BucketSelect::default();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///     connector.set_document(Box::new(document)).unwrap();
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "data/not-found-file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select::len")]
    async fn len(&self) -> Result<usize> {
        let client = self.client().await?;
        let query: String = format!(
            "{} {}",
            self.query
                .clone()
                .to_lowercase()
                .split("where")
                .next()
                .unwrap(),
            "limit 1"
        );
        let document = self.document()?;
        let body_input = input_serialization(document).await?;
        let body_output = output_serialization(document).await?;
        let bucket = self.bucket.clone();
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let mut event_stream = match client
            .select_object_content()
            .bucket(&bucket)
            .key(&path)
            .expression(&query)
            .expression_type(ExpressionType::Sql)
            .input_serialization(body_input.clone())
            .output_serialization(body_output.clone())
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))
        {
            Ok(event_stream) => event_stream,
            Err(e) => {
                warn!(error = ?e, "failed to send select request");

                return Ok(0);
            }
        };

        let mut len = 0;

        match read_event_stream_length(&mut event_stream, &mut len).await {
            Ok(_) => (),
            Err(e) => {
                warn!(error = ?e, "failed to read event stream length");

                return Ok(0);
            }
        };

        info!(len, "resource length resolved");

        Ok(len)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::{bucket_select::BucketSelect, Connector};
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
    ///
    ///     let mut connector = BucketSelect::default();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.set_document(document);
    ///
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select::fetch")]
    async fn fetch(&mut self) -> Result<Option<DataStream>> {
        let client = self.client_mut().await?;
        let document = self.document()?;
        let body_input = input_serialization(document).await?;
        let body_output = output_serialization(document).await?;
        let path = self.path();
        let bucket = self.bucket.clone();
        let query = self.query.clone();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let mut buffer = Vec::default();

        if let (Some(true), Some("csv")) = (
            self.metadata().has_headers,
            self.metadata().mime_subtype.as_deref(),
        ) {
            let mut document_for_header = document.clone_box();
            let mut metadata = document_for_header.metadata().clone();
            metadata.has_headers = Some(false);
            document_for_header.set_metadata(metadata);

            let csv_body_input = input_serialization(&*document_for_header).await?;
            let csv_body_output = output_serialization(&*document_for_header).await?;

            let csv_query_header = format!(
                "{} {}",
                self.query
                    .clone()
                    .to_lowercase()
                    .split("where")
                    .next()
                    .unwrap(),
                "limit 1"
            );

            let mut event_stream = client
                .select_object_content()
                .bucket(&bucket)
                .key(&path)
                .expression(&csv_query_header)
                .expression_type(ExpressionType::Sql)
                .input_serialization(csv_body_input)
                .output_serialization(csv_body_output)
                .send()
                .compat()
                .await
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;

            read_event_stream(&mut event_stream, &mut buffer).await?;
        }

        let mut event_stream = client
            .select_object_content()
            .bucket(&bucket)
            .key(&path)
            .expression(&query)
            .expression_type(ExpressionType::Sql)
            .input_serialization(body_input)
            .output_serialization(body_output)
            .send()
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;

        read_event_stream(&mut event_stream, &mut buffer).await?;

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
    #[instrument(skip(_dataset), name = "bucket_select::send")]
    async fn send(&mut self, _dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        unimplemented!("Can't send data. Use the bucket connector instead of this connector")
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!("Can't erase data. Use the bucket connector instead of this connector")
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        BucketSelectPaginator::new(self).await?.paginate(self).await
    }
}

#[derive(Debug)]
pub struct BucketSelectPaginator {
    pub paths: IntoIter<String>,
    pub skip: usize,
}

impl BucketSelectPaginator {
    pub async fn new(connector: &BucketSelect) -> Result<Self> {
        let mut bucket = Bucket::default();
        bucket.endpoint = connector.endpoint.clone();
        bucket.region = connector.region.clone();
        bucket.bucket = connector.bucket.clone();
        bucket.path = connector.path.clone();
        bucket.parameters = connector.parameters.clone();
        bucket.limit = connector.limit;
        bucket.skip = connector.skip;

        let bucket_paginator = BucketPaginator::new(&bucket).await?;

        Ok(BucketSelectPaginator {
            paths: bucket_paginator.paths,
            skip: bucket_paginator.skip,
        })
    }
    /// Paginate through the bucket folder.
    /// Wildcard is allowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::bucket_select::{BucketSelect, BucketSelectPaginator};
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
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
    ///     let mut connector = BucketSelect::default();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/*.json$".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.limit = Some(5);
    ///     connector.skip = 1;
    ///     connector.set_document(Box::new(document)).unwrap();
    ///
    ///     let paginator = BucketSelectPaginator::new(&connector).await.unwrap();
    ///
    ///     let mut paging = paginator.paginate(&connector).await.unwrap();
    ///
    ///     assert_eq!(
    ///         "data/multi_lines.json".to_string(),
    ///         paging.next().await.transpose().unwrap().unwrap().path()
    ///     );
    ///     assert_eq!(
    ///         "data/one_line.json".to_string(),
    ///         paging.next().await.transpose().unwrap().unwrap().path()
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "bucket_select::paginate")]
    pub async fn paginate(&self, connector: &BucketSelect) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let mut paths = self.paths.clone();

        let stream = Box::pin(stream! {
            for path in &mut paths {
                trace!(next_path = path.as_str(), "Next path");

                let mut new_connector = connector.clone();
                new_connector.path = path;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream yields a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("The stream stops yielding new connectors");
        });

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "csv")]
    use crate::document::csv::Csv;
    use crate::document::json::Json;
    // use crate::document::jsonl::Jsonl;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

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
    #[apply(test!)]
    async fn len() {
        let document = Json::default();
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();
        assert!(
            0 < connector.len().await.unwrap(),
            "The length of the document is not greather than 0"
        );
        connector.path = "data/not-found-file".to_string();
        assert_eq!(0, connector.len().await.unwrap());
    }
    #[apply(test!)]
    async fn is_empty() {
        let document = Json::default();
        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/one_line.json".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "data/not_found.json".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn fetch() {
        let document = Json::default();

        let mut connector = BucketSelect::default();
        connector.path = "data/one_line.json".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn json_document() {
        use crate::DataResult;

        let document = Json::default();

        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/multi_lines.json".to_string();
        connector.query = "select * from s3object[*]._1 LIMIT 1".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let expected_data: Value = serde_json::from_str(r#"{"number": 10,"group": 1456,"string": "value to test","long-string": "Long val\nto test","boolean": true,"special_char": "é","rename_this": "field must be renamed","date": "2019-12-31","filesize": 1000000,"round": 10.156,"url": "?search=test me","list_to_sort": "A,B,C","code": "value_to_map","remove_field": "field to remove"}"#,).unwrap();

        let mut datastream = connector.fetch().await.unwrap().unwrap();

        assert_eq!(
            DataResult::Ok(expected_data),
            datastream.next().await.unwrap(),
            "The connector has no data."
        );
    }
    // Face issue in the github CI
    // #[apply(test!)]
    // async fn json_lines() {
    //     use crate::DataResult;

    //     let document = Jsonl::default();

    //     let mut connector = BucketSelect::default();
    //     connector.bucket = "my-bucket".to_string();
    //     connector.path = "data/multi_lines.jsonl".to_string();
    //     connector.query = "select * from s3object".to_string();
    //     connector.metadata = document.metadata();

    //     let expected_data: Value = serde_json::from_str(r#"{"number": 10,"group": 1456,"string": "value to test","long-string": "Long val\nto test","boolean": true,"special_char": "é","rename_this": "field must be renamed","date": "2019-12-31","filesize": 1000000,"round": 10.156,"url": "?search=test me","list_to_sort": "A,B,C","code": "value_to_map","remove_field": "field to remove"}"#,).unwrap();

    //     let mut datastream = connector.fetch(&document).await.unwrap().unwrap();

    //     assert_eq!(
    //         DataResult::Ok(expected_data),
    //         datastream.next().await.unwrap(),
    //         "The connector has no data."
    //     );
    // }
    #[cfg(feature = "csv")]
    #[apply(test!)]
    async fn csv_with_header() {
        use crate::DataResult;

        let document = Csv::default();

        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/multi_lines.csv".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let expected_data: Value = serde_json::from_str(r#"{"number": 10,"group": 1456,"string": "value to test","long-string": "Long val\nto test","boolean": true,"special_char": "é","rename_this": "field must be renamed","date": "2019-12-31","filesize": 1000000,"round": 10.156,"url": "?search=test me","list_to_sort": "A,B,C","code": "value_to_map","remove_field": "field to remove"}"#,).unwrap();

        let mut datastream = connector.fetch().await.unwrap().unwrap();

        assert_eq!(
            DataResult::Ok(expected_data),
            datastream.next().await.unwrap(),
            "The connector has no data."
        );
    }
    #[cfg(feature = "csv")]
    #[apply(test!)]
    async fn csv_without_header() {
        use crate::DataResult;

        let document = Csv {
            metadata: Metadata {
                has_headers: Some(false),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/multi_lines-without_header.csv".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let expected_data: Value = serde_json::from_str(r#"[10,1456,"value to test","Long val\nto test",true,"é","field must be renamed","2019-12-31",1000000,10.156,"?search=test me","A,B,C","value_to_map","field to remove"]"#).unwrap();

        let mut datastream = connector.fetch().await.unwrap().unwrap();

        assert_eq!(
            DataResult::Ok(expected_data),
            datastream.next().await.unwrap(),
            "The connector has no data."
        );
    }
    #[apply(test!)]
    async fn paginate() {
        let document = Json::default();

        let mut connector = BucketSelect::default();
        connector.path = "data/multi_lines.json".to_string();
        connector.bucket = "my-bucket".to_string();
        connector.query = "select * from s3object".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = BucketSelectPaginator::new(&connector).await.unwrap();

        let mut paging = paginator.paginate(&connector).await.unwrap();

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
    async fn paginate_with_wildcard() {
        let document = Json::default();

        let mut connector = BucketSelect::default();
        connector.bucket = "my-bucket".to_string();
        connector.path = "data/*.json$".to_string();
        connector.query = "select * from s3object".to_string();
        connector.limit = Some(5);
        connector.skip = 1;
        connector.set_document(Box::new(document)).unwrap();

        let paginator = BucketSelectPaginator::new(&connector).await.unwrap();

        let mut paging = paginator.paginate(&connector).await.unwrap();

        assert_eq!(
            "data/multi_lines.json".to_string(),
            paging.next().await.transpose().unwrap().unwrap().path()
        );
        assert_eq!(
            "data/one_line.json".to_string(),
            paging.next().await.transpose().unwrap().unwrap().path()
        );
    }
}
