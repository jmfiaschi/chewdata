use super::bucket::{Bucket, BucketPaginator};
use super::Paginator;
use crate::connector::Connector;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use json_value_merge::Merge;
use regex::Regex;
use rusoto_core::credential::ProvideAwsCredentials;
use rusoto_s3::{
    CSVInput, CSVOutput, InputSerialization, JSONInput, JSONOutput, OutputSerialization,
    ParquetInput, SelectObjectContentRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Result, Write},
};
use surf_bucket_select::model::{
    event_stream::EventStream, select_object_content::SelectObjectContentEventStreamItem,
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct BucketSelect {
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
    pub query: String,
    #[serde(alias = "params")]
    pub parameters: Box<Value>,
    pub limit: Option<usize>,
    pub skip: usize,
    pub timeout: Option<Duration>,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            metadata: Metadata::default(),
            query: "select * from s3object".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            region: rusoto_core::Region::default().name().to_string(),
            bucket: String::default(),
            path: String::default(),
            parameters: Box::new(Value::default()),
            timeout: None,
            limit: None,
            skip: 0,
            inner: Cursor::default(),
        }
    }
}

impl fmt::Display for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut secret_access_key = self.secret_access_key.clone().unwrap_or_default();
        secret_access_key.replace_range(
            0..(secret_access_key.len() / 2),
            (0..(secret_access_key.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );
        f.debug_struct("BucketSelect")
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
            .finish()
    }
}

impl BucketSelect {
    /// Get a Select object Content Request object with a BucketSelect connector.
    ///
    /// # Example: Get for json format
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use rusoto_s3::{SelectObjectContentRequest, InputSerialization, JSONInput, OutputSerialization, JSONOutput};
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "my-key".to_string();
    /// connector.query = "my-query".to_string();
    /// connector.metadata = Metadata {
    ///     ..Json::default().metadata
    /// };
    ///
    /// let select_object_content_request_expected = SelectObjectContentRequest {
    ///     bucket: "my-bucket".to_string(),
    ///     key: "my-key".to_string(),
    ///     expression: "my-query".to_string(),
    ///     expression_type: "SQL".to_string(),
    ///     input_serialization: InputSerialization {
    ///         json: Some(JSONInput {
    ///             type_: Some("DOCUMENT".to_string()),
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     output_serialization: OutputSerialization {
    ///         json: Some(JSONOutput {
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// assert_eq!(select_object_content_request_expected, connector.select_object_content_request());
    /// ```
    ///
    /// # Example: Get for jsonl format
    ///
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::Metadata;
    /// use rusoto_s3::{SelectObjectContentRequest, InputSerialization, JSONInput, OutputSerialization, JSONOutput};
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "my-key".to_string();
    /// connector.query = "my-query".to_string();
    /// connector.metadata = Metadata {
    ///     ..Jsonl::default().metadata
    /// };
    ///
    /// let select_object_content_request_expected = SelectObjectContentRequest {
    ///     bucket: "my-bucket".to_string(),
    ///     key: "my-key".to_string(),
    ///     expression: "my-query".to_string(),
    ///     expression_type: "SQL".to_string(),
    ///     input_serialization: InputSerialization {
    ///         json: Some(JSONInput {
    ///             type_: Some("LINES".to_string()),
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     output_serialization: OutputSerialization {
    ///         json: Some(JSONOutput {
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// assert_eq!(select_object_content_request_expected, connector.select_object_content_request());
    /// ```
    ///
    /// # Example: Get for csv format with header
    ///
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::Metadata;
    /// use rusoto_s3::{SelectObjectContentRequest, InputSerialization, CSVInput, CSVOutput, OutputSerialization};
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "my-key".to_string();
    /// connector.query = "my-query".to_string();
    /// connector.metadata = Metadata {
    ///     ..Csv::default().metadata
    /// };
    ///
    /// let select_object_content_request_expected = SelectObjectContentRequest {
    ///     bucket: "my-bucket".to_string(),
    ///     key: "my-key".to_string(),
    ///     expression: "my-query".to_string(),
    ///     expression_type: "SQL".to_string(),
    ///     input_serialization: InputSerialization {
    ///         csv: Some(CSVInput {
    ///             field_delimiter: Some(",".to_string()),
    ///             file_header_info: Some("USE".to_string()),
    ///             quote_character: Some("\"".to_string()),
    ///             quote_escape_character: Some("\\".to_string()),
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     output_serialization: OutputSerialization {
    ///         csv: Some(CSVOutput {
    ///             field_delimiter: Some(",".to_string()),
    ///             quote_character: Some("\"".to_string()),
    ///             quote_escape_character: Some("\\".to_string()),
    ///             record_delimiter: Some("\n".to_string()),
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// assert_eq!(select_object_content_request_expected, connector.select_object_content_request());
    /// ```
    /// # Example: Get for csv format without header
    ///
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::document::csv::Csv;
    /// use chewdata::Metadata;
    /// use rusoto_s3::{SelectObjectContentRequest, InputSerialization, CSVInput, OutputSerialization, CSVOutput};
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "my-key".to_string();
    /// connector.query = "my-query".to_string();
    /// connector.metadata = Metadata {
    ///     has_headers: Some(false),
    ///     ..Csv::default().metadata
    /// };
    ///
    /// let select_object_content_request_expected = SelectObjectContentRequest {
    ///     bucket: "my-bucket".to_string(),
    ///     key: "my-key".to_string(),
    ///     expression: "my-query".to_string(),
    ///     expression_type: "SQL".to_string(),
    ///     input_serialization: InputSerialization {
    ///         csv: Some(CSVInput {
    ///             field_delimiter: Some(",".to_string()),
    ///             file_header_info: Some("NONE".to_string()),
    ///             quote_character: Some("\"".to_string()),
    ///             quote_escape_character: Some("\\".to_string()),
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     output_serialization: OutputSerialization {
    ///         csv: Some(CSVOutput {
    ///             field_delimiter: Some(",".to_string()),
    ///             quote_character: Some("\"".to_string()),
    ///             quote_escape_character: Some("\\".to_string()),
    ///             record_delimiter: Some("\n".to_string()),
    ///             ..Default::default()
    ///         }),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// assert_eq!(select_object_content_request_expected, connector.select_object_content_request());
    /// ```
    pub fn select_object_content_request(&self) -> SelectObjectContentRequest {
        let metadata = self.metadata();
        let input_serialization = match metadata.mime_subtype.as_deref() {
            Some("csv") => InputSerialization {
                csv: Some(CSVInput {
                    field_delimiter: metadata.clone().delimiter,
                    file_header_info: Some(
                        match metadata.has_headers {
                            Some(true) => "USE",
                            Some(false) => "NONE",
                            _ => "USE",
                        }
                        .to_owned(),
                    ),
                    quote_character: metadata.clone().quote,
                    quote_escape_character: metadata.clone().escape,
                    ..Default::default()
                }),
                compression_type: metadata.compression,
                ..Default::default()
            },
            Some("octet-stream") | Some("parquet") => InputSerialization {
                parquet: Some(ParquetInput {}),
                compression_type: metadata.compression,
                ..Default::default()
            },
            Some("json") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("DOCUMENT".to_owned()),
                }),
                compression_type: metadata.compression,
                ..Default::default()
            },
            Some("x-ndjson") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("LINES".to_owned()),
                }),
                compression_type: metadata.compression,
                ..Default::default()
            },
            _ => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("LINES".to_owned()),
                }),
                compression_type: metadata.compression,
                ..Default::default()
            },
        };

        let output_serialization = match metadata.mime_subtype.as_deref() {
            Some("csv") => OutputSerialization {
                csv: Some(CSVOutput {
                    field_delimiter: metadata.delimiter,
                    quote_character: metadata.quote,
                    quote_escape_character: metadata.escape,
                    record_delimiter: match metadata
                        .terminator
                        .unwrap_or_else(|| "\n".to_string())
                        .as_str()
                    {
                        "CRLF" => Some("\n\r".to_string()),
                        "CR" => Some("\n".to_string()),
                        "LF" => Some("\r".to_string()),
                        terminal => Some(terminal.to_string()),
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            _ => OutputSerialization {
                json: Some(JSONOutput {
                    record_delimiter: metadata.delimiter,
                }),
                ..Default::default()
            },
        };

        SelectObjectContentRequest {
            bucket: self.bucket.clone(),
            key: self.path(),
            expression: self.query.clone(),
            expression_type: "SQL".to_owned(),
            input_serialization,
            output_serialization,
            ..Default::default()
        }
    }
    async fn fetch_data(&mut self) -> Result<Vec<u8>> {
        let client = surf::client();

        let endpoint = match self.endpoint.to_owned() {
            Some(endpoint) => endpoint,
            None => format!("https://s3-{}.amazonaws.com", self.region),
        };

        let credentials_provider: Box<dyn ProvideAwsCredentials + Sync + Send> =
            match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
                (Some(access_key_id), Some(secret_access_key)) => {
                    Box::new(rusoto_core::credential::StaticProvider::new_minimal(
                        access_key_id.to_owned(),
                        secret_access_key.to_owned(),
                    ))
                }
                (_, _) => Box::new(
                    rusoto_core::credential::DefaultCredentialsProvider::new()
                        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?,
                ),
            };

        let select_object_content_request = self.select_object_content_request();

        let req = surf_bucket_select::select_object_content(
            endpoint,
            select_object_content_request,
            Some(credentials_provider),
            self.region.to_owned(),
            self.timeout,
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?
        .build();

        let mut res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let body_bytes = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8_lossy(&body_bytes)
                ),
            ));
        }

        let mut buffer = Vec::default();

        if body_bytes.is_empty() {
            warn!("The response body of the bucket select is empty");
            return Ok(buffer);
        }

        trace!(
            data = String::from_utf8_lossy(&body_bytes).to_string().as_str(),
            "Data fetch from the bucket"
        );

        let mut event_stream =
            EventStream::<SelectObjectContentEventStreamItem>::new(body_bytes.clone());

        while let Some(item_result) = event_stream.next().await {
            match item_result {
                Ok(SelectObjectContentEventStreamItem::Records(records_event)) => {
                    if let Some(bytes) = records_event.payload {
                        buffer.append(&mut bytes.to_vec());
                    };
                }
                Ok(SelectObjectContentEventStreamItem::End(_)) => break,
                Err(e) => return Err(Error::new(ErrorKind::Interrupted, format!("{:?}", e))),
                _ => {}
            }
        }

        Ok(buffer)
    }
    async fn fetch_length(&mut self) -> Result<usize> {
        let client = surf::client();
        let endpoint = match self.endpoint.to_owned() {
            Some(endpoint) => endpoint,
            None => format!("https://s3-{}.amazonaws.com", self.region),
        };

        let credentials_provider: Box<dyn ProvideAwsCredentials + Sync + Send> =
            match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
                (Some(access_key_id), Some(secret_access_key)) => {
                    Box::new(rusoto_core::credential::StaticProvider::new_minimal(
                        access_key_id.to_owned(),
                        secret_access_key.to_owned(),
                    ))
                }
                (_, _) => Box::new(
                    rusoto_core::credential::DefaultCredentialsProvider::new()
                        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?,
                ),
            };

        let select_object_content_request = self.select_object_content_request();

        let req = surf_bucket_select::select_object_content(
            endpoint,
            select_object_content_request,
            Some(credentials_provider),
            self.region.to_owned(),
            None,
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?
        .build();

        let mut res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let body_bytes = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8_lossy(&body_bytes)
                ),
            ));
        }

        let mut buffer: usize = 0;

        if body_bytes.is_empty() {
            warn!("The response body of the bucket select is empty");
            return Ok(buffer);
        }

        trace!(
            data = String::from_utf8_lossy(&body_bytes).to_string().as_str(),
            "Data fetch from the bucket"
        );

        let mut event_stream =
            EventStream::<SelectObjectContentEventStreamItem>::new(body_bytes.clone());

        while let Some(item_result) = event_stream.next().await {
            match item_result {
                Ok(SelectObjectContentEventStreamItem::Stats(stats)) => {
                    if let Some(stats) = stats.details {
                        buffer += stats.bytes_scanned.unwrap_or(0) as usize
                    };
                }
                Ok(SelectObjectContentEventStreamItem::End(_)) => break,
                Err(e) => return Err(Error::new(ErrorKind::Interrupted, format!("{:?}", e))),
                _ => {}
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
    /// ```rust
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
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Example
    /// ```rust
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

        let mut new_parameters = new_parameters.clone();
        new_parameters.merge(metadata.clone());
        let mut old_parameters = *self.parameters.clone();
        old_parameters.merge(metadata);

        let mut actuel_path = self.path.clone();
        actuel_path.replace_mustache(old_parameters);

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
        let mut metadata = Map::default();
        
        metadata.insert("metadata".to_string(), self.metadata().into());
        path.replace_mustache(Value::Object(metadata));
        
        match (self.is_variable(), *self.parameters.clone()) {
            (true, params) => {
                path.replace_mustache(params);
                path
            }
            _ => path,
        }
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.region = "us-east-1".to_string();
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
    #[instrument]
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

        info!(len = len, "The connector found data in the resource");
        Ok(len)
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.region = "us-east-1".to_string();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
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
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::{bucket_select::BucketSelect, Connector};
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///
    ///     assert_eq!(0, connector.inner().len());
    ///
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.region = "us-east-1".to_string();
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.query = "select * from s3object".to_string();
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

            let headers = connector.fetch_data().await?;
            self.inner.write_all(&headers)?;
        }

        let body = self.fetch_data().await?;
        self.inner.write_all(&body)?;

        // initialize the position of the cursors
        self.inner.set_position(0);

        info!("The connector fetch data into the resource with success");
        Ok(())
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "Can't erase the document. Use the bucket connector instead of this connector"
        )
    }
    /// See [`Connector::send`] for more details.
    async fn send(&mut self, _position: Option<isize>) -> Result<()> {
        unimplemented!("Can't send data to the remote document. Use the bucket connector instead of this connector")
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(BucketSelectPaginator::new(self.clone())?))
    }
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for BucketSelect {
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
impl async_std::io::Write for BucketSelect {
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
pub struct BucketSelectPaginator {
    connector: BucketSelect,
    paths: IntoIter<String>,
}

impl BucketSelectPaginator {
    pub fn new(connector: BucketSelect) -> Result<Self> {
        let mut bucket = Bucket::default();
        bucket.endpoint = connector.endpoint.clone();
        bucket.access_key_id = connector.access_key_id.clone();
        bucket.secret_access_key = connector.secret_access_key.clone();
        bucket.region = connector.region.clone();
        bucket.bucket = connector.bucket.clone();
        bucket.path = connector.path.clone();
        bucket.parameters = connector.parameters.clone();
        bucket.limit = connector.limit;
        bucket.skip = connector.skip;

        let bucket_paginator = BucketPaginator::new(bucket)?;

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
    /// # Example
    /// ```rust
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
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.region = "us-east-1".to_string();
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
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
    /// # Example: With wildcard, limit and skip. List results are always returned in UTF-8 binary order
    /// ```rust
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
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.path = "data/*.json$".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     connector.limit = Some(5);
    ///     connector.skip = 1;
    ///     connector.metadata = Metadata {
    ///         ..Json::default().metadata
    ///     };
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert_eq!("data/multi_lines.json".to_string(), stream.next().await.transpose()?.unwrap().path());
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
