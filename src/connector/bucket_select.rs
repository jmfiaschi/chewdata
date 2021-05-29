use crate::connector::Connector;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use futures::StreamExt;
use http::status::StatusCode;
use regex::Regex;
use rusoto_core::{credential::StaticProvider, Region, RusotoError};
use rusoto_s3::{
    CSVInput, CSVOutput, HeadObjectRequest, InputSerialization, JSONInput, JSONOutput,
    OutputSerialization, ParquetInput, S3Client, SelectObjectContentRequest,
    S3 as RusotoS3,
};
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use async_trait::async_trait;
use async_std::io::{Cursor, Write, prelude::WriteExt};
use std::pin::Pin;
use std::task::{Poll, Context};
use async_std::prelude::*;

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct BucketSelect {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    pub path: String,
    pub query: String,
    pub parameters: Value,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            metadata: Metadata::default(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            region: Region::default().name().to_owned(),
            bucket: "".to_owned(),
            path: "".to_owned(),
            query: "".to_owned(),
            inner: Cursor::new(Vec::default()),
            parameters: Value::Null,
        }
    }
}

impl Clone for BucketSelect {
    fn clone(&self) -> Self {
        BucketSelect {
            metadata: self.metadata.to_owned(),
            endpoint: self.endpoint.to_owned(),
            access_key_id: self.access_key_id.to_owned(),
            secret_access_key: self.secret_access_key.to_owned(),
            region: self.region.to_owned(),
            bucket: self.bucket.to_owned(),
            path: self.path.to_owned(),
            query: self.query.to_owned(),
            inner: Cursor::new(Vec::default()),
            parameters: self.parameters.to_owned(),
        }
    }
}

impl fmt::Display for BucketSelect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}


impl BucketSelect {
    pub fn s3_client(&self) -> S3Client {
        match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
            (Some(access_key_id), Some(secret_access_key)) => S3Client::new_with(
                rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
                StaticProvider::new_minimal(access_key_id.to_owned(), secret_access_key.to_owned()),
                Region::Custom {
                    name: self.region.to_owned(),
                    endpoint: match self.endpoint.to_owned() {
                        Some(endpoint) => endpoint,
                        None => format!("https://s3-{}.amazonaws.com", self.region),
                    },
                },
            ),
            (_, _) => S3Client::new(Region::Custom {
                name: self.region.to_owned(),
                endpoint: match self.endpoint.to_owned() {
                    Some(endpoint) => endpoint,
                    None => format!("https://s3-{}.amazonaws.com", self.region),
                },
            }),
        }
    }
    /// Test if the path is variable.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = BucketSelect::default();
    /// assert_eq!(false, connector.is_variable_path());
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_variable_path());
    /// ```
    pub fn is_variable_path(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    fn select_object_content_request(
        &mut self,
        query: String,
        metadata: Metadata,
    ) -> SelectObjectContentRequest {
        let connector = self.clone();

        let input_serialization = match metadata.mime_type.as_deref() {
            Some("text/csv; charset=utf-8") | Some("text/csv") => InputSerialization {
                csv: Some(CSVInput {
                    field_delimiter: metadata.delimiter.to_owned(),
                    file_header_info: Some(
                        match metadata.has_headers {
                            Some(true) => "USE",
                            Some(false) => "NONE",
                            _ => "USE",
                        }
                        .to_owned(),
                    ),
                    quote_character: metadata.quote.to_owned(),
                    quote_escape_character: metadata.escape.to_owned(),
                    ..Default::default()
                }),
                compression_type: metadata.compression.to_owned(),
                ..Default::default()
            },
            Some("application/octet-stream") => InputSerialization {
                parquet: Some(ParquetInput {}),
                compression_type: metadata.compression.to_owned(),
                ..Default::default()
            },
            Some("application/json") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("DOCUMENT".to_owned()),
                }),
                compression_type: metadata.compression.to_owned(),
                ..Default::default()
            },
            Some("application/x-ndjson") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("DOCUMENT".to_owned()),
                }),
                compression_type: metadata.compression.to_owned(),
                ..Default::default()
            },
            _ => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("LINES".to_owned()),
                }),
                compression_type: metadata.compression.to_owned(),
                ..Default::default()
            },
        };

        let output_serialization = match metadata.mime_type.as_deref() {
            Some("text/csv; charset=utf-8") | Some("text/csv") => OutputSerialization {
                csv: Some(CSVOutput {
                    field_delimiter: metadata.delimiter.to_owned(),
                    quote_character: metadata.quote.to_owned(),
                    quote_escape_character: metadata.escape.to_owned(),
                    record_delimiter: metadata.terminator.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            _ => OutputSerialization {
                json: Some(JSONOutput {
                    record_delimiter: metadata.delimiter.to_owned(),
                }),
                ..Default::default()
            },
        };

        SelectObjectContentRequest {
            bucket: connector.bucket.to_owned(),
            key: connector.path(),
            expression: query,
            expression_type: "SQL".to_owned(),
            input_serialization,
            output_serialization,
            ..Default::default()
        }
    }
    async fn init_buffer_by_query_and_metadata(
        &mut self,
        query: String,
        metadata: Metadata,
    ) -> Result<()> {
        let connector = self.clone();
        let s3_client = connector.s3_client();
        let request = self.select_object_content_request(query, metadata);

        let output = s3_client
                .select_object_content(request)
                .await
                .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

        let mut event_stream = match output.payload {
            Some(event_stream) => event_stream,
            None => return Ok(()),
        };

        let mut buffer = String::default();
        while let Some(Ok(item)) = event_stream.next().await {
            if let rusoto_s3::SelectObjectContentEventStreamItem::Records(records_event) = item
            {
                if let Some(bytes) = records_event.payload {
                    buffer.push_str(&String::from_utf8(bytes.to_vec()).unwrap());
                };
            }
        };

        self.inner.write_all(buffer.as_bytes()).await?;

        Ok(())
    }
    async fn initialize(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Init inner buffer");
        let mut metadata_header = self.metadata.clone();
        metadata_header.has_headers = Some(false);
        let metadata_body = self.metadata.clone();

        match (
            metadata_body.has_headers,
            metadata_body.mime_type.as_deref(),
        ) {
            (Some(true), Some("text/csv")) | (Some(true), Some("text/csv; charset=utf-8")) => self
                .init_buffer_by_query_and_metadata(
                    format!(
                        "{} {}",
                        self.query
                            .clone()
                            .to_lowercase()
                            .split("where")
                            .next()
                            .unwrap(),
                        "limit 1"
                    ),
                    metadata_header,
                ).await?,
            _ => (),
        };

        self.init_buffer_by_query_and_metadata(self.query.clone(), metadata_body).await?;

        // initialize the position of the cursor
        self.inner.set_position(0);

        debug!(slog_scope::logger(), "Init inner buffer ended");

        Ok(())
    }
}

#[async_trait]
impl Connector for BucketSelect {
    /// Set the path parameters.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = BucketSelect::default();
    /// assert_eq!(Value::Null, connector.parameters);
    /// let params: Value = Value::String("my param".to_string());
    /// connector.set_parameters(params.clone());
    /// assert_eq!(params.clone(), connector.parameters.clone());
    /// ```
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters.clone();
    }
    fn is_variable_path(&self) -> bool { false }
    /// Get the resolved path.
    ///
    /// # Example
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
        match (self.is_variable_path(), self.parameters.clone()) {
            (true, params) => self.path.clone().replace_mustache(params),
            _ => self.path.clone(),
        }
    }
    /// Check if the connector of the current path has data.
    /// If
    ///     - Check if the current connector has data into the inner buffer.
    /// If No
    ///     - Try to fetch the remote file.
    /// If Yes
    ///     - Check if the remote file contains bytes.
    /// If every tests failed, return true.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    /// use chewdata::Metadata;
    ///
    /// let mut metadata = Metadata::default();
    /// metadata.mime_type = Some("application/json".to_string());
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.endpoint = Some("http://localhost:9000".to_string());
    /// connector.access_key_id = Some("minio_access_key".to_string());
    /// connector.secret_access_key = Some("minio_secret_key".to_string());
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "data/one_line.json".to_string();
    /// connector.query = "select * from s3object".to_string();
    /// connector.metadata = metadata;
    /// assert!(!connector.is_empty().unwrap(), "The document should not be empty.");
    /// connector.path = "data/not_found.json".to_string();
    /// assert!(connector.is_empty().unwrap(), "The document should be empty because the document not exist.");
    /// ```
    async fn is_empty(&self) -> Result<bool> {
        if 0 < self.inner().len() {
            return Ok(false);
        }

        {
            let mut connector_clone = self.clone();
            let mut buf = String::new();
            connector_clone.inner.set_position(0);
            match connector_clone.read_to_string(&mut buf).await {
                Ok(_) => (),
                Err(_) => {
                    info!(slog_scope::logger(), "The file not exist"; "path" => connector_clone.path());
                    return Ok(true);
                }
            }
            if 0 < buf.len() {
                return Ok(false);
            }
        }

        Ok(true)
    }
    /// Get the inner buffer reference.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = BucketSelect::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// Get the total document size.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = BucketSelect::default();
    /// connector.endpoint = Some("http://localhost:9000".to_string());
    /// connector.access_key_id = Some("minio_access_key".to_string());
    /// connector.secret_access_key = Some("minio_secret_key".to_string());
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "data/one_line.json".to_string();
    /// assert!(0 < connector.len().unwrap(), "The length of the document is not greather than 0");
    /// connector.path = "data/not-found-file".to_string();
    /// assert_eq!(0, connector.len().unwrap());
    /// ```
    async fn len(&self) -> Result<usize> {
        let s3_client = self.s3_client();
        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: self.path(),
            ..Default::default()
        };

        match s3_client.head_object(request).await {
            Ok(response) => match response.content_length {
                Some(len) => Ok(len as usize),
                None => Ok(0 as usize),
            },
            Err(e) => {
                let error = format!("{:?}", e);
                match e {
                    RusotoError::Unknown(http_response) => match http_response.status {
                        StatusCode::NOT_FOUND => Ok(0),
                        _ => Err(Error::new(ErrorKind::Interrupted, error)),
                    },
                    _ => Err(Error::new(ErrorKind::Interrupted, e)),
                }
            }
        }
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    async fn erase(&mut self) -> Result<()> { 
        info!(slog_scope::logger(), "Can't clean the document"; "connector" => format!("{:?}", self), "path" => self.path());
        Ok(()) 
    }
    async fn seek_and_flush(&mut self, _position: i64) -> Result<()> {
        self.flush().await
    }
}

#[async_trait]
impl async_std::io::Read for BucketSelect {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        if self.inner.clone().into_inner().is_empty() {
            match self.initialize().boxed().poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, e))),
                Poll::Pending => return Poll::Pending
            };
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[async_trait]
impl Write for BucketSelect {
    /// Write the data into the inner buffer before to flush it.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use std::io::Write;
    ///
    /// let mut connector = BucketSelect::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(Err(Error::new(ErrorKind::Interrupted, "Connector type 'bucket_select' can't write data into the connector because it used only to retrieve data in a bucket file. use instead the connector 'bucket'")))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(ErrorKind::Interrupted, "Connector type 'bucket_select' can't flush data into the connector because it used only to retrieve data in a bucket file. use instead the connector 'bucket'")))
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>{
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
