use super::Paginator;
use crate::connector::Connector;
use crate::document::DocumentType;
use crate::helper::mustache::Mustache;
use crate::DataResult;
use crate::Metadata;
use async_std::prelude::*;
use async_trait::async_trait;
use regex::Regex;
use rusoto_core::credential::ProvideAwsCredentials;
use rusoto_s3::{
    CSVInput, CSVOutput, InputSerialization, JSONInput, JSONOutput, OutputSerialization,
    ParquetInput, SelectObjectContentRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Result, Write},
};
use surf_bucket_select::model::{
    event_stream::EventStream, select_object_content::SelectObjectContentEventStreamItem,
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct BucketSelect {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "document")]
    document_type: Box<DocumentType>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    pub path: String,
    pub query: String,
    pub parameters: Value,
    pub timeout: Option<Duration>,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            metadata: Metadata::default(),
            query: "select * from s3object".to_string(),
            document_type: Box::new(DocumentType::default()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            region: rusoto_core::Region::default().name().to_string(),
            bucket: String::default(),
            path: String::default(),
            parameters: Value::default(),
            timeout: None,
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
            .field("document_type", &self.document_type)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &secret_access_key)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("path", &self.path)
            .field("parameters", &self.parameters)
            .finish()
    }
}

impl BucketSelect {
    fn select_object_content_request(
        &mut self,
        query: String,
        metadata: Metadata,
    ) -> SelectObjectContentRequest {
        let connector = self.clone();

        let input_serialization = match metadata.mime_type.as_deref() {
            Some("text/csv; charset=utf-8") | Some("text/csv") => InputSerialization {
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
            Some("application/octet-stream") => InputSerialization {
                parquet: Some(ParquetInput {}),
                compression_type: metadata.compression,
                ..Default::default()
            },
            Some("application/json") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("DOCUMENT".to_owned()),
                }),
                compression_type: metadata.compression,
                ..Default::default()
            },
            Some("application/x-ndjson") => InputSerialization {
                json: Some(JSONInput {
                    type_: Some("DOCUMENT".to_owned()),
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

        let output_serialization = match metadata.mime_type.as_deref() {
            Some("text/csv; charset=utf-8") | Some("text/csv") => OutputSerialization {
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
            bucket: connector.bucket.to_owned(),
            key: connector.path(),
            expression: query,
            expression_type: "SQL".to_owned(),
            input_serialization,
            output_serialization,
            ..Default::default()
        }
    }
    async fn fetch_data_by_query_and_metadata(
        &mut self,
        query: String,
        metadata: Metadata,
    ) -> Result<String> {
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

        let select_object_content_request = self.select_object_content_request(query, metadata);
        println!("select_object_content_request {:?}", select_object_content_request);
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

        println!("req {:?}", req);

        let mut res = client
            .send(req)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let payload = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8(payload)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        let mut event_stream =
            EventStream::<SelectObjectContentEventStreamItem>::new(payload.clone());
        let mut buffer = String::default();

        
        while let Some(Ok(item)) = event_stream.next().await {
            println!("item {:?}", item);
            match item {
                SelectObjectContentEventStreamItem::Records(records_event) => {
                    if let Some(bytes) = records_event.payload {
                        buffer.push_str(&String::from_utf8(bytes.to_vec()).unwrap());
                    };
                }
                SelectObjectContentEventStreamItem::End(_) => break,
                _ => {}
            }
        }

        Ok(buffer)
    }
    async fn fetch_length_by_query_and_metadata(
        &mut self,
        query: String,
        metadata: Metadata,
    ) -> Result<usize> {
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

        let select_object_content_request = self.select_object_content_request(query, metadata);

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

        let payload = res
            .body_bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        if !res.status().is_success() {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "Curl failed with status code '{}' and response body: {}",
                    res.status(),
                    String::from_utf8(payload)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                ),
            ));
        }

        let mut event_stream =
            EventStream::<SelectObjectContentEventStreamItem>::new(payload.clone());
        let mut buffer: usize = 0;

        while let Some(Ok(item)) = event_stream.next().await {
            match item {
                SelectObjectContentEventStreamItem::Stats(stats) => {
                    if let Some(stats) = stats.details {
                        buffer += stats.bytes_scanned.unwrap_or(0) as usize
                    };
                }
                SelectObjectContentEventStreamItem::End(_) => break,
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
        self.parameters = parameters;
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata
            .clone()
            .merge(self.document_type.document().metadata())
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
            return Ok(false);
        }

        let actuel_path = self.path.clone().replace_mustache(self.parameters.clone());
        let new_path = self.path.clone().replace_mustache(new_parameters);

        if actuel_path == new_path {
            return Ok(false);
        }

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
        match (self.is_variable(), self.parameters.clone()) {
            (true, params) => self.path.clone().replace_mustache(params),
            _ => self.path.clone(),
        }
    }
    /// See [`Connector::document_type`] for more details.
    fn document_type(&self) -> Box<DocumentType> {
        self.document_type.clone()
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
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "data/not-found-file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn len(&mut self) -> Result<usize> {
        let query = format!(
            "{} {}",
            self.query
                .clone()
                .to_lowercase()
                .split("where")
                .next()
                .unwrap(),
            "limit 1"
        );

        Ok(self
            .fetch_length_by_query_and_metadata(query.clone(), self.metadata())
            .await
            .unwrap_or(0))
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
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
    /// use chewdata::document::DocumentType;
    /// use surf::http::Method;
    /// use chewdata::Metadata;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = BucketSelect::default();
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.path = "data/one_line.json".to_string();
    ///     connector.endpoint = Some("http://localhost:9000".to_string());
    ///     connector.access_key_id = Some("minio_access_key".to_string());
    ///     connector.secret_access_key = Some("minio_secret_key".to_string());
    ///     connector.bucket = "my-bucket".to_string();
    ///     connector.query = "select * from s3object".to_string();
    ///     println!("connector : {:?}", connector);
    ///     connector.fetch().await?;
    ///     println!("inner len : {:?}", connector.inner().len());
    ///     println!("inner  : {:?}", String::from_utf8(connector.inner().to_vec()).unwrap());
    ///     assert!(0 < connector.inner().len(), "The inner connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn fetch(&mut self) -> Result<()> {
        let metadata = self.metadata();

        match (metadata.has_headers, metadata.mime_type.as_deref()) {
            (Some(true), Some("text/csv")) | (Some(true), Some("text/csv; charset=utf-8")) => {
                let mut metadata_header = metadata.clone();
                metadata_header.has_headers = Some(false);
                let headers = self
                    .fetch_data_by_query_and_metadata(
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
                    )
                    .await?;
                self.inner.write_all(headers.as_bytes())?;
            }
            _ => (),
        };

        let body = self
            .fetch_data_by_query_and_metadata(self.query.clone(), metadata)
            .await?;
        self.inner.write_all(body.as_bytes())?;

        // initialize the position of the cursors
        self.inner.set_position(0);

        Ok(())
    }
    /// See [`Connector::push_data`] for more details.
    async fn push_data(&mut self, _data: DataResult) -> Result<()> {
        unimplemented!("Can't push data to the remote document. Use the bucket connector instead of this connector")
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "Can't erase the document. Use the bucket connector instead of this connector"
        )
    }
    /// See [`Connector::send`] for more details.
    async fn send(&mut self) -> Result<()> {
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
    has_next: bool,
}

impl BucketSelectPaginator {
    pub fn new(connector: BucketSelect) -> Result<Self> {
        Ok(BucketSelectPaginator {
            connector,
            has_next: true,
        })
    }
}

#[async_trait]
impl Paginator for BucketSelectPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::bucket_select::BucketSelect;
    /// use chewdata::connector::Connector;
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
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert!(paginator.next_page().await?.is_some(), "Can't get the first reader.");
    ///     assert!(paginator.next_page().await?.is_none(), "Can't paginate more than one time.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>> {
        Ok(match self.has_next {
            true => {
                let mut connector = self.connector.clone();
                self.has_next = false;
                connector.fetch().await?;
                Some(Box::new(connector))
            }
            false => None,
        })
    }
}
