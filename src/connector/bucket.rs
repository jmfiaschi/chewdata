use crate::connector::Connector;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use futures::FutureExt;
use http::status::StatusCode;
use regex::Regex;
use rusoto_core::{credential::StaticProvider, Region, RusotoError};
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, PutObjectRequest, S3Client, S3 as RusotoS3};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use async_trait::async_trait;
use async_std::io::{Cursor, Write, prelude::WriteExt, SeekFrom};
use std::pin::Pin;
use std::task::{Poll, Context};
use tokio::io::AsyncReadExt;
use async_std::prelude::*;

#[derive(Debug, Deserialize, Serialize)]
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
    pub path: String,
    pub parameters: Value,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
}

impl Default for Bucket {
    fn default() -> Self {
        Bucket {
            metadata: Metadata::default(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            region: Region::default().name().to_owned(),
            bucket: "".to_owned(),
            path: "".to_owned(),
            inner: Cursor::new(Vec::default()),
            parameters: Value::Null,
        }
    }
}

impl Clone for Bucket {
    fn clone(&self) -> Self {
        Bucket {
            metadata: self.metadata.to_owned(),
            endpoint: self.endpoint.to_owned(),
            access_key_id: self.access_key_id.to_owned(),
            secret_access_key: self.secret_access_key.to_owned(),
            region: self.region.to_owned(),
            bucket: self.bucket.to_owned(),
            path: self.path.to_owned(),
            inner: Cursor::new(Vec::default()),
            parameters: self.parameters.to_owned(),
        }
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}

impl Bucket {
    fn s3_client(&self) -> S3Client {
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
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
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
    /// Initilize the inner buffer.
    async fn initialize(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Init inner buffer");
        let connector = self.clone();
        let s3_client = connector.s3_client();
        let request = GetObjectRequest {
            bucket: connector.bucket.clone(),
            key: connector.path(),
            ..Default::default()
        };

        let response = s3_client
            .get_object(request)
            .await
            .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

        let result: Result<String> = match response.body {
            Some(body) => {
                let mut buffer = String::new();
                let mut async_read = body.into_async_read();
                async_read.read_to_string(&mut buffer).await?;
                Ok(buffer)
            }
            None => Ok(String::default()),
        };

        self.inner.write_all(result?.as_bytes()).await?;
        // initialize the position of the cursor
        self.inner.set_position(0);
        debug!(slog_scope::logger(), "Init inner buffer ended");

        Ok(())
    }
}

#[async_trait]
impl Connector for Bucket {
    /// Set the path parameters.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
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
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Bucket::default();
    /// connector.endpoint = Some("http://localhost:9000".to_string());
    /// connector.access_key_id = Some("minio_access_key".to_string());
    /// connector.secret_access_key = Some("minio_secret_key".to_string());
    /// connector.bucket = "my-bucket".to_string();
    /// connector.path = "data/one_line.json".to_string();
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
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Bucket::default();
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
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Bucket::default();
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
    /// Seek the position into the document, append the inner buffer data and flush the connector.
    ///
    /// # Example: Seek from the end
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Bucket::default();
    /// connector_write.endpoint = Some("http://localhost:9000".to_string());
    /// connector_write.access_key_id = Some("minio_access_key".to_string());
    /// connector_write.secret_access_key = Some("minio_secret_key".to_string());
    /// connector_write.bucket = "my-bucket".to_string();
    /// connector_write.path = "data/out/test_bucket_seek_and_flush_1".to_string();
    /// connector_write.erase();
    ///
    /// connector_write.write(r#"[{"column1":"value1"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    /// connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    /// ```
    /// # Example: Seek from the start
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Bucket::default();
    /// connector_write.endpoint = Some("http://localhost:9000".to_string());
    /// connector_write.access_key_id = Some("minio_access_key".to_string());
    /// connector_write.secret_access_key = Some("minio_secret_key".to_string());
    /// connector_write.bucket = "my-bucket".to_string();
    /// connector_write.path = "data/out/test_bucket_seek_and_flush_2".to_string();
    /// connector_write.erase();
    ///
    /// let str = r#"[{"column1":"value1"}]"#;
    /// connector_write.write(str.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush(-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    /// connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.seek_and_flush((str.len() as i64)-1).unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    /// ```
    async fn seek_and_flush(&mut self, position: i64) -> Result<()> {
        debug!(slog_scope::logger(), "Seek & Flush");
        if self.is_variable_path()
            && self.parameters == Value::Null
            && self.inner.get_ref().is_empty()
        {
            warn!(slog_scope::logger(), "Can't flush with variable path and without parameters";"path"=>self.path.clone(),"parameters"=>self.parameters.to_string());
            return Ok(());
        }

        let mut position = position;

        if 0 >= (self.len().await? as i64 + position) {
            position = 0;
        }

        let mut content_file = Vec::default();
        let path_resolved = self.path();

        if 0 != position {
            info!(slog_scope::logger(), "Fetch previous data into S3"; "path" => path_resolved.to_string());
            {
                let mut connector_clone = self.clone();
                connector_clone.read_to_end(&mut content_file).await?;
            }
        }

        let mut cursor = Cursor::new(content_file);
        if 0 < position {
            cursor.seek(SeekFrom::Start(position as u64)).await?;
        }
        if 0 > position {
            cursor.seek(SeekFrom::End(position as i64)).await?;
        }
        cursor.write_all(self.inner.get_ref()).await?;

        let s3_client = self.s3_client();
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved,
            body: Some(cursor.into_inner().into()),
            ..Default::default()
        };

        match s3_client.put_object(put_request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::NotFound, e)),
        }?;

        self.inner.flush().await?;
        self.inner = Cursor::new(Vec::default());

        info!(slog_scope::logger(), "Seek & Flush ended");
        Ok(())
    }
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    async fn erase(&mut self) -> Result<()> {
        info!(slog_scope::logger(), "Clean the document"; "connector" => format!("{}", self), "path" => self.path());
        let path_resolved = self.path();
        let s3_client = self.s3_client();
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved,
            body: Some(Vec::default().into()),
            ..Default::default()
        };

        match s3_client.put_object(put_request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::NotFound, e)),
        }
    }
}

#[async_trait]
impl async_std::io::Read for Bucket {
    /// Fetch the document from the bucket and push it into the inner memory and read it.
    ///
    /// # Example:
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector = Bucket::default();
    /// connector.path = "data/one_line.json".to_string();
    /// connector.endpoint = Some("http://localhost:9000".to_string());
    /// connector.access_key_id = Some("minio_access_key".to_string());
    /// connector.secret_access_key = Some("minio_secret_key".to_string());
    /// connector.bucket = "my-bucket".to_string();
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
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
impl Write for Bucket {
    /// Write the data into the inner buffer before to flush it.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use std::io::Write;
    ///
    /// let mut connector = Bucket::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    /// Write all into the document and flush the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::bucket::Bucket;
    /// use chewdata::connector::Connector;
    /// use std::io::{Read, Write};
    ///
    /// let mut connector_write = Bucket::default();
    /// connector_write.endpoint = Some("http://localhost:9000".to_string());
    /// connector_write.access_key_id = Some("minio_access_key".to_string());
    /// connector_write.secret_access_key = Some("minio_secret_key".to_string());
    /// connector_write.bucket = "my-bucket".to_string();
    /// connector_write.path = "data/out/test_bucket_flush_1".to_string();
    /// connector_write.erase();
    ///
    /// connector_write.write(r#"{"column1":"value1"}"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.flush().unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column1":"value1"}"#, buffer);
    ///
    /// connector_write.write(r#"{"column1":"value2"}"#.to_string().into_bytes().as_slice()).unwrap();
    /// connector_write.flush().unwrap();
    /// let mut buffer = String::default();
    /// let mut connector_read = connector_write.clone();
    /// connector_read.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column1":"value1"}{"column1":"value2"}"#, buffer);
    /// ```
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        debug!(slog_scope::logger(), "Flush started");

        if self.is_variable_path()
            && self.parameters == Value::Null
            && self.inner.get_ref().is_empty()
        {
            warn!(slog_scope::logger(), "Can't flush with variable path and without parameters";"path"=>self.path.clone(),"parameters"=>self.parameters.to_string());
            return Poll::Ready(Ok(()));
        }

        let mut content_file = Vec::default();
        let path_resolved = self.path();

        // Try to fetch the content of the document if exist in the bucket.
        info!(slog_scope::logger(), "Fetch previous data into S3"; "path" => path_resolved.to_string());
        let mut connector_clone = self.clone();
        connector_clone.inner.set_position(0);
        match connector_clone
            .read_to_end(&mut content_file)
            .poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, e))),
                Poll::Pending => return Poll::Pending
            };

        // if the content_file is not empty, append the inner buffer into the content_file.
        content_file.append(&mut self.inner.clone().into_inner());

        let s3_client = self.s3_client();
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved,
            body: Some(content_file.into()),
            ..Default::default()
        };

        match s3_client
            .put_object(put_request)
            .poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, e))),
                Poll::Pending => return Poll::Pending
            };

        match self
            .inner
            .flush()
            .poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, e))),
                Poll::Pending => return Poll::Pending
            };
        self.inner = Cursor::new(Vec::default());

        debug!(slog_scope::logger(), "Flush ended");
        Poll::Ready(Ok(()))
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>{
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
