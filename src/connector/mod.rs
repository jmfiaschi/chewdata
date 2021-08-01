#[cfg(feature = "use_curl_connector")]
pub mod authenticator;
#[cfg(feature = "use_bucket_connector")]
pub mod bucket;
#[cfg(feature = "use_bucket_connector")]
pub mod bucket_select;
#[cfg(feature = "use_curl_connector")]
pub mod curl;
pub mod in_memory;
pub mod io;
pub mod local;
#[cfg(feature = "use_mongodb_connector")]
pub mod mongodb;

#[cfg(feature = "use_bucket_connector")]
use self::bucket::Bucket;
#[cfg(feature = "use_bucket_connector")]
use self::bucket_select::BucketSelect;
#[cfg(feature = "use_curl_connector")]
use self::curl::Curl;
use self::in_memory::InMemory;
use self::io::Io;
use self::local::Local;
#[cfg(feature = "use_mongodb_connector")]
use self::mongodb::Mongodb;
use crate::DataResult;
use crate::{document::DocumentType, Metadata};
use async_std::io::{Read, Write};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use crate::Dataset;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ConnectorType {
    #[serde(rename = "in_memory")]
    #[serde(alias = "mem")]
    InMemory(InMemory),
    #[serde(rename = "io")]
    Io(Io),
    #[serde(rename = "local")]
    Local(Local),
    #[cfg(feature = "use_bucket_connector")]
    #[serde(rename = "bucket")]
    Bucket(Bucket),
    #[cfg(feature = "use_bucket_connector")]
    #[serde(rename = "bucket_select")]
    BucketSelect(BucketSelect),
    #[cfg(feature = "use_curl_connector")]
    #[serde(rename = "curl")]
    Curl(Curl),
    #[cfg(feature = "use_mongodb_connector")]
    #[serde(rename = "mongodb")]
    #[serde(alias = "mongo")]
    Mongodb(Mongodb),
}

impl Default for ConnectorType {
    fn default() -> Self {
        ConnectorType::Io(Io::default())
    }
}

impl ConnectorType {
    pub fn connector_inner(self) -> Box<dyn Connector> {
        match self {
            ConnectorType::InMemory(connector) => Box::new(connector),
            ConnectorType::Io(connector) => Box::new(connector),
            ConnectorType::Local(connector) => Box::new(connector),
            #[cfg(feature = "use_curl_connector")]
            ConnectorType::Curl(connector) => Box::new(connector),
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::Bucket(connector) => Box::new(connector),
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::BucketSelect(connector) => Box::new(connector),
            #[cfg(feature = "use_mongodb_connector")]
            ConnectorType::Mongodb(connector) => Box::new(connector),
        }
    }
    pub fn connector(&self) -> &dyn Connector {
        match self {
            ConnectorType::InMemory(connector) => connector,
            ConnectorType::Io(connector) => connector,
            ConnectorType::Local(connector) => connector,
            #[cfg(feature = "use_curl_connector")]
            ConnectorType::Curl(connector) => connector,
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::Bucket(connector) => connector,
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::BucketSelect(connector) => connector,
            #[cfg(feature = "use_mongodb_connector")]
            ConnectorType::Mongodb(connector) => connector,
        }
    }
    pub fn connector_mut(&mut self) -> &mut dyn Connector {
        match self {
            ConnectorType::InMemory(connector) => connector,
            ConnectorType::Io(connector) => connector,
            ConnectorType::Local(connector) => connector,
            #[cfg(feature = "use_curl_connector")]
            ConnectorType::Curl(connector) => connector,
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::Bucket(connector) => connector,
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::BucketSelect(connector) => connector,
            #[cfg(feature = "use_mongodb_connector")]
            ConnectorType::Mongodb(connector) => connector,
        }
    }
}

/// Struct that implement this trait can get a reader or writer in order to do something on a document.
#[async_trait]
pub trait Connector: Send + Sync + std::fmt::Debug + ConnectorClone + Unpin + Read + Write {
    // Fetch data from the remote resource and set the inner of the connector.
    async fn fetch(&mut self) -> Result<()>;
    // Pull the data from the inner connector, transform the data with the document type and return data as a stream.
    async fn pull_data(
        &mut self,
    ) -> std::io::Result<Dataset> {
        debug!(slog_scope::logger(), "pull data");
        let mut paginator = self.paginator().await?;
        let document = self.document_type().document_inner();

        Ok(Box::pin(stream! {
            while let Some(ref mut connector_reader) = match paginator.next_page().await {
                Ok(connector_option) => connector_option,
                Err(e) => {
                    error!(slog_scope::logger(), "Can't get the next paginator"; "error" => e);
                    None
                }
            } {
                debug!(slog_scope::logger(), "Next page started"; "connector" => format!("{:?}", connector_reader));
                let mut data = match document.read_data(connector_reader).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!(slog_scope::logger(), "Can't pull the data"; "connector" => format!("{:?}", connector_reader), "error" => e);
                        break;
                    }
                };
                while let Some(data_result) = data.next().await {
                    yield data_result;
                }
                debug!(slog_scope::logger(), "Next page ended"; "connector" => format!("{:?}", connector_reader));
            }
        }))
    }
    // Push the data into the inner connector and format it with the document type link to the connector.
    async fn push_data(&mut self, data: DataResult) -> Result<()>;
    // Send the data from the inner connector to the remote resource.
    async fn send(&mut self) -> Result<()>;
    fn is_resource_will_change(&self, new_parameters: Value) -> Result<bool>;
    /// Set parameters.
    fn set_parameters(&mut self, parameters: Value);
    /// Set metadata
    fn set_metadata(&mut self, _metadata: Metadata) {}
    /// Test if the connector is variable and if the context change, the resource will change.
    fn is_variable(&self) -> bool;
    /// Check if remote document is empty.
    async fn is_empty(&mut self) -> Result<bool> {
        Err(Error::new(ErrorKind::NotFound, "function not implemented"))
    }
    /// Get the remote document size of the current path.
    async fn len(&mut self) -> Result<usize> {
        Err(Error::new(ErrorKind::NotFound, "function not implemented"))
    }
    /// Path of the document
    fn path(&self) -> String;
    /// Intitialize the paginator and return it. The paginator loop on a list of Reader.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>>;
    fn document_type(&self) -> DocumentType;
    /// Erase the content of the document.
    async fn erase(&mut self) -> Result<()> {
        Err(Error::new(ErrorKind::NotFound, "function not implemented"))
    }
    /// clear the inner
    fn clear(&mut self);
    /// Get the connect buffer inner reference.
    fn inner(&self) -> &Vec<u8>;
    /// Check if the inner contain data
    fn inner_has_data(&self) -> bool {
        let document = self.document_type().document_inner();
        let inner = self.inner();

        document.has_data(std::str::from_utf8(&inner).unwrap())
    }
    /// Get the connector metadata
    fn metadata(&self) -> Metadata {
        Metadata::default()
    }
    // Get the current position in the remote document that can be used by a cursor
    async fn current_position(&self) -> Result<usize> {
        Ok(0)
    }
}

impl fmt::Display for dyn Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}
pub trait ConnectorClone {
    fn clone_box(&self) -> Box<dyn Connector>;
}

impl<T> ConnectorClone for T
where
    T: 'static + Connector + Clone,
{
    fn clone_box(&self) -> Box<dyn Connector> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Connector> {
    fn clone(&self) -> Box<dyn Connector> {
        self.clone_box()
    }
}

#[async_trait]
pub trait Paginator: std::fmt::Debug + Unpin {
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>>;
}
