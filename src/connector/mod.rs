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
use crate::document::Document;
use crate::Dataset;
use crate::Metadata;
use async_std::io::{Read, Write};
use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;

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
    pub fn boxed_inner(self) -> Box<dyn Connector> {
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
}

/// Struct that implement this trait can get a reader or writer in order to do something on a document.
#[async_trait]
pub trait Connector: Send + Sync + std::fmt::Debug + ConnectorClone + Unpin + Read + Write {
    // Fetch data from the resource and set the inner of the connector.
    async fn fetch(&mut self) -> Result<()>;
    // Return the dataset that contain a stream of data.
    #[instrument]
    async fn dataset(&mut self, document: Box<dyn Document>) -> std::io::Result<Option<Dataset>> {
        let mut connector = self.clone_box();
    
        match document.has_data(self.inner())? {
            false => return Ok(None),
            true => ()
        };

        Ok(Some(document.read_data(&mut connector).await?))
    }
    // Send the data from the inner connector to the remote resource.
    async fn send(&mut self, position: Option<isize>) -> Result<()>;
    fn is_resource_will_change(&self, new_parameters: Value) -> Result<bool>;
    /// Set parameters.
    fn set_parameters(&mut self, parameters: Value);
    /// Set the connector metadata that can change with the document metadata.
    fn set_metadata(&mut self, _metadata: Metadata) {}
    /// Get the connector metadata
    fn metadata(&self) -> Metadata {
        Metadata::default()
    }
    /// Test if the connector is variable and if the context change, the resource will change.
    fn is_variable(&self) -> bool;
    /// Check if the resource is empty.
    async fn is_empty(&mut self) -> Result<bool> {
        Err(Error::new(ErrorKind::Unsupported, "function not implemented"))
    }
    /// Get the resource size of the current path.
    async fn len(&mut self) -> Result<usize> {
        Err(Error::new(ErrorKind::Unsupported, "function not implemented"))
    }
    /// Path of the document
    fn path(&self) -> String;
    /// Intitialize the paginator and return it. The paginator loop on a list of Reader.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>>;
    /// Erase the content of the resource.
    async fn erase(&mut self) -> Result<()> {
        Err(Error::new(ErrorKind::Unsupported, "function not implemented"))
    }
    /// clear the inner
    fn clear(&mut self);
    /// Get the connect buffer inner reference.
    fn inner(&self) -> &Vec<u8>;
    /// Return a chunk of bytes with a start and end position in the document.
    async fn chunk(&self, _start: usize, _end: usize) -> Result<Vec<u8>> {
        Err(Error::new(ErrorKind::Unsupported, "function not implemented"))
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
    /// Update the document in the paginator. Used to find the total of items in a payload
    fn set_document(&mut self, _document: Box<dyn Document>) {}
    /// Get the stream of connectors
    async fn stream(&mut self) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>>;
    /// Try to fetch the number of item in the connector.
    /// None: Can't retrieve the total of item for any reason.
    /// Some(count): Retrieve the total of item and store the value in the paginator.
    async fn count(&mut self) -> Result<Option<usize>>;
    /// Test if the paginator can be parallelizable.
    fn is_parallelizable(&mut self) -> bool;
}
