#[cfg(feature = "curl")]
pub mod authenticator;
#[cfg(feature = "bucket")]
pub mod bucket;
#[cfg(feature = "bucket")]
pub mod bucket_select;
#[cfg(feature = "curl")]
pub mod curl;
pub mod in_memory;
pub mod io;
pub mod local;
#[cfg(feature = "mongodb")]
pub mod mongodb;
// #[cfg(feature = "psql")]
// pub mod psql;
// #[cfg(feature = "mysql")]
// pub mod mysql;
// #[cfg(feature = "mssql")]
// pub mod mssql;
// #[cfg(feature = "sqlite")]
// pub mod sqlite;

#[cfg(feature = "bucket")]
use self::bucket::Bucket;
#[cfg(feature = "bucket")]
use self::bucket_select::BucketSelect;
#[cfg(feature = "curl")]
use self::curl::Curl;
use self::in_memory::InMemory;
use self::io::Io;
use self::local::Local;
#[cfg(feature = "mongodb")]
use self::mongodb::Mongodb;
use crate::DataSet;
use crate::document::Document;
use crate::DataStream;
use crate::Metadata;
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
    #[cfg(feature = "bucket")]
    #[serde(rename = "bucket")]
    Bucket(Bucket),
    #[cfg(feature = "bucket")]
    #[serde(rename = "bucket_select")]
    BucketSelect(BucketSelect),
    #[cfg(feature = "curl")]
    #[serde(rename = "curl")]
    Curl(Curl),
    #[cfg(feature = "mongodb")]
    #[serde(rename = "mongodb")]
    #[serde(alias = "mongo")]
    Mongodb(Mongodb),
    // #[cfg(feature = "psql")]
    // #[serde(rename = "psql")]
    // #[serde(alias = "pgsql")]
    // #[serde(alias = "pg")]
    // Psql(Psql),
    // #[cfg(feature = "mysql")]
    // #[serde(rename = "mysql")]
    // Mysql(Mysql),
    // #[cfg(feature = "mssql")]
    // #[serde(rename = "mssql")]
    // Mssql(Mssql),
    // #[cfg(feature = "sqlite")]
    // #[serde(rename = "sqlite")]
    // Sqlite(Sqlite),
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
            #[cfg(feature = "curl")]
            ConnectorType::Curl(connector) => Box::new(connector),
            #[cfg(feature = "bucket")]
            ConnectorType::Bucket(connector) => Box::new(connector),
            #[cfg(feature = "bucket")]
            ConnectorType::BucketSelect(connector) => Box::new(connector),
            #[cfg(feature = "mongodb")]
            ConnectorType::Mongodb(connector) => Box::new(connector),
            // #[cfg(feature = "psql")]
            // ConnectorType::Psql(connector) => Box::new(connector),
            // #[cfg(feature = "mysql")]
            // ConnectorType::Mysql(connector) => Box::new(connector),
            // #[cfg(feature = "mssql")]
            // ConnectorType::Mssql(connector) => Box::new(connector),
            // #[cfg(feature = "sqlite")]
            // ConnectorType::Sqlite(connector) => Box::new(connector),
        }
    }
}

/// Struct that implement this trait can get a reader or writer in order to do something on a document.
#[async_trait]
pub trait Connector: Send + Sync + std::fmt::Debug + ConnectorClone + Unpin {
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
        Ok(0 == self.len().await?)
    }
    /// Get the resource size of the current path.
    async fn len(&mut self) -> Result<usize> {
        Ok(0)
    }
    /// Path of the document
    fn path(&self) -> String;
    /// Fetch data from the resource and set the inner of the connector.
    async fn fetch(&mut self, document: Box<dyn Document>) -> std::io::Result<Option<DataStream>>;
    /// Send the data from the inner connector to the remote resource.
    async fn send(&mut self, document: Box<dyn Document>, dataset: &DataSet) -> std::io::Result<Option<DataStream>>;
    /// Erase the content of the resource.
    async fn erase(&mut self) -> Result<()> {
        Err(Error::new(ErrorKind::Unsupported, "function not implemented"))
    }
    /// Intitialize the paginator and return it. The paginator loop on a list of Reader.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>>;
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
    fn is_parallelizable(&self) -> bool;
}
