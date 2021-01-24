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
use crate::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ConnectorType {
    #[serde(rename = "in_memory")]
    #[serde(alias = "mem")]
    #[serde(alias = "m")]
    InMemory(InMemory),
    #[serde(rename = "io")]
    #[serde(alias = "i")]
    Io(Io),
    #[serde(rename = "local")]
    #[serde(alias = "l")]
    Local(Local),
    #[cfg(feature = "use_bucket_connector")]
    #[serde(rename = "bucket")]
    #[serde(alias = "b")]
    Bucket(Bucket),
    #[cfg(feature = "use_bucket_connector")]
    #[serde(rename = "bucket_select")]
    #[serde(alias = "bs")]
    BucketSelect(BucketSelect),
    #[cfg(feature = "use_curl_connector")]
    #[serde(rename = "curl")]
    #[serde(alias = "c")]
    Curl(Curl),
    #[cfg(feature = "use_mongodb_connector")]
    #[serde(rename = "mongodb")]
    Mongodb(Mongodb),
}

impl Default for ConnectorType {
    fn default() -> Self {
        ConnectorType::Io(Io::default())
    }
}

impl std::fmt::Display for ConnectorType {
    /// Display a inner buffer into `Connector`.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{ConnectorType, in_memory::InMemory};
    /// use std::io::Write;
    ///
    /// let mut connector_type = ConnectorType::InMemory(InMemory::new(""));
    /// connector_type.connector_mut().write_all("My text".to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!("My text", format!("{}", connector_type));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorType::InMemory(connector) => write!(f, "{}", connector),
            ConnectorType::Io(connector) => write!(f, "{}", connector),
            ConnectorType::Local(connector) => write!(f, "{}", connector),
            #[cfg(feature = "use_curl_connector")]
            ConnectorType::Curl(connector) => write!(f, "{}", connector),
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::Bucket(connector) => write!(f, "{}", connector),
            #[cfg(feature = "use_bucket_connector")]
            ConnectorType::BucketSelect(connector) => write!(f, "{}", connector),
            #[cfg(feature = "use_mongodb_connector")]
            ConnectorType::Mongodb(connector) => write!(f, "{}", connector),
        }
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
pub trait Connector: Read + Write + Send + std::fmt::Debug {
    /// Set parameters.
    fn set_parameters(&mut self, parameters: Value);
    /// Get the resolved path.
    fn path(&self) -> String;
    /// Get the connect buffer inner reference.
    fn inner(&self) -> &Vec<u8>;
    /// Check if the connector and the document have data.
    fn is_empty(&self) -> Result<bool>;
    /// Append the inner buffer into the end of the document and flush the inner buffer.
    fn seek_and_flush(&mut self, _position: i64) -> Result<()> {
        self.flush()
    }
    /// Get the total document size.
    fn len(&self) -> Result<usize> {
        Err(Error::new(ErrorKind::NotFound, "function not implemented"))
    }
    /// Set the metadata of the connection.
    fn set_metadata(&mut self, _metadata: Metadata) {}
    /// Change the value of the flush_and_read parameter. Used to update the inner with the document content after flush.
    fn set_flush_and_read(&mut self, _flush_and_read: bool) {}
    /// Test if the path is dynamic.
    fn is_variable_path(&self) -> bool;
    /// Erase the content of the document.
    fn erase(&mut self) -> Result<()>;
}
