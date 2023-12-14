//! Retreive the number of records through the mongodb metadata.
//!
//! ### Confirguration
//!
//! | key  | alias | Description                            | Default Value | Possible Values |
//! | ---- | ----- | -------------------------------------- | --------------| ----------------|
//! | type | -     | Required in order to use this counter. | `metadata`    | `metadata`      |
//!
//! ### Example
//!
//!  ```json
//!  [
//!      {
//!          "type": "read",
//!          "connector":{
//!              "type": "psql",
//!              "endpoint": "mongodb://admin:admin@localhost:27017",
//!              "database": "local",
//!              "collection": "startup_log",
//!              "paginator": {
//!                  "type": "offset",
//!                  "limit": 100,
//!                  "skip": 0,
//!                  "count": null
//!              },
//!              "counter": {
//!                  "type": "metadata"
//!              }
//!          }
//!      }
//!  ]
//!  ```
use mongodb::{bson::Document, Client};
use serde::{Deserialize, Serialize};

use crate::connector::mongodb::Mongodb;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Metadata {}

impl Metadata {
    /// Get the number of items from the collection metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::mongodb::Mongodb;
    /// use async_std::prelude::*;
    /// use std::io;
    /// use chewdata::connector::counter::mongodb::metadata::Metadata;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let counter = Metadata::default();
    ///     assert!(0 < counter.count(&connector).await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "metadata::count")]
    pub async fn count(&self, connector: &Mongodb) -> Result<usize> {
        let client = match Client::with_uri_str(&connector.endpoint).await {
            Ok(client) => client,
            Err(e) => return Err(Error::new(ErrorKind::Interrupted, e)),
        };
        let db = client.database(&connector.database);
        let collection = db.collection::<Document>(&connector.collection);
        let count = collection
            .estimated_document_count(None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        trace!(count = count, "Count with success");

        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn count() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        let counter = Metadata::default();
        assert!(0 < counter.count(&connector).await.unwrap());
    }
}
