//! Retrieve document with a cursor. The paginator cannot be parallelized.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                        | Default Value | Possible Values |
//! | ---------- | ----- | ---------------------------------------------------------------------------------- | ------------- | --------------- |
//! | type       | -     | Required in order to use this paginator.                                           | `cursor`      | `cursor`        |
//! | limit      | -     | Limit of records to retrieve for each request.                                     | `100`         | Unsigned number |
//! | skip       | -     | The number of documents to skip before counting.                                   | `0`           | Unsigned number |
//!
//! ### Example
//!
//! ```json
//! [
//!     {
//!         "type": "w",
//!         "connector":{
//!             "type": "mongodb",
//!             "endpoint": "mongodb://admin:admin@localhost:27017",
//!             "db": "tests",
//!             "collection": "test",
//!             "paginator": {
//!                 "type": "cursor",
//!                 "limit": 100,
//!                 "skip": 0
//!             }
//!         },
//!         "concurrency_limit":3
//!     }
//! ]
//! ```
use crate::{
    connector::{mongodb::Mongodb, Connector},
    ConnectorStream,
};
use async_compat::{Compat, CompatExt};
use async_stream::stream;
use smol::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Client,
};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Cursor {
    pub limit: usize,
    pub skip: usize,
}

impl Default for Cursor {
    fn default() -> Self {
        Cursor {
            limit: 100,
            skip: 0,
        }
    }
}

impl Cursor {
    /// Cursor paginator.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::{mongodb::Mongodb, Connector};
    /// use smol::prelude::*;
    /// use chewdata::connector::paginator::mongodb::cursor::Cursor;
    /// use std::io;
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    /// 
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let paginator = Cursor {
    ///         skip: 0,
    ///         limit: 1,
    ///         ..Default::default()
    ///     };
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "cursor::paginate")]
    pub async fn paginate(&self, connector: &Mongodb) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let hostname = connector.endpoint.clone();
        let database = connector.database.clone();
        let collection = connector.collection.clone();
        let parameters = connector.parameters.clone();
        let skip = self.skip;
        let batch_size = self.limit;

        let mut options = (*connector.find_options.clone()).unwrap_or_default();
        options.skip = Some(skip as u64);

        let filter: Document = match connector.filter(&parameters) {
            Some(filter) => serde_json::from_str(filter.to_string().as_str())?,
            None => Document::new(),
        };

        let client = Client::with_uri_str(&hostname)
            .compat()
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let cursor = Compat::new(async {
            collection
                .find(filter)
                .with_options(Some(options))
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))
        }).await?;
        let cursor_size = cursor.count().await;

        Ok(Box::pin(stream! {
            for i in 0..cursor_size {
                if 0 == i%batch_size || i == cursor_size {
                    let mut new_connector = connector.clone();

                    let mut options = (*new_connector.find_options.clone()).unwrap_or_default();
                    options.skip = Some(i as u64);
                    options.limit = Some(batch_size as i64);

                    new_connector.find_options = Box::new(Some(options.clone()));

                    trace!(connector = format!("{:?}", new_connector).as_str(), "Yield a new connector");
                    yield Ok(Box::new(new_connector) as Box<dyn Connector>);
                }
            }
            trace!("Stop yielding new connector");
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn paginate() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();

        let paginator = Cursor {
            skip: 0,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
    }
    #[apply(test!)]
    async fn paginate_to_end() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();

        let paginator = Cursor {
            skip: 0,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
}
