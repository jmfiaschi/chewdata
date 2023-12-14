//! Retrieve the number of documents and parallelized the calls to retreave documents.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                        | Default Value | Possible Values |
//! | ---------- | ----- | ---------------------------------------------------------------------------------- | ------------- | --------------- |
//! | type       | -     | Required in order to use this paginator.                                           | `cursor`      | `cursor`        |
//! | limit      | -     | Limit of records to retrieve for each request.                                     | `100`         | Unsigned number |
//! | skip       | -     | The number of documents to skip before counting.                                   | `0`           | Unsigned number |
//! | count      | -     | The number of documents to retrieve. If null, the connector's counter is used to determine the number of documents. | `null`        | Unsigned number |
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
//!                 "type": "offset",
//!                 "limit": 100,
//!                 "skip": 0,
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
use async_stream::stream;
use mongodb::{bson::doc, options::FindOptions};
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Offset {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub has_next: bool,
}

impl Default for Offset {
    fn default() -> Self {
        Offset {
            limit: 100,
            skip: 0,
            count: None,
            has_next: true,
        }
    }
}

impl Offset {
    /// Offset paginator.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{mongodb::Mongodb, Connector};
    /// use chewdata::connector::paginator::mongodb::offset::Offset;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let paginator = Offset {
    ///         skip: 0,
    ///         limit: 1,
    ///         ..Default::default()
    ///     };
    ///
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset::paginate")]
    pub async fn paginate(&self, connector: &Mongodb) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;
        let count_opt = self.count;

        Ok(Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut find_options = FindOptions::default();
                find_options.skip = Some(skip as u64);
                find_options.limit = Some(limit as i64);
                new_connector.find_options = Box::new(Some(find_options.clone()));

                if let Some(count) = count_opt {
                    if count <= limit + skip {
                        has_next = false;
                    }
                }

                skip += limit;

                trace!(connector = format!("{:?}", new_connector).as_str(), "Yield a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("Stop yielding new connector");
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::document::json::Json;

    use super::*;

    #[async_std::test]
    async fn paginate() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        let mut datastream = connector.fetch(&document).await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        let mut datastream = connector.fetch(&document).await.unwrap().unwrap();
        let data_2 = datastream.next().await.unwrap();

        assert!(
            data_1 != data_2,
            "The content of this two stream are not different."
        );
    }
}
