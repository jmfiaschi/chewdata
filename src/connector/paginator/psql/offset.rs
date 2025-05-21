//! Retrieve the number of elements.
//!
//!
//! ### Configuration
//!
//! | key   | alias | Description                                                | Default Value | Possible Values |
//! | ----- | ----- | ---------------------------------------------------------- | ------------- | --------------- |
//! | type  | -     | Required in order to use this paginator                    | `offset`      | `offset`        |
//! | limit | -     | Limit of records to retrieve for each call                 | `100`         | Unsigned number |
//! | skip  | -     | Skip a number of records and retrieve the rest of records  | `0`           | Unsigned number |
//! | count | -     | Total of records to retrieve before to stop the pagination | `null`        | Unsigned number |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "connector":{
//!             "type": "psql",
//!             "endpoint": "psql://admin:admin@localhost:27017",
//!             "database": "local",
//!             "collection": "startup_log",
//!             "paginator": {
//!                 "type": "offset",
//!                 "limit": 100,
//!                 "skip": 0,
//!                 "count": 20000
//!             }
//!         }
//!     }
//! ]
//! ```
use crate::{
    connector::{psql::Psql, Connector},
    ConnectorStream,
};
use async_stream::stream;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Offset {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
}

impl Default for Offset {
    fn default() -> Self {
        Offset {
            limit: 100,
            skip: 0,
            count: None,
        }
    }
}

impl Offset {
    /// Paginate through the connector.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{psql::Psql, Connector};
    /// use chewdata::connector::paginator::psql::offset::Offset;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    /// 
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost:27017".into();
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
    pub async fn paginate(&self, connector: &Psql) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;
        let query = connector
            .query
            .clone()
            .unwrap_or_else(|| "SELECT * FROM {{ collection }}".to_string());
        let count_opt = self.count;

        Ok(Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();

                new_connector.query = Some(format!("SELECT * from ({}) as paginator LIMIT {} OFFSET {};", query.clone(), limit, skip));

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
    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test;
    use smol::stream::StreamExt;
    use crate::document::json::Json;

    #[apply(test!)]
    async fn paginate() {
        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();
        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the second reader."
        );
    }
    #[apply(test!)]
    async fn paginate_with_skip_and_limit() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        let mut datastream = connector.fetch().await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        let mut datastream = connector.fetch().await.unwrap().unwrap();
        let data_2 = datastream.next().await.unwrap();
        assert!(
            data_1 != data_2,
            "The content of this two stream is not different."
        );
    }
}
