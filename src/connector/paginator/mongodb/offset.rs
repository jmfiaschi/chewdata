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
//!         "thread_number":3
//!     }
//! ]
//! ```
use crate::connector::mongodb::CounterType;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use mongodb::{bson::doc, options::FindOptions};
use serde::{Deserialize, Serialize};
use std::{
    io::{Error, ErrorKind, Result},
    pin::Pin,
};

use crate::connector::{mongodb::Mongodb, paginator::Paginator, Connector};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Offset {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Mongodb>>,
    #[serde(skip)]
    pub has_next: bool,
}

impl Default for Offset {
    fn default() -> Self {
        Offset {
            limit: 100,
            skip: 0,
            count: None,
            connector: None,
            has_next: true,
        }
    }
}

impl Offset {
    pub fn set_connector(&mut self, connector: Mongodb) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for Offset {
    /// See [`Paginator::count`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{mongodb::{Mongodb, CounterType, Metadata}, Connector};
    /// use chewdata::connector::paginator::mongodb::offset::Offset;
    /// use chewdata::connector::paginator::mongodb::PaginatorType;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(Offset::default());
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.count().await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::count")]
    async fn count(&mut self) -> Result<Option<usize>> {
        let connector = match self.connector {
            Some(ref mut connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't count the number of element in the collection without a connector",
            )),
        }?;

        let mut counter_type = None;
        if connector.counter_type.is_none() {
            counter_type = Some(CounterType::default());
        }

        if let Some(counter_type) = counter_type {
            self.count = counter_type.count(*connector.clone(), None).await?;

            info!(
                size = self.count,
                "The connector's counter count elements in the collection with success"
            );
            return Ok(self.count);
        }

        trace!(size = self.count, "The connector's counter not exist or can't count the number of elements in the collection");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::{mongodb::Mongodb, Connector};
    /// use chewdata::connector::paginator::mongodb::offset::Offset;
    /// use chewdata::connector::paginator::mongodb::PaginatorType;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(Offset {
    ///         skip: 0,
    ///         limit: 1,
    ///         ..Default::default()
    ///     });
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::stream")]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let mut paginator = self.clone();
        let connector = match paginator.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;

        let count_opt = match paginator.count {
            Some(count) => Some(count),
            None => paginator.count().await?,
        };

        let stream = Box::pin(stream! {
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

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                yield Ok(new_connector as Box<dyn Connector>);
            }
            trace!("The stream stop to return new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        self.count.is_some()
    }
}