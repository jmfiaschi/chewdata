//! Retrieve the number of elements from the response and parallelize the HTTP calls.
//! If no element number is found, the paginator iterates until it receives no data, without parallelization.
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
//!             "type": "curl",
//!             "endpoint": "{{ CURL_ENDPOINT }}",
//!             "path": "/get?skip={{ paginator.skip }}&limit={{ paginator.limit }}",
//!             "method": "get",
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
use crate::connector::Paginator;
use crate::connector::{curl::Curl, Connector};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::{Error, ErrorKind, Result},
    pin::Pin,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Offset {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Curl>>,
}

impl Default for Offset {
    fn default() -> Self {
        Offset {
            limit: 100,
            skip: 0,
            count: None,
            connector: None,
        }
    }
}

impl Offset {
    pub fn set_connector(&mut self, connector: Curl) -> &mut Self
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
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::counter::curl::CounterType;
    /// use chewdata::connector::counter::curl::header::Header;
    /// use chewdata::connector::paginator::curl::PaginatorType;
    /// use chewdata::connector::paginator::curl::offset::Offset;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/get".to_string();
    ///     connector.paginator_type = PaginatorType::Offset(Offset::default());
    ///     connector.counter_type = Some(CounterType::Header(Header::new("Content-Length".to_string(), None)));
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert_eq!(Some(194), paginator.count().await?);
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
                "The paginator cannot count the number of elements in the resource without a connector.",
            )),
        }?;

        if let Some(counter_type) = connector.counter_type.clone() {
            self.count = counter_type.count(*connector.clone(), None).await?;

            info!(
                size = self.count,
                "The counter of the connector successfully counts the elements in the resource."
            );
            return Ok(self.count);
        }

        trace!(size = self.count, "The counter of the connector does not exist or is unable to count the number of elements in the resource.");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::paginator::curl::PaginatorType;
    /// use chewdata::connector::paginator::curl::offset::Offset;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/links/{{ paginator.skip }}/10".to_string();
    ///     connector.paginator_type = PaginatorType::Offset(Offset {
    ///         skip: 1,
    ///         limit: 1,
    ///         ..Default::default()
    ///     });
    ///
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::stream")]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let mut paginator = self.clone();
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator cannot paginate without a connector.",
            )),
        }?;

        let mut has_next = true;
        let limit = paginator.limit;
        let mut skip = paginator.skip;

        let count_opt = match paginator.count {
            Some(count) => Some(count),
            None => paginator.count().await?,
        };

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();
                new_parameters.merge_in("/paginator/limit", Value::String(limit.to_string()))?;
                new_parameters.merge_in("/paginator/skip", Value::String(skip.to_string()))?;

                new_connector.set_parameters(new_parameters);

                if let Some(count) = count_opt {
                    if count <= limit + skip {
                        has_next = false;
                    }
                }

                if connector.path() == new_connector.path() {
                    has_next = false;
                }

                skip += limit;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream returns the latest new connector.");
                yield Ok(new_connector as Box<dyn Connector>);
            }
            trace!("The stream stops returning new connectors.");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        self.count.is_some()
    }
}
