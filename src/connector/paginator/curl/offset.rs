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
use crate::connector::curl::Curl;
use crate::connector::Connector;
use crate::connector::Paginator;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{io::Error, io::ErrorKind, io::Result, pin::Pin};

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

#[async_trait]
impl Paginator for Offset {
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::paginator::curl::offset::Offset;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use crate::chewdata::connector::paginator::Paginator;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/get".to_string();
    ///    
    ///     let paginator = Offset {
    ///         connector: Some(Box::new(connector)),
    ///         ..Default::default()
    ///     };
    ///
    ///     assert!(!paginator.is_parallelizable());
    ///
    ///     let mut stream = paginator.stream().await.unwrap();
    ///     let connector = stream.next().await.transpose().unwrap();
    ///     assert!(connector.is_some());
    ///     let connector = stream.next().await.transpose().unwrap();
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::stream")]
    async fn stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let paginator = self.clone();
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = paginator.limit;
        let mut skip = paginator.skip;
        let count_opt = paginator.count;

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();
                new_parameters.merge_in("/paginator/limit", &Value::String(limit.to_string()))?;
                new_parameters.merge_in("/paginator/skip", &Value::String(skip.to_string()))?;

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

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream yields a new connector.");
                yield Ok(new_connector as Box<dyn Connector>);
            }
            trace!("The stream stops yielding new connectors.");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        self.count.is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::curl::Curl;
    #[cfg(feature = "xml")]
    use crate::document::xml::Xml;
    use futures::StreamExt;
    use http_types::Method;

    use super::*;

    #[cfg(feature = "xml")]
    #[async_std::test]
    async fn stream() {
        use crate::connector::curl::Curl;

        let mut document = Xml::default();
        document.entry_path = "/html/body/*/a".to_string();

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();

        let paginator = Offset {
            skip: 1,
            limit: 1,
            connector: Some(Box::new(connector)),
            ..Default::default()
        };

        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();

        let mut connector = stream.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/1/10", connector.path().as_str());
        let len1 = connector
            .fetch(&document)
            .await
            .unwrap()
            .unwrap()
            .count()
            .await;
        assert!(0 < len1, "Can't read the content of the file.");

        let mut connector = stream.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/2/10", connector.path().as_str());
        let len2 = connector
            .fetch(&document)
            .await
            .unwrap()
            .unwrap()
            .count()
            .await;
        assert!(0 < len2, "Can't read the content of the file.");

        assert!(
            len1 != len2,
            "The content of this two files is not different."
        );
    }
    #[async_std::test]
    async fn stream_one_time() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();

        let paginator = Offset {
            connector: Some(Box::new(connector)),
            ..Default::default()
        };

        assert!(!paginator.is_parallelizable());

        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
    #[async_std::test]
    async fn stream_tree_times_and_parallize() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            count: Some(3),
            connector: Some(Box::new(connector)),
            ..Default::default()
        };

        assert!(paginator.is_parallelizable());

        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
}
