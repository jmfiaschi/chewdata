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
use crate::connector::Connector;
use crate::{connector::curl::Curl, ConnectorStream};
use async_stream::stream;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol::stream::StreamExt;
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
    /// Offset paginator.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::paginator::curl::offset::Offset;
    /// use smol::prelude::*;
    /// use std::io;
    /// use http::Method;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::GET;
    ///     connector.path = "/get".to_string();
    ///    
    ///     let paginator = Offset {
    ///         ..Default::default()
    ///     };
    ///
    ///     let mut paging = paginator.paginate(&connector).await.unwrap();
    ///     let connector = paging.next().await.transpose().unwrap();
    ///     assert!(connector.is_some());
    ///     let connector = paging.next().await.transpose().unwrap();
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset::paginate")]
    pub async fn paginate(&self, connector: &Curl) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let limit = self.limit;
        let mut skip = self.skip;
        let count_opt = self.count;

        let stream = Box::pin(stream! {
            let mut has_next = true;

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

                // Loop until the connector stop to return data. Last check to avoid infinit loop.
                // Define a counter will avoid to enter in this check.
                if has_next && count_opt.is_none() {
                    let mut dataset = match new_connector.fetch().await? {
                        Some(dataset) => dataset,
                        None => break
                    };

                    let data_opt = dataset.next().await;

                    match data_opt {
                        Some(_) => (),
                        None => break,
                    };
                }

                skip += limit;

                trace!(connector = format!("{:?}", new_connector), "Yield a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("Stop yielding new connector");
        });

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::curl::Curl;
    use crate::document::json::Json;
    #[cfg(feature = "xml")]
    use crate::document::xml::Xml;
    use http::Method;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

    use super::*;

    #[cfg(feature = "xml")]
    #[apply(test!)]
    async fn paginate() {
        use http::Method;

        let mut document = Xml::default();
        document.entry_path = "/html/body/*/a".to_string();

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::GET;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Offset {
            skip: 1,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/1/10", connector.path().as_str());
        let len1 = connector.fetch().await.unwrap().unwrap().count().await;
        assert!(0 < len1, "Can't read the content of the file.");

        let mut connector = paging.next().await.transpose().unwrap().unwrap();
        assert_eq!("/links/2/10", connector.path().as_str());
        let len2 = connector.fetch().await.unwrap().unwrap().count().await;
        assert!(0 < len2, "Can't read the content of the file.");

        assert!(
            len1 != len2,
            "The content of this two files is not different."
        );
    }
    #[apply(test!)]
    async fn paginate_one_time() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::GET;
        connector.path = "/get".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Offset {
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
    #[apply(test!)]
    async fn paginate_tree_times_and_parallize() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::GET;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            count: Some(3),
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
    #[apply(test!)]
    async fn paginate_until_reach_the_end() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::GET;
        connector.path = "/links/{{ paginator.skip }}/10".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Offset {
            skip: 0,
            limit: 1,
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
    }
}
