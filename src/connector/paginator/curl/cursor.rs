//! Retrieve the token for the next call from the response. The paginator cannot be parallelized.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                        | Default Value | Possible Values |
//! | ---------- | ----- | ---------------------------------------------------------------------------------- | ------------- | --------------- |
//! | type       | -     | Required in order to use this paginator.                                            | `cursor`      | `cursor`        |
//! | limit      | -     | Limit of records to retrieve for each request.                                         | `100`         | Unsigned number |
//! | entry_path | -     | The entry path for capturing the token in the response's body. | `/next`       | String          |
//! | next       | -     | Force to start the pagination in a specifique position.                             | `null`        | String          |
//!
//! ### Example
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "connector":{
//!             "type": "curl",
//!             "endpoint": "{{ CURL_ENDPOINT }}",
//!             "path": "/get?next={{ paginator.next }}",
//!             "method": "get",
//!             "paginator": {
//!                 "type": "cursor",
//!                 "limit": 10,
//!                 "entry_path": "/next",
//!                 "next": "e5f705e2-5ed8-11ed-9b6a-0242ac120002"
//!             }
//!         }
//!     }
//! ]
//! ```
//!
//! Response body:
//!
//! ```json
//! {
//!     "data": [
//!         ...
//!     ],
//!     "previous": "22d05674-5ed6-11ed-9b6a-0242ac120002",
//!     "next": "274b5dac-5ed6-11ed-9b6a-0242ac120002"
//! }
//! ```
use crate::connector::Connector;
use crate::{connector::curl::Curl, ConnectorStream};
use smol::stream::StreamExt;
use async_stream::stream;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Cursor {
    pub limit: usize,
    pub entry_path: String,
    #[serde(rename = "next")]
    pub next_token: Option<String>,
}

impl Default for Cursor {
    fn default() -> Self {
        Cursor {
            limit: 100,
            next_token: None,
            entry_path: "/next".to_string(),
        }
    }
}

impl Cursor {
    /// Cursor paginate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::paginator::curl::{PaginatorType, cursor::Cursor};
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    /// 
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = "GET".into();
    ///     connector.path = "/uuid?next={{ paginator.next }}".to_string();
    ///
    ///     let paginator = Cursor {
    ///         limit: 1,
    ///         entry_path: "/uuid".to_string(),
    ///         ..Default::default()
    ///     };
    ///
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///
    ///     assert!(paging.next().await.transpose()?.is_some());
    ///     assert!(paging.next().await.transpose()?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "cursor::paginate")]
    pub async fn paginate(&self, connector: &Curl) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let limit = self.limit;
        let entry_path = self.entry_path.clone();
        let mut next_token_opt = self.next_token.clone();

        let mut document = connector.document()?.clone();
        document.set_entry_path(entry_path.clone());

        let stream = Box::pin(stream! {
            let mut has_next = true;
            
            while has_next {
                let mut new_connector = connector.clone();
                new_connector.set_document(document.clone())?;

                let mut new_parameters = connector.parameters.clone();

                if let Some(next_token) = next_token_opt {
                    new_parameters.merge_in("/paginator/next", &Value::String(next_token))?;
                } else {
                    new_parameters.merge_in("/paginator/next", &Value::String("".to_string()))?;
                }

                new_parameters
                    .merge_in("/paginator/limit", &Value::String(limit.to_string()))?;
                new_connector.set_parameters(new_parameters);

                let mut dataset = match new_connector.fetch().await? {
                    Some(dataset) => dataset,
                    None => break
                };

                let data_opt = dataset.next().await;

                let value = match data_opt {
                    Some(data) => data.to_value(),
                    None => Value::Null,
                };

                next_token_opt = match value {
                    Value::Number(_) => Some(value.to_string()),
                    Value::String(string) => Some(string),
                    _ => None,
                };

                if next_token_opt.is_none() {
                    has_next = false;
                }

                trace!(connector = format!("{:?}", new_connector).as_str(), "Yield a new connector");
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
    use crate::connector::paginator::curl::cursor::Cursor;
    use crate::connector::Connector;
    use crate::document::json::Json;
    use smol::stream::StreamExt;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn paginate() {
        let document = Json::default();
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".into();
        connector.path = "/uuid?next={{ paginator.next }}".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let paginator = Cursor {
            limit: 1,
            entry_path: "/uuid".to_string(),
            ..Default::default()
        };

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let mut datastream = connector.unwrap().fetch().await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();

        let connector = paging.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let mut datastream = connector.unwrap().fetch().await.unwrap().unwrap();
        let data_2 = datastream.next().await.unwrap();

        assert!(
            data_1 != data_2,
            "The content of this two stream are not different."
        );
    }
}
