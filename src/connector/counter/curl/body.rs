//! Find the number of elements from the response's body.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                                | Default Value | Possible Values |
//! | ---------- | ----- | ------------------------------------------------------------------------------------------ | ------------- | --------------- |
//! | type       | -     | Required in order to use this counter.                                                      | `body`        | `body`          |
//! | entry_path | -     | The entry path for capturing the value in the response's body.                                         | `/count`      | String          |
//! | path       | -     | The URL path to retrieve the total number of records. If `null`, it takes the connector path  by default. | `null`        | String          |
//!
//! ### Example
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
//!                 "type": "offset",
//!                 "limit": 100,
//!                 "skip": 0,
//!                 "count": null
//!             },
//!             "counter": {
//!                 "type": "body",
//!                 "entry_path": "/count",
//!                 "path": "/count"
//!             }
//!         }
//!     }
//! ]
//! ```
//!
//! body response:
//!
//! ```json
//! {
//!     "count": 1200
//! }
//! ```
use async_std::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Result;

use crate::{
    connector::{curl::Curl, Connector},
    document::Document,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Body {
    pub entry_path: String,
    pub path: Option<String>,
}

impl Default for Body {
    fn default() -> Self {
        Body {
            entry_path: "/count".to_string(),
            path: None,
        }
    }
}

impl Body {
    pub fn new(entry_path: String, path: Option<String>) -> Self {
        Body { entry_path, path }
    }
    /// To retrieve the number of items from the response's body and return `None` if the counter is unable to count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::counter::curl::body::Body;
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Post;
    ///     connector.path = "/anything?count=10".to_string();
    ///
    ///     let mut counter = Body::default();
    ///     counter.entry_path = "/args/count".to_string();
    ///     assert_eq!(Some(10), counter.count(connector, Box::new(Json::default())).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "body_counter::count")]
    pub async fn count(
        &self,
        connector: Curl,
        document: Box<dyn Document>,
    ) -> Result<Option<usize>> {
        let mut connector = connector.clone();
        let mut document = document.clone();

        if let Some(path) = self.path.clone() {
            connector.path = path;
        }

        document.set_entry_path(self.entry_path.clone());

        let mut dataset = match connector.fetch(&*document).await? {
            Some(dataset) => dataset,
            None => {
                trace!("No data was found.");
                return Ok(None);
            }
        };

        let data_opt = dataset.next().await;

        let value = match data_opt {
            Some(data) => data.to_value(),
            None => Value::Null,
        };

        let count = match value {
            Value::Number(_) => value.as_u64().map(|number| number as usize),
            Value::String(_) => match value.as_str() {
                Some(value) => match value.parse::<usize>() {
                    Ok(number) => Some(number),
                    Err(_) => None,
                },
                None => None,
            },
            _ => None,
        };

        trace!(
            size = count,
            "The counter counts the elements in the resource successfully."
        );
        Ok(count)
    }
}
