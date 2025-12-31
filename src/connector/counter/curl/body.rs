//! Find the number of elements from the response's body.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                                | Default Value | Possible Values |
//! | ---------- | ----- | ------------------------------------------------------------------------------------------ | ------------- | --------------- |
//! | type       | -     | Required in order to use this counter.                                                      | `body`        | `body`          |
//! | entry_path | -     | The entry path for capturing the value in the response's body.                                         | `/count`      | String          |
//! | path       | -     | The URL path to retrieve the total number of records. If `null`, it takes the connector path  by default. | `null`        | String          |
//! | method     | -     | HTTP Method to apply (HEAD | POST | GET | etc...) | `null`        | String          |
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
//!                 "path": "/count",
//!                 "method": "get"
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
use crate::connector::{curl::Curl, Connector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol::stream::StreamExt;
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Body {
    pub entry_path: String,
    pub path: Option<String>,
    pub method: Option<String>,
}

impl Default for Body {
    fn default() -> Self {
        Body {
            entry_path: "/count".to_string(),
            path: None,
            method: None,
        }
    }
}

impl Body {
    pub fn new(entry_path: String, path: Option<String>, method: Option<String>) -> Self {
        Body {
            entry_path,
            path,
            method,
        }
    }
    /// To retrieve the number of items from the response's body and return `None` if the counter is unable to count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::counter::curl::body::Body;
    /// use chewdata::document::json::Json;
    /// use smol::prelude::*;
    /// use std::io;
    /// use crate::chewdata::document::Document;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = "POST".into();
    ///     connector.path = "/anything?count=10".to_string();
    ///     connector.metadata = Json::default().metadata();
    ///
    ///     let mut counter = Body::default();
    ///     counter.entry_path = "/args/not_found".to_string();
    ///     assert_eq!(Some(10), counter.count(&connector).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "body::count")]
    pub async fn count(&self, connector: &Curl) -> Result<Option<usize>> {
        let mut connector = connector.clone();
        let mut document = connector.document()?.clone_box();
        document.set_entry_path(self.entry_path.clone());
        connector.set_document(document)?;

        if let Some(ref path) = self.path {
            connector.path = path.clone();
        }

        if let Some(ref method) = self.method {
            connector.method = method.clone();
        }

        let mut dataset = match connector.fetch().await? {
            Some(dataset) => dataset,
            None => {
                trace!("No data found");
                return Ok(None);
            }
        };

        let value = dataset
            .next()
            .await
            .map_or(Value::Null, |data| data.to_value());

        let count_opt = match value {
            Value::Number(n) => n.as_u64().map(|number| number as usize),
            Value::String(s) => s.parse::<usize>().ok(),
            _ => None,
        };

        info!(count = count_opt, "✅ Count with success");

        Ok(count_opt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::json::Json;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn count_return_value() {
        let document = Json::default();

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "POST".to_string();
        connector.path = "/anything?count=10".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let mut counter = Body::default();
        counter.entry_path = "/args/count".to_string();
        assert!(
            Some(0) < counter.count(&connector).await.unwrap(),
            "Counter count() must return a value upper than 0."
        );
    }
    #[apply(test!)]
    async fn count_not_return_value() {
        let document = Json::default();

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "POST".to_string();
        connector.path = "/anything?count=10".to_string();
        connector.set_document(Box::new(document)).unwrap();

        let mut counter = Body::default();
        counter.entry_path = "/args/not_found".to_string();
        assert_eq!(None, counter.count(&connector).await.unwrap());
    }
}
