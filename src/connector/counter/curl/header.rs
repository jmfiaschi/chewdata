//! Find the number of elements form the response's header.
//!
//! ### Confirguration
//!
//! | key  | alias | Description                                                                                | Default Value   | Possible Values |
//! | ---- | ----- | ------------------------------------------------------------------------------------------ | --------------- | --------------- |
//! | type | -     | Required in order to use this counter.                                                     | `header`        | `header`        |
//! | name | -     | Header name where to find the total of records.                                             | `X-Total-Count` | String          |
//! | path | -     | The URL path to retrieve the total number of records. If `null`, it takes the connector path  by default. | `null`          | String          |
//!
//! ### Example
//!
//!  ```json
//!  [
//!      {
//!          "type": "read",
//!          "connector":{
//!              "type": "curl",
//!              "endpoint": "{{ CURL_ENDPOINT }}",
//!              "path": "/get?next={{ paginator.next }}",
//!              "method": "get",
//!              "paginator": {
//!                  "type": "offset",
//!                  "limit": 100,
//!                  "skip": 0,
//!                  "count": null
//!              },
//!              "counter": {
//!                  "type": "header",
//!                  "name": "X-Total-Count",
//!                  "path": "/count"
//!              }
//!          }
//!      }
//!  ]
//!  ```
use serde::{Deserialize, Serialize};
use std::io::Result;
use crate::connector::curl::Curl;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Header {
    pub name: String,
    pub path: Option<String>,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            name: "Content-Length".to_string(),
            path: None,
        }
    }
}

impl Header {
    pub fn new(name: String, path: Option<String>) -> Self {
        Header { name, path }
    }
    /// To retrieve the number of items from the response's header and return `None` if the counter is unable to count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::curl::Curl;
    /// use chewdata::connector::counter::curl::header::Header;
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
    ///     connector.path = "/get".to_string();
    ///
    ///     let mut counter = Header::default();
    ///     counter.name = "Content-Length".to_string();
    ///     assert!(Some(0) < counter.count(&connector).await.unwrap(), "Counter count() must return a value upper than 0.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "header::count")]
    pub async fn count(&self, connector: &Curl) -> Result<Option<usize>> {
        let mut connector = connector.clone();

        if let Some(ref path) = self.path {
            connector.path = path.clone();
        }

        let headers = connector.head().await?;

        for (key, value) in headers {
            if self.name.eq_ignore_ascii_case(&key) {
                return Ok(match String::from_utf8_lossy(&value).parse::<usize>() {
                    Ok(count) => {
                        trace!(size = count, "Count with success");
                        Some(count)
                    }
                    Err(_) => {
                        trace!("Can't count");
                        None
                    }
                });
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn count_return_value() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".to_string();
        connector.path = "/get".to_string();
        let mut counter = Header::default();
        counter.name = "Content-Length".to_string();
        assert!(
            Some(0) < counter.count(&connector).await.unwrap(),
            "Counter must return a value upper than 0."
        );
    }
    #[apply(test!)]
    async fn count_not_return_value() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = "GET".to_string();
        connector.path = "/get".to_string();
        let mut counter = Header::default();
        counter.name = "not_found".to_string();
        assert_eq!(None, counter.count(&connector).await.unwrap());
    }
}
