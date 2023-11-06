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
use std::io::{Error, ErrorKind, Result};
use surf::http::Url;

use crate::connector::{curl::Curl, Connector};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Header {
    pub name: String,
    pub path: Option<String>,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            name: "X-Total-Count".to_string(),
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
    ///
    ///     let mut counter = Header::default();
    ///     counter.name = "Content-Length".to_string();
    ///     assert!(Some(0) < counter.count(connector).await.unwrap(), "Counter count() must return a value upper than 0.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "header_counter::count")]
    pub async fn count(&self, connector: Curl) -> Result<Option<usize>> {
        let mut connector = connector.clone();
        let client = connector.client().await?;

        if let Some(path) = self.path.clone() {
            connector.path = path;
        }

        let url = Url::parse(format!("{}{}", connector.endpoint, connector.path()).as_str())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let res = client
            .head(url)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        if !res.status().is_success() {
            warn!(
                status = res.status().to_string().as_str(),
                "Can't retrieve the number of elements from the resource with the method HEAD"
            );

            return Ok(None);
        }

        let header_value = res
            .header(self.name.as_str())
            .map(|value| value.as_str())
            .unwrap_or("0");

        if header_value == "0" {
            return Ok(None);
        }

        Ok(match header_value.to_string().parse::<usize>() {
            Ok(count) => {
                trace!(
                    size = count,
                    "The counter counts the elements in the resource successfully."
                );
                Some(count)
            }
            Err(_) => {
                trace!("The counter is unable to count elements in the resource.");
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use http_types::Method;

    use super::*;

    #[async_std::test]
    async fn count_return_value() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        let mut counter = Header::default();
        counter.name = "Content-Length".to_string();
        assert!(
            Some(0) < counter.count(connector).await.unwrap(),
            "Counter count() must return a value upper than 0."
        );
    }
    #[async_std::test]
    async fn count_not_return_value() {
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.method = Method::Get;
        connector.path = "/get".to_string();
        let mut counter = Header::default();
        counter.name = "not_found".to_string();
        assert_eq!(None, counter.count(connector).await.unwrap());
    }
}
