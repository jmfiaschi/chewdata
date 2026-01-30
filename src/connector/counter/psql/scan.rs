//! Retreive the number of records through a full scan of the collection.
//!
//! ### Confirguration
//!
//! | key  | alias | Description                            | Default Value | Possible Values |
//! | ---- | ----- | -------------------------------------- | --------------| ----------------|
//! | type | -     | Required in order to use this counter. | `scan`        | `scan`          |
//!
//! ### Example
//!
//!  ```json
//!  [
//!      {
//!          "type": "read",
//!          "connector":{
//!              "type": "psql",
//!              "endpoint": "psql://admin:admin@localhost:27017",
//!              "database": "local",
//!              "collection": "startup_log",
//!              "paginator": {
//!                  "type": "offset",
//!                  "limit": 100,
//!                  "skip": 0,
//!                  "count": null
//!              },
//!              "counter": {
//!                  "type": "scan"
//!              }
//!          }
//!      }
//!  ]
//!  ```
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};

use crate::connector::psql::Psql;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Scan {}

impl Scan {
    /// Get the number of items from a full scan.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::psql::Psql;
    /// use smol::prelude::*;
    /// use std::io;
    /// use chewdata::connector::counter::psql::scan::Scan;
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "postgres://admin:admin@localhost".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.read".into();
    ///
    ///     let counter = Scan::default();
    ///     assert!(0 < counter.count(&connector).await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "scan::count")]
    pub async fn count(&self, connector: &Psql) -> Result<usize> {
        let (query_sanitized, _) =
            connector.query_sanitized("SELECT COUNT(1) FROM {{ collection }}", &Value::Null)?;

        let client = connector.client().await?;

        let count: i64 = sqlx::query_scalar(query_sanitized.as_str())
            .fetch_one(&client)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        trace!(count = count, "Count with success");

        Ok(count as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::psql::Psql;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn count() {
        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let counter = Scan::default();
        assert!(0 < counter.count(&connector).await.unwrap());
    }
}
