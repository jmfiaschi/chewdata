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
//!
use crate::connector::Paginator;
use crate::connector::{curl::Curl, Connector};
use crate::document::DocumentType;
use async_std::stream::StreamExt;
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
pub struct Cursor {
    pub limit: usize,
    pub entry_path: String,
    #[serde(rename = "document")]
    #[serde(alias = "doc")]
    pub document_type: DocumentType,
    #[serde(skip)]
    pub connector: Option<Box<Curl>>,
    #[serde(rename = "next")]
    pub next_token: Option<String>,
}

impl Default for Cursor {
    fn default() -> Self {
        Cursor {
            limit: 100,
            connector: None,
            document_type: DocumentType::default(),
            next_token: None,
            entry_path: "/next".to_string(),
        }
    }
}

impl Cursor {
    pub fn set_connector(&mut self, connector: Curl) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for Cursor {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{curl::Curl, Connector};
    /// use chewdata::connector::paginator::curl::{PaginatorType, cursor::Cursor};
    /// use chewdata::document::{DocumentType, json::Json};
    /// use surf::http::Method;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.method = Method::Get;
    ///     connector.path = "/uuid?next={{ paginator.next }}".to_string();
    ///     connector.paginator_type = PaginatorType::Cursor(Cursor {
    ///         limit: 1,
    ///         entry_path: "/uuid".to_string(),
    ///         document_type: DocumentType::default(),
    ///         ..Default::default()
    ///     });
    ///     let paginator = connector.paginator().await?;
    ///     let mut stream = paginator.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///     assert!(stream.next().await.transpose()?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "cursor_paginator::stream")]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut document = self.document_type.clone().boxed_inner();
        let mut has_next = true;
        let limit = self.limit;
        let entry_path = self.entry_path.clone();
        let mut next_token_opt = self.next_token.clone();

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut new_parameters = connector.parameters.clone();

                if let Some(next_token) = next_token_opt {
                    new_parameters.merge_in("/paginator/next", Value::String(next_token))?;
                }

                new_parameters
                    .merge_in("/paginator/limit", Value::String(limit.to_string()))?;
                new_connector.set_parameters(new_parameters);

                document.set_entry_path(entry_path.clone());

                let mut dataset = match new_connector.fetch(&*document).await? {
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

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream returns a new connector.");
                yield Ok(new_connector.clone() as Box<dyn Connector>);
            }
            trace!("The stream stops returning new connectors.");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        false
    }
}
