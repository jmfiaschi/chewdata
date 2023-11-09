pub mod body;
pub mod header;

use crate::connector::counter::curl::header::Header;
use crate::connector::curl::Curl;
use crate::{connector::counter::curl::body::Body, document::Document};
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "header")]
    Header(Header),
    #[serde(rename = "body")]
    Body(Body),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Header(Header::default())
    }
}

impl CounterType {
    pub async fn count(
        &self,
        connector: &Curl,
        document: Box<dyn Document>,
    ) -> Result<Option<usize>> {
        match self {
            CounterType::Header(header_counter) => header_counter.count(&connector).await,
            CounterType::Body(body_counter) => body_counter.count(&connector, document).await
        }
    }
}
