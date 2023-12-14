pub mod wildcard;

use futures::Stream;
use serde::{Deserialize, Serialize};
use std::io::Result;
use std::pin::Pin;
use wildcard::Wildcard;

use crate::connector::{local::Local, Connector};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "wildcard")]
    Wildcard(Wildcard),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Wildcard(Wildcard::default())
    }
}

impl PaginatorType {
    pub async fn paginate(
        &self,
        connector: &Local,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        match self {
            PaginatorType::Wildcard(paginator) => paginator.paginate(connector).await,
        }
    }
}
