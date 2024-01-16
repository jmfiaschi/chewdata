pub mod offset;

use futures::Stream;
use offset::Offset;
use serde::{Deserialize, Serialize};
use std::io::Result;
use std::pin::Pin;

use crate::connector::{psql::Psql, Connector};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "offset")]
    Offset(Offset),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Offset(Offset::default())
    }
}

impl PaginatorType {
    pub async fn paginate(
        &self,
        connector: &Psql,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        match self {
            PaginatorType::Offset(paginator) => {
                let mut paginator = paginator.clone();
                if paginator.count.is_none() {
                    paginator.count = Some(connector.len().await?);
                }
                paginator.paginate(connector).await
            }
        }
    }
}
