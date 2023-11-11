pub mod metadata;

use self::metadata::Metadata;
use crate::connector::mongodb::Mongodb;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "metadata")]
    #[serde(skip_serializing)]
    Metadata(Metadata),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Metadata(Metadata::default())
    }
}

impl CounterType {
    pub async fn count(&self, connector: &Mongodb) -> Result<usize> {
        match self {
            CounterType::Metadata(counter) => counter.count(connector).await,
        }
    }
}
