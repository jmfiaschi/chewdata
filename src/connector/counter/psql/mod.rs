pub mod scan;

use self::scan::Scan;
use crate::connector::psql::Psql;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "scan")]
    #[serde(skip_serializing)]
    Scan(Scan),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Scan(Scan::default())
    }
}

impl CounterType {
    pub async fn count(&self, connector: &Psql) -> Result<usize> {
        match self {
            CounterType::Scan(scan) => scan.count(connector).await,
        }
    }
}
