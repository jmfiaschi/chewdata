#[cfg(feature = "curl")]
pub mod curl;
#[cfg(feature = "mongodb")]
pub mod mongodb;
pub mod once;

use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::Stream;
use std::io::Result;

use super::Connector;

#[async_trait]
pub trait Paginator: std::fmt::Debug + Unpin {
    /// Get the stream of connectors
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>>;
    /// Test if the paginator can be parallelizable.
    fn is_parallelizable(&self) -> bool;
}
