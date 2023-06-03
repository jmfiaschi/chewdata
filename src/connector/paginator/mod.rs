#[cfg(feature = "curl")]
pub mod curl;
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
    /// Try to fetch the number of item in the connector.
    /// None: Can't retrieve the total of item for any reason.
    /// Some(count): Retrieve the total of item and store the value in the paginator.
    async fn count(&mut self) -> Result<Option<usize>>;
    /// Test if the paginator can be parallelizable.
    fn is_parallelizable(&self) -> bool;
}
