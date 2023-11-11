use crate::connector::Connector;
use async_std::prelude::*;
use async_stream::stream;
use std::{io::Result, pin::Pin};

#[derive(Debug)]
pub struct Once {}

impl Once {
    /// Paginate through the connector in parameter.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    /// use chewdata::connector::paginator::once::Once;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let connector = Io::default();
    ///     let paginator = Once{};
    ///
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the first reader");
    ///     assert!(paging.next().await.transpose()?.is_none(), "Must return only on connector for IO");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "once::paginate")]
    pub async fn paginate(
        &self,
        connector: &dyn Connector,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let new_connector = connector.clone_box();

        Ok(Box::pin(stream! {
            trace!(connector = format!("{:?}", new_connector).as_str(), "The stream yields a new connector.");
            yield Ok(new_connector);
            trace!("The stream stops yielding new connectors.");
        }))
    }
}
