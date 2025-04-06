use crate::{connector::Connector, ConnectorStream};
use async_stream::stream;
use std::io::Result;

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
    /// use smol::prelude::*;
    /// use std::io;
    /// use chewdata::connector::paginator::once::Once;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    /// 
    /// #[apply(main!)]
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
    pub async fn paginate(&self, connector: &dyn Connector) -> Result<ConnectorStream> {
        let new_connector = connector.clone_box();

        Ok(Box::pin(stream! {
            trace!(connector = format!("{:?}", new_connector).as_str(), "Yield a new connector");
            yield Ok(new_connector);
            trace!("Stop yielding new connector");
        }))
    }
}
