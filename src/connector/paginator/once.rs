use crate::connector::Connector;
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use std::{io::Error, io::ErrorKind, io::Result, pin::Pin};

use super::Paginator;

#[derive(Debug)]
pub struct Once {
    pub connector: Option<Box<dyn Connector>>,
}

#[async_trait]
impl Paginator for Once {
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    /// use chewdata::document::json::Json;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let connector = Io::default();
    ///     let document = Json::default();
    ///
    ///     let mut stream = connector.paginator(&document).await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader");
    ///     assert!(stream.next().await.transpose()?.is_none(), "Must return only on connector for IO");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "io_paginator::stream")]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let new_connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;
        let stream = Box::pin(stream! {
            trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector.");
            yield Ok(new_connector);
            trace!("The stream stops to return a new connectors.");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        false
    }
}
