use crate::connector::local::Local;
use crate::connector::Connector;
use crate::ConnectorStream;
use async_stream::stream;
use glob::glob;
use serde::{Deserialize, Serialize};
use std::io::Result;
use std::io::{Error, ErrorKind};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Wildcard {
    pub paths: Vec<String>,
}

impl Wildcard {
    /// Create a new Wildcard paginator and load in memory all file paths in the connector's path
    pub fn new(connector: &Local) -> Result<Self> {
        if connector.path().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "The field 'path' for a local connector can't be an empty string".to_string(),
            ));
        }

        let paths: Vec<String> = match glob(connector.path().as_str()) {
            Ok(paths) => Ok(paths
                .filter(|p| p.is_ok())
                .map(|p| p.unwrap().display().to_string())
                .collect()),
            Err(e) => Err(Error::new(ErrorKind::InvalidInput, e)),
        }?;

        if paths.is_empty() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!(
                    "No files found with this path pattern '{}'",
                    connector.path()
                ),
            ));
        }

        Ok(Wildcard { paths })
    }
    /// Paginate through the connector.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    /// use chewdata::connector::paginator::local::wildcard::Wildcard;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/one_line.*".to_string();
    ///
    ///     let paginator = Wildcard::new(&connector)?;
    ///
    ///     let mut paging = paginator.paginate(&connector).await?;
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(paging.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "wildcard::paginate")]
    pub async fn paginate(&self, connector: &Local) -> Result<ConnectorStream> {
        let connector = connector.clone();
        let mut paths = self.paths.clone().into_iter();

        Ok(Box::pin(stream! {
            for path in &mut paths {
                let mut new_connector = connector.clone();
                new_connector.path = path.clone();

                trace!(connector = format!("{:?}", new_connector).as_str(), "Yield a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("Stop yielding new connector");
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{
        connector::local::Local,
        document::{json::Json, DocumentClone},
    };

    #[async_std::test]
    async fn paginate() {
        let document = Json::default();
        let mut connector = Local::default();
        connector.path = "./data/one_line.*".to_string();
        connector.set_document(&document.clone_box()).unwrap();

        let paginator = Wildcard::new(&connector).unwrap();

        let mut paging = paginator.paginate(&connector).await.unwrap();

        let connector = paging.next().await.transpose().unwrap().unwrap();
        let file_len1 = connector.len().await.unwrap();
        assert!(
            0 < file_len1,
            "The size of the file must be upper than zero."
        );

        let connector = paging.next().await.transpose().unwrap().unwrap();
        let file_len2 = connector.len().await.unwrap();
        assert!(
            0 < file_len2,
            "The size of the file must be upper than zero."
        );
        assert!(
            file_len1 != file_len2,
            "The file size of this two files are not different."
        );
    }
}
