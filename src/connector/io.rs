use super::{Connector, Paginator};
use crate::document::Document;
use crate::{DataSet, DataStream, Metadata};
use async_std::io::BufReader;
use async_std::io::{stdin, stdout};
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::{fmt, io::Result};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Io {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(default = "default_eof")]
    #[serde(alias = "end_of_input")]
    pub eoi: String,
}

fn default_eof() -> String {
    "".to_string()
}

// Not display the inner for better performance with big data
impl fmt::Debug for Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Io")
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[async_trait]
impl Connector for Io {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        "stdout".to_string()
    }
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::is_variable_path`] for more details.
    fn is_variable(&self) -> bool {
        false
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(false)
    }
    /// See [`Connector::fetch`] for more details.
    #[instrument(name = "io::fetch")]
    async fn fetch(&mut self, document: &dyn Document) -> std::io::Result<Option<DataStream>> {
        let stdin = BufReader::new(stdin());

        trace!("Fetch lines");
        let mut lines = stdin.lines();
        let mut buf = String::default();

        trace!("Read lines");
        while let Some(line) = lines.next().await {
            let current_line: String = line?;
            if current_line.eq(self.eoi.as_str()) {
                break;
            };
            buf = format!("{}{}\n", buf, current_line);
        }
        trace!("Save lines into the buffer");
        if !document.has_data(buf.as_bytes())? {
            return Ok(None);
        }

        let dataset = document.read(&buf.into_bytes())?;

        info!("The connector fetch data with success");
        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    #[instrument(skip(dataset), name = "io::send")]
    async fn send(&mut self, document: &dyn Document, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let mut buffer = Vec::default();

        buffer.append(&mut document.header(dataset)?);
        buffer.append(&mut document.write(dataset)?);
        buffer.append(&mut document.footer(dataset)?);

        trace!("Write data into stdout");
        stdout().write_all(&buffer).await?;
        // Force to send data
        trace!("Flush data into stdout");
        stdout().flush().await?;

        info!("The connector send data into the resource with success");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "IO connector can't erase data to the remote document. Use other connector type"
        )
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        Ok(Box::pin(IoPaginator::new(self.clone())?))
    }
}

#[derive(Debug)]
pub struct IoPaginator {
    connector: Io,
}

impl IoPaginator {
    pub fn new(connector: Io) -> Result<Self> {
        Ok(IoPaginator { connector })
    }
}

#[async_trait]
impl Paginator for IoPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let connector = Io::default();
    ///
    ///     let mut stream = connector.paginator().await?.stream().await?;
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
        let new_connector = self.connector.clone();
        let stream = Box::pin(stream! {
            trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
            yield Ok(Box::new(new_connector.clone()) as Box<dyn Connector>);
            trace!("The stream stop to return a new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::prelude::StreamExt;

    #[async_std::test]
    async fn paginator_stream() {
        let connector = Io::default();
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader"
        );
        assert!(
            stream.next().await.transpose().unwrap().is_none(),
            "Must return only on connector for IO"
        );
    }
}
