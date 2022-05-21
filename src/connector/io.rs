use super::{Connector, Paginator};
use crate::Metadata;
use async_std::io::BufReader;
use async_std::io::{stdin, stdout};
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    fmt,
    io::{Cursor, Result},
};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Io {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(default = "default_eof")]
    #[serde(alias = "end_of_input")]
    pub eoi: String,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

fn default_eof() -> String {
    "".to_string()
}

impl fmt::Display for Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
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
    /// See [`Connector::is_empty`] for more details.
    async fn is_empty(&mut self) -> Result<bool> {
        Ok(true)
    }
    /// See [`Connector::len`] for more details.
    async fn len(&mut self) -> Result<usize> {
        Ok(0)
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(false)
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::fetch`] for more details.
    #[instrument]
    async fn fetch(&mut self) -> Result<()> {
        // Avoid to fetch two times the same data in the same connector
        if !self.inner.get_ref().is_empty() {
            return Ok(());
        }

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

        trace!("Save lines into the inner buffer");
        self.inner = Cursor::new(buf.into_bytes());

        info!("The connector fetch data with success");
        Ok(())
    }
    /// See [`Connector::send`] for more details.
    #[instrument]
    async fn send(&mut self, _position: Option<isize>) -> Result<()> {
        trace!("Write data into stdout");
        stdout().write_all(self.inner.get_ref()).await?;
        // Force to send data
        trace!("Flush data into stdout");
        stdout().flush().await?;
        self.clear();

        info!("The connector send data into the resource with success");
        Ok(())
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
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for Io {
    /// See [`async_std::io::Read::poll_read`] for more details.
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Read::read(&mut self.inner, buf))
    }
}

#[async_trait]
impl async_std::io::Write for Io {
    /// See [`async_std::io::Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`async_std::io::Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`async_std::io::Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
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
    /// ```no_run
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
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let new_connector = self.connector.clone();
        let stream = Box::pin(stream! {
            while true {
                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                yield Ok(Box::new(new_connector.clone()) as Box<dyn Connector>);
            }
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
    use async_std::{
        prelude::StreamExt,
    };

    #[async_std::test]
    async fn paginator_stream() {
        let connector = Io::default();
        let mut paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the second reader."
        );
    }
}
