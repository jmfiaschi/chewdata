use super::{Connector, Paginator};
use crate::document::DocumentType;
use crate::Metadata;
use async_std::io::{stdin, stdout};
use async_std::prelude::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    fmt,
    io::{Cursor, Result},
};
use crate::step::DataResult;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct Io {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "document")]
    document_type: DocumentType,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8(self.inner.clone().into_inner()).unwrap_or("".to_string()))
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
    /// See [`Connector::document_type`] for more details.
    fn document_type(&self) -> DocumentType {
        self.document_type.clone()
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool>{
        Ok(false)
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::push_data`] for more details.
    async fn push_data(&mut self, data: DataResult) -> Result<()> {
        let connector = self;
        let document = connector.document_type().document_inner();

        document.write_data(connector, data.to_json_value()).await
    }
    /// See [`Connector::fetch`] for more details.
    async fn fetch(&mut self) -> Result<()> {
        self.inner = Cursor::new(Vec::default());

        Ok(())
    }
    /// See [`Connector::send`] for more details.
    async fn send(&mut self) -> Result<()> {
        self.document_type()
            .document_inner()
            .flush(self)
            .await
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        Ok(())
    }
    /// See [`Connector::flush_into`] for more details.
    async fn flush_into(&mut self, _position: i64) -> Result<()> {
        stdout().write_all(self.inner.get_ref()).await?;
        self.flush().await?;
        self.inner = Default::default();
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(IoPaginator::new(self.clone())?))
    }
}

#[async_trait]
impl async_std::io::Read for Io {
    /// See [`Read::poll_read`] for more details.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut stdin()).poll_read(cx, buf)
    }
}

#[async_trait]
impl async_std::io::Write for Io {
    /// See [`Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct IoPaginator {
    connector: Io,
    has_next: bool,
}

impl IoPaginator {
    pub fn new(connector: Io) -> Result<Self> {
        Ok(IoPaginator {
            connector: connector,
            has_next: true,
        })
    }
}

#[async_trait]
impl Paginator for IoPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::io::Io;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Io::default();
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert!(paginator.next_page().await?.is_some(), "Can't get the first reader.");
    ///     assert!(paginator.next_page().await?.is_none(), "Can't paginate more than one time.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>> {
        Ok(match self.has_next {
            true => {
                let mut connector = self.connector.clone();
                self.has_next = false;
                connector.fetch().await?;
                Some(Box::new(connector))
            }
            false => None,
        })
    }
}
