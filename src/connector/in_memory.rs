use super::{Connector, Paginator};
use crate::document::DocumentType;
use crate::step::DataResult;
use crate::Metadata;
use async_std::sync::Mutex;
use async_trait::async_trait;
use serde::{de, Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Result, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct InMemory {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "document")]
    pub document_type: DocumentType,
    #[serde(alias = "value")]
    #[serde(alias = "doc")]
    #[serde(alias = "data")]
    #[serde(deserialize_with = "deserialize_inner")]
    #[serde(skip_serializing)]
    // The result value like if the document is in remote.
    // Read the content only with the method io::Read::read().
    pub document: Arc<Mutex<Cursor<Vec<u8>>>>,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

impl fmt::Display for InMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or("".to_string())
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for InMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemory")
            .field("metadata", &self.metadata)
            .field("document_type", &self.document_type)
            .finish()
    }
}

fn deserialize_inner<'de, D>(
    deserializer: D,
) -> std::result::Result<Arc<Mutex<Cursor<Vec<u8>>>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(Arc::new(Mutex::new(Cursor::new(s.into_bytes()))))
}

impl InMemory {
    /// Creates a new document type `InMemory`.
    pub fn new(str: &str) -> InMemory {
        InMemory {
            document: Arc::new(Mutex::new(Cursor::new(str.to_string().into_bytes()))),
            ..Default::default()
        }
    }
}

#[async_trait]
impl Connector for InMemory {
    /// See [`Connector::document_type`] for more details.
    fn document_type(&self) -> DocumentType {
        self.document_type.clone()
    }
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        "in_memory".to_string()
    }
    /// See [`Connector::is_variable`] for more details.
    fn is_variable(&self) -> bool {
        false
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::new("");
    ///     assert_eq!(true, connector.is_empty().await?);
    ///     let mut connector = InMemory::new("My text");
    ///     assert_eq!(false, connector.is_empty().await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&mut self) -> io::Result<bool> {
        Ok(self.document.lock().await.get_ref().is_empty())
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::new(r#"[{"column1":"value1"}]"#);
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn len(&mut self) -> io::Result<usize> {
        Ok(self.document.lock().await.get_ref().len())
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(false)
    }
    /// See [`Connector::push_data`] for more details.
    async fn push_data(&mut self, data: DataResult) -> Result<()> {
        debug!(slog_scope::logger(), "push data"; "data" => format!("{}", data.to_json_value()));
        let document = self.document_type().document_inner();
        document.write_data(self, data.to_json_value()).await
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::new("My text");
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.fetch().await?;
    ///     assert!(0 < connector.inner().len(), "The inner connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn fetch(&mut self) -> Result<()> {
        let document = self.document.lock().await;
        self.inner = io::Cursor::new(document.get_ref().clone());
        Ok(())
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::new("My text");
    ///     connector.erase().await?;
    ///     connector.fetch().await?;
    ///     assert_eq!(false, connector.inner_has_data());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn erase(&mut self) -> io::Result<()> {
        let mut document = self.document.lock().await;
        *document = Cursor::default();
        Ok(())
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use chewdata::step::DataResult;
    /// use serde_json::{from_str, Value};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let value: Value = from_str(r#"{"column1":"value2"}"#)?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     let mut connector = InMemory::new(r#"[{"column1":"value1"}]"#);
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///
    ///     let mut buffer = String::default();
    ///     connector.fetch().await?;
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn send(&mut self) -> Result<()> {
        self.document_type().document_inner().close(self).await?;

        let entry_point_path_start_len = self
            .document_type()
            .document_inner()
            .entry_point_path_start()
            .len();
        let entry_point_path_end_len = self
            .document_type()
            .document_inner()
            .entry_point_path_end()
            .len();

        let inner = self.inner().clone();
        self.clear();
        let remote_len = self.len().await?;

        let mut position: i64 = remote_len as i64 - entry_point_path_end_len as i64;

        if 0 > position {
            position = 0;
        }

        let mut document = self.document.lock().await;
        if remote_len == 0 {
            document.seek(SeekFrom::Start(0))?;
        }

        if remote_len == entry_point_path_start_len + entry_point_path_end_len {
            document.seek(SeekFrom::Start(0))?;
        }

        if remote_len > entry_point_path_start_len + entry_point_path_end_len {
            document.seek(SeekFrom::Start(position as u64))?;
        }

        document.seek(SeekFrom::Start(position as u64))?;

        document.write_all(&inner)?;
        document.set_position(0);

        Ok(())
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(InMemoryPaginator::new(self.clone())?))
    }
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for InMemory {
    /// See [`async_std::io::Read::poll_read`] for more details.
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(std::io::Read::read(&mut self.inner, buf))
    }
}

#[async_trait]
impl async_std::io::Write for InMemory {
    /// See [`async_std::io::Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`async_std::io::Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`async_std::io::Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct InMemoryPaginator {
    connector: InMemory,
    has_next: bool,
}

impl InMemoryPaginator {
    pub fn new(connector: InMemory) -> Result<Self> {
        Ok(InMemoryPaginator {
            connector: connector,
            has_next: true,
        })
    }
}

#[async_trait]
impl Paginator for InMemoryPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::default();
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert!(paginator.next_page().await?.is_some(), "Can't get the first reader.");
    ///     assert!(paginator.next_page().await?.is_none(), "Can't paginate more than one time.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>> {
        let mut connector = self.connector.clone();
        Ok(match self.has_next {
            true => {
                self.has_next = false;
                connector.fetch().await?;
                Some(Box::new(connector))
            }
            false => None,
        })
    }
}
