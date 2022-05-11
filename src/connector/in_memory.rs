use super::{Connector, Paginator};
use crate::Metadata;
use async_std::sync::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use serde::{de, Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Result, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct InMemory {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "value")]
    #[serde(alias = "doc")]
    #[serde(alias = "data")]
    #[serde(deserialize_with = "deserialize_inner")]
    #[serde(skip_serializing)]
    // The result value in memory.
    // Read the content only with the method io::Read::read().
    pub memory: Arc<Mutex<Buffer>>,
    #[serde(skip)]
    pub inner: Buffer,
}

impl fmt::Display for InMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for InMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemory")
            .field("metadata", &self.metadata)
            .finish()
    }
}

type Buffer = Cursor<Vec<u8>>;

fn deserialize_inner<'de, D>(deserializer: D) -> std::result::Result<Arc<Mutex<Buffer>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(Arc::new(Mutex::new(Cursor::new(s.into_bytes()))))
}

impl Into<InMemory> for Vec<u8> {
    fn into(self) -> InMemory {
        InMemory {
            memory: Arc::new(Mutex::new(Cursor::new(self))),
            ..Default::default()
        }
    }
}

impl Into<InMemory> for &str {
    /// Can fail for non UTF-8 str. use  `str.into()` instead.
    fn into(self) -> InMemory {
        InMemory {
            memory: Arc::new(Mutex::new(Cursor::new(self.to_string().into_bytes()))),
            ..Default::default()
        }
    }
}

impl InMemory {    
    pub fn new(str: &str) -> InMemory { str.into() }
}

#[async_trait]
impl Connector for InMemory {
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
        Ok(self.memory.lock().await.get_ref().is_empty())
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
        Ok(self.memory.lock().await.get_ref().len())
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
    #[instrument]
    async fn fetch(&mut self) -> Result<()> {
        // Avoid to fetch two times the same data in the same connector
        if !self.inner.get_ref().is_empty() {
            return Ok(());
        }
        
        let resource = self.memory.lock().await;
        self.inner = io::Cursor::new(resource.get_ref().clone());

        info!("The connector fetch data with success");
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
    ///     assert_eq!(true, connector.inner().is_empty());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn erase(&mut self) -> io::Result<()> {
        let mut memory = self.memory.lock().await;
        *memory = Cursor::default();

        info!("The connector erase data into the memory with success");
        Ok(())
    }
    /// See [`Connector::send`] for more details.
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
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.write(r#"{"column1":"value2"}"#.as_bytes()).await?;
    ///     connector.send(Some(0)).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}{"column1":"value2"}"#, buffer);
    ///
    ///     connector.write(r#"{"column1":"value3"}"#.as_bytes()).await?;
    ///     connector.send(Some(-20)).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}{"column1":"value3"}"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn send(&mut self, position: Option<isize>) -> Result<()> {
        let inner = self.inner().clone();
        let resource_len = self.len().await?;
        self.clear();

        let mut memory = self.memory.lock().await;

        match position {
            Some(pos) => match resource_len as isize + pos {
                start if start > 0 => memory.seek(SeekFrom::Start(start as u64)),
                _ => memory.seek(SeekFrom::Start(0)),
            },
            None => memory.seek(SeekFrom::Start(0)),
        }?;

        memory.write_all(&inner)?;
        memory.set_position(0);

        info!("The connector send data into the memory with success");
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
}

impl InMemoryPaginator {
    pub fn new(connector: InMemory) -> Result<Self> {
        Ok(InMemoryPaginator { connector })
    }
}

#[async_trait]
impl Paginator for InMemoryPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
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
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_none(), "Can't paginate more than one time.");
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
            trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector and stop");
            yield Ok(Box::new(new_connector.clone()) as Box<dyn Connector>);
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&mut self) -> bool {
        false
    }
}
