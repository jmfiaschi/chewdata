use super::{Connector, Paginator};
use crate::document::Document;
use crate::{DataSet, DataStream, Metadata};
use async_std::sync::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use serde::{de, Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Result, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::sync::Arc;
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
}

impl fmt::Display for InMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        futures::executor::block_on(async {
            write!(
                f,
                "{}",
                String::from_utf8(self.memory.lock().await.get_ref().to_vec()).unwrap()
            )
        })
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

impl From<Vec<u8>> for InMemory {
    fn from(v: Vec<u8>) -> InMemory {
        InMemory {
            memory: Arc::new(Mutex::new(Cursor::new(v))),
            ..Default::default()
        }
    }
}

impl From<&str> for InMemory {
    fn from(s: &str) -> InMemory {
        InMemory {
            memory: Arc::new(Mutex::new(Cursor::new(s.to_string().into_bytes()))),
            ..Default::default()
        }
    }
}

impl InMemory {
    pub fn new(str: &str) -> InMemory {
        str.into()
    }
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
    /// See [`Connector::len`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::jsonl::Jsonl;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     let datastream = connector.fetch(document).await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn fetch(&mut self, document: Box<dyn Document>) -> std::io::Result<Option<DataStream>> {
        let resource = self.memory.lock().await;
        info!("The connector fetch data with success");
        if !document.has_data(resource.get_ref())? {
            return Ok(None);
        }

        let dataset = document.read(resource.get_ref())?;

        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.send(document.clone(), &dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch(document.clone()).await.unwrap().unwrap();
    ///     assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());
    ///
    ///     let expected_result2 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
    ///     let dataset = vec![expected_result2.clone()];
    ///     connector.send(document.clone(), &dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch(document).await.unwrap().unwrap();
    ///     assert_eq!(expected_result1, datastream.next().await.unwrap());
    ///     assert_eq!(expected_result2, datastream.next().await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset))]
    async fn send(
        &mut self,
        mut document: Box<dyn Document>,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let position = match document.can_append() {
            true => Some(-(document.footer(dataset)?.len() as isize)),
            false => None,
        };
        let terminator = document.terminator()?;
        let footer = document.footer(dataset)?;
        let header = document.header(dataset)?;
        let body = document.write(dataset)?;

        let mut memory = self.memory.lock().await;
        let resource_len = memory.get_ref().len();

        match position {
            Some(pos) => match resource_len as isize + pos {
                start if start > 0 => memory.seek(SeekFrom::Start(start as u64)),
                _ => memory.seek(SeekFrom::Start(0)),
            },
            None => memory.seek(SeekFrom::Start(0)),
        }?;

        if 0 == resource_len {
            memory.write_all(&header)?;
        }
        if 0 < resource_len && resource_len > (header.len() as usize + footer.len() as usize) {
            memory.write_all(&terminator)?;
        }
        memory.write_all(&body)?;
        memory.write_all(&footer)?;
        memory.set_position(0);

        info!("The connector send data into the memory with success");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::jsonl::Jsonl;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.erase().await.unwrap();
    ///     let datastream = connector.fetch(document).await.unwrap();
    ///     assert!(datastream.is_none(), "The datastream must be empty");
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
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        Ok(Box::pin(InMemoryPaginator::new(self.clone())?))
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let connector = InMemory::default();
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
    fn is_parallelizable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::jsonl::Jsonl;
    use crate::DataResult;
    use async_std::prelude::StreamExt;

    #[async_std::test]
    async fn len() {
        let mut connector = InMemory::new(r#"[{"column1":"value1"}]"#);
        assert!(
            0 < connector.len().await.unwrap(),
            "The length of the document is not greather than 0."
        );
    }
    #[async_std::test]
    async fn is_empty() {
        let mut connector = InMemory::new("");
        assert_eq!(true, connector.is_empty().await.unwrap());
        let mut connector = InMemory::new("My text");
        assert_eq!(false, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
    async fn fetch() {
        let document = Box::new(Jsonl::default());
        let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero"
        );
    }
    #[async_std::test]
    async fn send() {
        let document = Box::new(Jsonl::default());

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        let mut connector = InMemory::new(r#""#);
        connector.send(document.clone(), &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read
            .fetch(document.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(document.clone(), &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch(document).await.unwrap().unwrap();
        assert_eq!(expected_result1, datastream.next().await.unwrap());
        assert_eq!(expected_result2, datastream.next().await.unwrap());
    }
    #[async_std::test]
    async fn erase() {
        let document = Box::new(Jsonl::default());
        let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
        connector.erase().await.unwrap();
        let datastream = connector.fetch(document).await.unwrap();
        assert!(datastream.is_none(), "The datastream must be empty");
    }
    #[async_std::test]
    async fn paginator_stream() {
        let connector = InMemory::default();
        let mut paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            stream.next().await.transpose().unwrap().is_none(),
            "Can't paginate more than one time."
        );
    }
}
