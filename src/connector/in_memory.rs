//! Read and write data through memory. You can use this connector if you want to inject constant in your flow.
//!
//! ### Configuration
//!
//! | key      | alias              | Description                             | Default Value | Possible Values       |
//! | -------- | ------------------ | --------------------------------------- | ------------- | --------------------- |
//! | type     | -                  | Required in order to use this connector | `in_memory`   | `in_memory` / `mem`   |
//! | metadata | meta               | Override metadata information           | `null`        | [`crate::Metadata`] |
//! | memory   | value / doc / data | Memory value                            | `null`        | String                |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "reader",
//!         "connector":{
//!             "type": "in_memory",
//!             "memory": "{\"username\": \"{{ MY_USERNAME }}\",\"password\": \"{{ MY_PASSWORD }}\"}"
//!         }
//!     }
//! ]
//! ```
use super::Connector;
use crate::connector::paginator::once::Once;
use crate::document::Document;
use crate::{DataSet, DataStream, Metadata};
use async_lock::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use serde::{de, Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Error, ErrorKind, Result, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct InMemory {
    #[serde(skip)]
    document: Option<Box<dyn Document>>,
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
        if let Some(memory) = self.memory.try_lock() {
            write!(
                f,
                "{}",
                String::from_utf8(memory.get_ref().to_vec()).unwrap()
            )
        } else {
            write!(f, "<InMemory locked>")
        }
    }
}

// Not display the memory for better performance.
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
    /// See [`Connector::set_document`] for more details.
    fn set_document(&mut self, document: Box<dyn Document>) -> Result<()> {
        self.document = Some(document.clone());

        Ok(())
    }
    /// See [`Connector::document`] for more details.
    fn document(&self) -> Result<&dyn Document> {
        self.document.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "The document has not been set in the connector",
            )
        })
    }
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
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = InMemory::new(r#"[{"column1":"value1"}]"#);
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "in_memory::len")]
    async fn len(&self) -> io::Result<usize> {
        let len = self.memory.lock().await.get_ref().len();

        info!(len = len, "Size of data found in the resource");

        Ok(len)
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
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
    /// use smol::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    /// use smol::prelude::*;
    /// use smol::stream::StreamExt;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.set_document(document);
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "in_memory::fetch")]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let document = self.document()?;
        let memory = self.memory.lock().await;

        info!("Fetch data with success");

        // Skip if memory is empty or document has no data
        if memory.get_ref().is_empty() || !document.has_data(memory.get_ref())? {
            return Ok(None);
        }

        let dataset = document.read(memory.get_ref())?;

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
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.set_document(document);
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());
    ///
    ///     let expected_result2 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
    ///     let dataset = vec![expected_result2.clone()];
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1, datastream.next().await.unwrap());
    ///     assert_eq!(expected_result2, datastream.next().await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "in_memory::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let document = self.document()?;
        let mut memory = self.memory.lock().await;

        let terminator = document.terminator()?;
        let footer = document.footer(dataset)?;
        let header = document.header(dataset)?;
        let body = document.write(dataset)?;

        if document.can_append() && !memory.get_ref().is_empty() {
            // Append mode: position before the last footer
            let start_pos = memory.get_ref().len().saturating_sub(footer.len());
            memory.set_position(start_pos as u64);
            memory.write_all(&terminator)?;
        } else {
            // Overwrite mode: clear buffer
            memory.get_mut().clear();
            memory.write_all(&header)?;
        }

        memory.write_all(&body)?;
        memory.write_all(&footer)?;
        memory.set_position(0);

        info!("Send data with success");
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
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Jsonl::default());
    ///
    ///     let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
    ///     connector.set_document(document);
    ///
    ///     connector.erase().await.unwrap();
    ///
    ///     let datastream = connector.fetch().await.unwrap();
    ///     assert!(datastream.is_none(), "The datastream must be empty");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "in_memory::erase")]
    async fn erase(&mut self) -> io::Result<()> {
        let mut memory = self.memory.lock().await;
        *memory = Cursor::default();

        info!("Erase data with success");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let paginator = Once {};
        paginator.paginate(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::json::Json;
    use crate::document::jsonl::Jsonl;
    use crate::DataResult;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

    #[apply(test!)]
    async fn len() {
        let connector = InMemory::new(r#"[{"column1":"value1"}]"#);
        assert!(
            0 < connector.len().await.unwrap(),
            "The length of the document is not greather than 0."
        );
    }
    #[apply(test!)]
    async fn is_empty() {
        let connector = InMemory::new("");
        assert_eq!(true, connector.is_empty().await.unwrap());
        let connector = InMemory::new("My text");
        assert_eq!(false, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn fetch() {
        let document = Jsonl::default();
        let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn send() {
        let document = Jsonl::default();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        let mut connector = InMemory::new(r#""#);
        connector.set_document(Box::new(document)).unwrap();
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1, datastream.next().await.unwrap());
        assert_eq!(expected_result2, datastream.next().await.unwrap());
    }
    #[apply(test!)]
    async fn erase() {
        let document = Jsonl::default();
        let mut connector = InMemory::new(r#"{"column1":"value1"}"#);
        connector.set_document(Box::new(document)).unwrap();
        connector.erase().await.unwrap();
        let datastream = connector.fetch().await.unwrap();
        assert!(datastream.is_none(), "The datastream must be empty");
    }
    #[apply(test!)]
    async fn paginate() {
        let mut connector = InMemory::default();
        let document = Json::default();
        connector.set_document(Box::new(document)).unwrap();
        let mut paging = connector.paginate().await.unwrap();
        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            paging.next().await.transpose().unwrap().is_none(),
            "Can't paginate more than one time."
        );
    }
}
