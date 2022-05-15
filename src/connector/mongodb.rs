use super::{Connector, Paginator};
use crate::{document::Document as ChewdataDocument, helper::mustache::Mustache};
use async_std::sync::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use mongodb::{
    bson::{doc, Document},
    options::{CountOptions, FindOptions, UpdateOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};
use std::{
    io::{Cursor, Error, ErrorKind, Result},
    sync::Arc,
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Mongodb {
    pub endpoint: String,
    #[serde(alias = "db")]
    pub database: String,
    #[serde(alias = "col")]
    pub collection: String,
    pub query: Box<Option<Value>>,
    pub find_options: Box<Option<FindOptions>>,
    #[serde(skip_serializing)]
    pub update_options: Box<Option<UpdateOptions>>,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
    #[serde(alias = "projection")]
    #[serde(skip)]
    pub inner: Box<Cursor<Vec<u8>>>,
}

impl Default for Mongodb {
    fn default() -> Self {
        let mut update_option = UpdateOptions::default();
        update_option.upsert = Some(true);

        Mongodb {
            endpoint: Default::default(),
            database: Default::default(),
            collection: Default::default(),
            query: Default::default(),
            find_options: Default::default(),
            update_options: Box::new(Some(update_option)),
            paginator_type: PaginatorType::default(),
            counter_type: None,
            inner: Default::default(),
        }
    }
}

impl fmt::Display for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mongodb")
            .field("endpoint", &self.endpoint)
            .field("collection", &self.collection)
            .field("database", &self.database)
            .field("query", &self.query)
            .field("find_options", &self.find_options)
            .field("update_options", &self.update_options)
            .finish()
    }
}

#[async_trait]
impl Connector for Mongodb {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        format!("{}/{}/{}", self.endpoint, self.database, self.collection)
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        let paginator = match self.paginator_type {
            PaginatorType::Offset(ref offset_paginator) => {
                let mut offset_paginator = offset_paginator.clone();
                offset_paginator.set_connector(self.clone());

                Box::new(offset_paginator) as Box<dyn Paginator + Send + Sync>
            }
            PaginatorType::Cursor(ref cursor_paginator) => {
                let mut cursor_paginator = cursor_paginator.clone();
                cursor_paginator.set_connector(self.clone());

                Box::new(cursor_paginator) as Box<dyn Paginator + Send + Sync>
            }
        };

        Ok(Pin::new(paginator))
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::is_variable`] for more details.
    fn is_variable(&self) -> bool {
        false
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(self.is_variable())
    }
    /// See [`Connector::is_empty`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     assert_eq!(true, connector.is_empty().await?);
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&mut self) -> Result<bool> {
        Ok(0 == self.len().await?)
    }
    /// See [`Connector::len`] for more details.
    async fn len(&mut self) -> Result<usize> {
        // TODO: find a way to have this method available
        // let hostname = self.endpoint.clone();
        // let database = self.database.clone();
        // let collection = self.collection.clone();

        // let client = match Client::with_uri_str(&hostname).await {
        //     Ok(client) => client,
        //     Err(e) => return Err(Error::new(ErrorKind::Interrupted, e)),
        // };
        // let db = client.database(&database);
        // let collection = db.collection(&collection);
        // let count = collection
        //     .estimated_document_count(None)
        //     .await
        //     .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        // Ok(count as usize)
        Ok(0)
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
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

        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let options = *self.find_options.clone();
        let query: Option<Document> = match *self.query {
            Some(ref query) => serde_json::from_str(query.to_string().as_str())?,
            None => None,
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let cursor = collection
            .find(query, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
        let data = serde_json::to_string(&docs)?;

        self.inner = Box::new(Cursor::new(data.as_bytes().to_vec()));

        info!("The connector fetch data with success");
        Ok(())
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "erase".into();
    ///
    ///     connector.write(r#"[{"column1":"value1"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///     connector.erase().await?;
    ///     connector.fetch().await?;
    ///     assert_eq!("[]".to_string(), String::from_utf8(connector.inner().clone()).unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn erase(&mut self) -> Result<()> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        collection
            .delete_many(doc! {}, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!("The connector erase data with success");
        Ok(())
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Example: Insert new data
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use serde_json::from_str;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "send_1".into();
    ///     connector.query = serde_json::from_str(r#"{"column1":"{{ column1 }}"}"#)?;
    ///     connector.update_options = serde_json::from_str(r#"{"upsert":true}"#)?;
    ///     connector.erase().await?;
    ///
    ///     connector.write(r#"[{"column1":"value1"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.query = Default::default();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     connector.write(r#"[{"column1":"value2"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.query = Default::default();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///     assert_eq!("value2", docs[1].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Update old data
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use serde_json::from_str;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "send_2".into();
    ///     connector.query = serde_json::from_str(r#"{"column1":"{{ column1 }}"}"#)?;
    ///     connector.update_options = serde_json::from_str(r#"{"upsert":true}"#)?;
    ///     connector.erase().await?;
    ///
    ///     connector.write(r#"[{"column1":"value1"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.query = Default::default();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     connector.query = serde_json::from_str(r#"{"column1":"value1"}"#)?;
    ///     connector.write(r#"[{"column1":"value2"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.query = Default::default();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value2", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///     let id = docs[0].as_document().unwrap().get_object_id("_id").unwrap().to_string();
    ///
    ///     connector.query = serde_json::from_str(format!(r#"{{"_id": {{"$oid":"{}"}}}}"#, id).as_str())?;
    ///     connector.write(r#"[{"column1":"value3"}]"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.query = Default::default();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value3", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn send(&mut self, _position: Option<isize>) -> Result<()> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();

        let docs: Vec<Document> = serde_json::from_slice(self.inner.get_ref())?;
        let update_options = self.update_options.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);

        for doc in docs {
            let query_update = match *self.query.clone() {
                Some(ref mut query_tmp) => {
                    let json_doc: Value = serde_json::to_value(doc.clone())?;
                    query_tmp.replace_mustache(json_doc.clone());
                    serde_json::from_str(query_tmp.to_string().as_str())?
                }
                None => doc.clone(),
            };

            let mut doc_without_id = doc.clone();
            if doc_without_id.get("_id").is_some() {
                doc_without_id.remove("_id");
            }

            trace!(
                query = format!("{:?}", &query_update).as_str(),
                update = format!("{:?}", doc! {"$set": &doc_without_id}).as_str(),
                "Query to update the collection"
            );

            let result = collection
                .update_many(
                    query_update,
                    doc! {"$set": doc_without_id},
                    *update_options.clone(),
                )
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

            if 0 < result.matched_count {
                trace!(
                    result = format!("{:?}", result).as_str(),
                    "Document(s) updated into the connection"
                );
            }
            if result.upserted_id.is_some() {
                trace!(
                    result = format!("{:?}", result).as_str(),
                    "Document(s) inserted into the connection"
                );
            }
        }

        self.clear();

        info!("The connector send data into the collection with success");
        Ok(())
    }
    /// See [`Connector::clear`] for more details.
    fn clear(&mut self) {
        self.inner = Default::default();
    }
}

#[async_trait]
impl async_std::io::Read for Mongodb {
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
impl async_std::io::Write for Mongodb {
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "scan")]
    #[serde(skip_serializing)]
    Scan(ScanCounter),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Scan(ScanCounter::default())
    }
}

impl CounterType {
    pub async fn count(
        &self,
        connector: Mongodb,
        _document: Option<Box<dyn ChewdataDocument>>,
    ) -> Result<Option<usize>> {
        match self {
            CounterType::Scan(scan) => scan.count(connector).await,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ScanCounter {
    #[serde(skip_serializing)]
    pub options: Option<CountOptions>,
}

impl ScanCounter {
    /// Get the number of items from the scan
    ///
    /// # Example: Get the number
    /// ```rust
    /// use chewdata::connector::mongodb::{Mongodb, ScanCounter};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let counter = ScanCounter::default();
    ///     assert!(counter.count(connector).await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Not get the number
    /// ```rust
    /// use chewdata::connector::mongodb::{Mongodb, ScanCounter};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "not_found".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let mut counter = ScanCounter::default();
    ///     assert_eq!(Some(0), counter.count(connector).await?);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    pub async fn count(&self, connector: Mongodb) -> Result<Option<usize>> {
        let hostname = connector.endpoint.clone();
        let database = connector.database.clone();
        let collection = connector.collection.clone();
        let options = self.options.clone();
        let query: Option<Document> = match *connector.query {
            Some(ref query) => serde_json::from_str(query.to_string().as_str())?,
            None => None,
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let count = collection
            .count_documents(query, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!(count = count, "The counter count with success");
        Ok(Some(count as usize))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "offset")]
    Offset(OffsetPaginator),
    #[serde(rename = "cursor")]
    Cursor(CursorPaginator),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Offset(OffsetPaginator::default())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct OffsetPaginator {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Mongodb>>,
    #[serde(skip)]
    pub has_next: bool,
}

impl Default for OffsetPaginator {
    fn default() -> Self {
        OffsetPaginator {
            limit: 100,
            skip: 0,
            count: None,
            connector: None,
            has_next: true,
        }
    }
}

impl OffsetPaginator {
    fn set_connector(&mut self, connector: Mongodb) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for OffsetPaginator {
    /// See [`Paginator::count`] for more details.
    ///
    /// # Example: Paginate indefinitely with the offset paginator
    /// ```rust
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, OffsetPaginator, CounterType, ScanCounter}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator::default());
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     assert!(paginator.count().await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn count(&mut self) -> Result<Option<usize>> {
        let connector = match self.connector {
            Some(ref mut connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't count the number of element in the collection without a connector",
            )),
        }?;

        let mut counter_type = None;
        if connector.counter_type.is_none() {
            counter_type = Some(CounterType::Scan(ScanCounter::default()));
        }

        if let Some(counter_type) = counter_type {
            self.count = counter_type.count(*connector.clone(), None).await?;

            info!(size = self.count, "The connector's counter count elements in the collection with success");
            return Ok(self.count);
        }

        trace!(size = self.count, "The connector's counter not exist or can't count the number of elements in the collection");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example: Paginate indefinitely with the offset paginator
    /// ```rust
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, OffsetPaginator}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
    ///         skip: 0,
    ///         limit: 1,
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;
    ///     let mut buffer1 = String::default();
    ///     let len1 = connector.read_to_string(&mut buffer1).await?;
    ///     assert!(true, "Can't read the content of the file.");
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;
    ///     let mut buffer2 = String::default();
    ///     let len2 = connector.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files is not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Paginate three times with the offset paginator and the paginator can return multi connectors in parallel
    /// ```rust
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, OffsetPaginator}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
    ///         skip: 0,
    ///         limit: 1,
    ///         count: Some(2),
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;

        let count_opt = match self.count {
            Some(count) => Some(count),
            None => self.count().await?,
        };

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();
                let mut find_options = FindOptions::default();
                find_options.skip = Some(skip as u64);
                find_options.limit = Some(limit as i64);
                new_connector.find_options = Box::new(Some(find_options.clone()));

                if let Some(count) = count_opt {
                    if count <= limit + skip {
                        has_next = false;
                    }
                }

                skip += limit;

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                yield Ok(new_connector as Box<dyn Connector>);
            }
            trace!("The stream stop to return new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        self.count.is_some()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct CursorPaginator {
    pub limit: usize,
    pub skip: usize,
    pub batch_size: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Mongodb>>,
    #[serde(skip)]
    pub cursor: Option<Arc<Mutex<mongodb::Cursor<Document>>>>,
}

impl Default for CursorPaginator {
    fn default() -> Self {
        CursorPaginator {
            limit: 100,
            skip: 0,
            batch_size: None,
            connector: None,
            cursor: None,
        }
    }
}

impl CursorPaginator {
    fn set_connector(&mut self, connector: Mongodb) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for CursorPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }
    /// See [`Paginator::set_document`] for more details.
    fn set_document(&mut self, _document: Box<dyn ChewdataDocument>) {}
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example: Paginate to the next cursor
    /// ```rust
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, CursorPaginator}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
    ///         skip: 0,
    ///         limit: 1,
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Reach the end of the cursor
    /// ```rust
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, CursorPaginator}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
    ///         skip: 0,
    ///         ..Default::default()
    ///     });
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(!paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_some());
    ///
    ///     let connector = stream.next().await.transpose()?;
    ///     assert!(connector.is_none());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = match self.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let hostname = connector.endpoint.clone();
        let database = connector.database.clone();
        let collection = connector.collection.clone();
        let mut skip = self.skip;
        let batch_size = self.limit;

        let mut options = (*connector.find_options.clone()).unwrap_or_default();
        options.skip = Some(skip as u64);

        let query: Option<Document> = match *connector.query {
            Some(ref query) => serde_json::from_str(query.to_string().as_str())?,
            None => None,
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let mut cursor = collection
            .find(query, Some(options))
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let stream = Box::pin(stream! {
            let mut docs: Vec<_> = Vec::default();
            while let Some(doc) = cursor.next().await {
                match doc {
                    Ok(doc) => docs.push(doc.clone()),
                    Err(e) => {
                        warn!(error = e.to_string().as_str(), "Document in error");
                        continue;
                    },
                };

                if batch_size <= docs.len() {
                    let mut new_connector = connector.clone();

                    let mut options = (*new_connector.find_options.clone()).unwrap_or_default();
                    options.skip = Some(skip as u64);
                    options.limit = Some(batch_size as i64);

                    new_connector.find_options = Box::new(Some(options.clone()));
                    skip+=batch_size;

                    let data = serde_json::to_string(&docs)?;
                    new_connector.inner = Box::new(Cursor::new(data.as_bytes().to_vec()));
                    docs = Vec::default();

                    trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                    yield Ok(new_connector as Box<dyn Connector>);
                }
            }
            if !docs.is_empty() {
                let mut new_connector = connector.clone();

                let mut options = (*new_connector.find_options.clone()).unwrap_or_default();
                options.skip = Some(skip as u64);
                options.limit = Some(batch_size as i64);

                new_connector.find_options = Box::new(Some(options.clone()));

                let data = serde_json::to_string(&docs)?;
                new_connector.inner = Box::new(Cursor::new(data.as_bytes().to_vec()));

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return the last new connector");
                yield Ok(new_connector as Box<dyn Connector>);
            }

        });
        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&self) -> bool {
        false
    }
}
