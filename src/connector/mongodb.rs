use crate::{document::DocumentType, step::DataResult};

use super::{Connector, Paginator};
use async_std::prelude::*;
use async_trait::async_trait;
use futures::StreamExt;
use mongodb::{
    bson::{doc, oid::ObjectId, Document},
    options::{FindOptions, InsertOneOptions, UpdateOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Error, ErrorKind, Result};
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct Mongodb {
    document_type: DocumentType,
    pub endpoint: String,
    #[serde(alias = "db")]
    pub database: String,
    #[serde(alias = "col")]
    pub collection: String,
    pub filter: Option<Document>,
    pub find_options: Option<FindOptions>,
    #[serde(skip_serializing)]
    pub update_options: Option<UpdateOptions>,
    #[serde(skip_serializing)]
    pub insert_options: Option<InsertOneOptions>,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or("".to_string())
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mongodb")
            .field("document_type", &self.document_type)
            .field("endpoint", &self.endpoint)
            .field("collection", &self.collection)
            .field("database", &self.database)
            .field("filter", &self.filter)
            .field("find_options", &self.find_options)
            .field("update_options", &self.update_options)
            .field("insert_options", &self.insert_options)
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
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(MongodbPaginator::new(self.clone()).await?))
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
    /// See [`Connector::document_type`] for more details.
    fn document_type(&self) -> DocumentType {
        self.document_type.clone()
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
    async fn fetch(&mut self) -> Result<()> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let options = self.find_options.clone();
        let filter = self.filter.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection(&collection);
        let cursor: mongodb::Cursor = collection
            .find(filter, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
        let data = serde_json::to_string(&docs)?;

        self.inner = Cursor::new(data.as_bytes().to_vec());

        Ok(())
    }
    /// See [`Connector::push_data`] for more details.
    async fn push_data(&mut self, data: DataResult) -> Result<()> {
        let document = self.document_type().document_inner();
        document.write_data(self, data.to_json_value()).await
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use chewdata::step::DataResult;
    /// use serde_json::{from_str, Value};
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
    ///     let value: Value = from_str(r#"{"column1":"value1"}"#)?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///     connector.erase().await?;
    ///     connector.fetch().await?;
    ///     assert_eq!(false, connector.inner_has_data());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn erase(&mut self) -> Result<()> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let db = client.database(&database);
        let collection = db.collection(&collection);
        collection
            .delete_many(doc! {}, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        Ok(())
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Example: Insert new data
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use chewdata::step::DataResult;
    /// use serde_json::{from_str, Value};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "send".into();
    ///     connector.erase().await?;
    ///
    ///     let value: Value = from_str(r#"{"column1":"value1"}"#)?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     let value: Value = from_str(r#"{"column1":"value2"}"#)?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
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
    /// use chewdata::step::DataResult;
    /// use serde_json::{from_str, Value};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "send".into();
    ///     connector.erase().await?;
    ///
    ///     let value: Value = from_str(r#"{"column1":"value1"}"#)?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///
    ///     let value: Value = from_str(format!(r#"{{"_id":"{}", "column1":"value3"}}"#, docs[0].as_document().unwrap().get("_id").unwrap().as_object_id().unwrap().to_string()).as_str())?;
    ///     let data = DataResult::Ok(value);
    ///
    ///     connector.push_data(data).await?;
    ///     connector.send().await?;
    ///
    ///     let mut buffer = String::default();
    ///     let mut connector_reader = connector.clone();
    ///     connector_reader.fetch().await?;
    ///     connector_reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = from_str(buffer.as_str())?;
    ///     assert_eq!("value3", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn send(&mut self) -> Result<()> {
        self.document_type().document_inner().close(self).await?;

        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();

        let docs: Vec<Document> = serde_json::from_slice(self.inner.get_ref())?;
        let update_options = self.update_options.clone();
        let insert_options = self.insert_options.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let db = client.database(&database);
        let collection = db.collection(&collection);

        for doc in docs {
            if let Some(id) = doc.get("_id") {
                let mut doc_without_id = doc.clone();
                doc_without_id.remove("_id").unwrap();

                collection
                    .update_one(
                        doc! { "_id": ObjectId::with_string(id.as_str().unwrap()).unwrap() },
                        doc! {"$set": doc_without_id},
                        update_options.clone(),
                    )
                    .await
                    .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                trace!(
                    slog_scope::logger(),
                    "Update the document in the collection"
                );
            } else {
                collection
                    .insert_one(doc.clone(), insert_options.clone())
                    .await
                    .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                trace!(
                    slog_scope::logger(),
                    "Insert the document in the collection"
                );
            }
        }

        self.flush().await?;
        self.clear();

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

#[derive(Debug)]
pub struct MongodbPaginator {
    connector: Mongodb,
    skip: i64,
    len: usize,
}

impl MongodbPaginator {
    pub async fn new(connector: Mongodb) -> Result<Self> {
        Ok(MongodbPaginator {
            connector: connector.clone(),
            skip: 0,
            len: connector.clone().len().await?,
        })
    }
}

#[async_trait]
impl Paginator for MongodbPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use mongodb::options::{FindOptions};
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut find_options = FindOptions::default();
    ///     find_options.limit = Some(5);
    ///
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.find_options = Some(find_options);
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     let mut connector = paginator.next_page().await?.unwrap();
    ///     let mut buffer1 = String::default();
    ///     let len1 = connector.read_to_string(&mut buffer1).await?;
    ///     assert!(true, "Can't read the content of the file.");
    ///
    ///     let mut connector = paginator.next_page().await?.unwrap();     
    ///     let mut buffer2 = String::default();
    ///     let len2 = connector.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files is not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>> {
        let mut connector = self.connector.clone();

        let find_options = match connector.find_options {
            Some(ref mut find_options) => find_options,
            None => return Ok(Some(Box::new(connector))),
        };

        let limit = match find_options.limit {
            Some(limit) => limit,
            None => return Ok(Some(Box::new(connector))),
        };

        self.skip = limit + find_options.skip.unwrap_or(self.skip);
  
        find_options.skip = Some(self.skip);
        connector.find_options = Some(find_options.clone());
        connector.fetch().await?;

        if !connector.inner_has_data() {
            return Ok(None);
        }
        
        Ok(Some(Box::new(connector)))
    }
}
