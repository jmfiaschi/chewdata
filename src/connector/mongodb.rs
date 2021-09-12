use super::{Connector, Paginator};
use crate::helper::mustache::Mustache;
use async_trait::async_trait;
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Error, ErrorKind, Result};
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Mongodb {
    pub endpoint: String,
    #[serde(alias = "db")]
    pub database: String,
    #[serde(alias = "col")]
    pub collection: String,
    pub query: Box<Option<Value>>,
    #[serde(alias = "projection")]
    pub find_options: Box<Option<FindOptions>>,
    #[serde(skip_serializing)]
    pub update_options: Box<Option<UpdateOptions>>,
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
        let options = *self.find_options.clone();
        let query: Option<Document> = match *self.query {
            Some(ref query) => serde_json::from_str(
                query
                .to_string()
                .as_str(),
            )?,
            None => None
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection(&collection);
        let cursor: mongodb::Cursor = collection
            .find(query, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
        let data = serde_json::to_string(&docs)?;

        self.inner = Box::new(Cursor::new(data.as_bytes().to_vec()));

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
        let collection = db.collection(&collection);

        for doc in docs {
            let query_update = match *self.query.clone() {
                Some(ref mut query_tmp) => {
                    let json_doc: Value = serde_json::to_value(doc.clone())?;
                    query_tmp.replace_mustache(json_doc.clone());
                    serde_json::from_str(query_tmp.to_string().as_str())?
                },
                None => {
                    doc.clone()
                }
            };

            let mut doc_without_id = doc.clone();
            if let Some(_) = doc_without_id.get("_id") {
                doc_without_id.remove("_id");
            }

            trace!(
                slog_scope::logger(),
                "update_many";
                "query" => format!("{:?}", &query_update),
                "update" => format!("{:?}", doc! {"$set": &doc_without_id}),
                "options" => format!("{:?}", &update_options),
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
                debug!(
                    slog_scope::logger(),
                    "Document(s) updated into the connection";
                    "result" => format!("{:?}", result),
                    "connector" => format!("{}", self),
                );
            }
            if let Some(_) = result.upserted_id {
                debug!(
                    slog_scope::logger(),
                    "Document(s) inserted into the connection";
                    "result" => format!("{:?}", result),
                    "connector" => format!("{}", self),
                );
            }
        }

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
}

impl MongodbPaginator {
    pub async fn new(connector: Mongodb) -> Result<Self> {
        Ok(MongodbPaginator {
            connector,
            skip: -1,
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
    ///     find_options.limit = Some(1);
    ///
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.find_options = Box::new(Some(find_options));
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

        let find_options = match *connector.find_options {
            Some(ref mut find_options) => find_options,
            None => return Ok(Some(Box::new(connector))),
        };

        let limit = match find_options.limit {
            Some(limit) => limit,
            None => return Ok(Some(Box::new(connector))),
        };

        self.skip = limit + find_options.skip.unwrap_or(self.skip);

        find_options.skip = Some(self.skip);
        connector.find_options = Box::new(Some(find_options.clone()));
        connector.fetch().await?;

        Ok(Some(Box::new(connector)))
    }
}
