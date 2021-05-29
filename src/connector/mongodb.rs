use super::{Connector, ConnectorType, Reader, Writer, Paginator};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document, oid::ObjectId},
    options::{FindOptions, UpdateOptions, InsertOneOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use async_trait::async_trait;
use std::{fmt, pin::Pin};
use std::task::{Poll, Context};
use async_std::prelude::*;
use std::io::{Cursor, Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct Mongodb {
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
}

impl fmt::Display for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}

#[async_trait]
impl Connector for Mongodb {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        format!("{}/{}/{}", self.endpoint, self.database, self.collection)
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator>>> {
        Ok(Box::pin(MongodbPaginator::new(ConnectorType::Mongodb(self.clone())).await?))
    }
    /// See [`Connector::reader`] for more details.
    async fn reader(&self) -> Result<Box<dyn Reader>> {
        MongodbReader::new(ConnectorType::Mongodb(self.clone())).await
    }
    /// See [`Connector::writer`] for more details.
    async fn writer(&self) -> Result<Box<dyn Writer>> {
        MongodbWriter::new(ConnectorType::Mongodb(self.clone())).await
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::is_variable_path`] for more details.
    fn is_variable_path(&self) -> bool { false }
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
    ///     assert_eq!(false, connector.is_empty().await?);
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&self) -> Result<bool> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        
        let client = match Client::with_uri_str(&hostname).await {
            Ok(client) => client,
            Err(e) => {
                return Err(Error::new(ErrorKind::Interrupted, e))
            }
        };
        let db = client.database(&database);
        let collection = db.collection(&collection);
        let doc = collection.find_one(Some(doc! {}), None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        match doc {
            Some(_) => Ok(false),
            None => Ok(true)
        }
    }
    /// See [`Connector::len`] for more details.
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
    ///     assert!(0 < connector.len().await?);
    ///     Ok(())
    /// }
    /// ```
    async fn len(&self) -> Result<usize> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        
        let client = match Client::with_uri_str(&hostname).await {
            Ok(client) => client,
            Err(e) => {
                return Err(Error::new(ErrorKind::Interrupted, e))
            }
        };
        let db = client.database(&database);
        let collection = db.collection(&collection);
        let count = collection.estimated_document_count(None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        
        Ok(count as usize)
    }
}

#[derive(Debug)]
pub struct MongodbReader {
    connector_type: ConnectorType,
    inner: Cursor<Vec<u8>>,
    collection: mongodb::Collection
}

impl MongodbReader {
    /// Create new Reader and init the buffer.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::{Mongodb, MongodbReader};
    /// use chewdata::connector::{Connector, ConnectorType, Reader};
    /// use serde_json::Value;
    /// use std::io;
    /// 
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let mut reader = MongodbReader::new(ConnectorType::Mongodb(connector.clone())).await?;
    ///     assert!(0 < reader.inner().len(), format!("The collection shouldn't be empty. {}", connector));
    ///     Ok(())
    /// }
    /// ```
    pub async fn new(connector_type: ConnectorType) -> Result<Box<dyn Reader>> {
        let connector = match &connector_type {
            ConnectorType::Mongodb(connector) =>  connector,
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Connector not handle"))
        };
        let hostname = connector.endpoint.clone();
        let database = connector.database.clone();
        let collection = connector.collection.clone();
        let options = connector.find_options.clone();
        let filter = connector.filter.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection(&collection);
        let cursor: mongodb::Cursor = collection.find(filter, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
        let docs_string = serde_json::to_string(&docs)?;

        Ok(Box::new(MongodbReader {
            connector_type: connector_type,
            inner: Cursor::new(docs_string.as_bytes().to_vec()),
            collection: collection,
        }))
    }
}

impl Reader for MongodbReader {
    /// See [`Reader::connector_type`] for more details.
    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }
    /// See [`Reader::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
}

#[async_trait]
impl async_std::io::Read for MongodbReader {
    /// See [`Read::poll_read`] for more details.
    ///
    /// # Example:
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::io::Read;
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
    ///     let mut buffer = String::default();
    ///     let mut reader = connector.reader().await?;
    ///     let len = reader.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Can't read the content of the file");
    ///     let len = reader.read_to_string(&mut buffer).await?;
    ///     assert!(0 == len, "Can't reach the end of the file.");
    ///     Ok(())
    /// }
    /// ```
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Read::read(&mut self.inner, buf))
    }
}

#[derive(Debug)]
pub struct MongodbWriter {
    connector_type: ConnectorType,
    inner: Cursor<Vec<u8>>,
    collection: mongodb::Collection
}

impl MongodbWriter {
    pub async fn new(connector_type: ConnectorType) -> Result<Box<dyn Writer>> {
        let connector = match &connector_type {
            ConnectorType::Mongodb(connector) =>  connector,
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Connector not handle"))
        };
        let hostname = connector.endpoint.clone();
        let database = connector.database.clone();
        let collection = connector.collection.clone();
        let client = match Client::with_uri_str(&hostname).await {
            Ok(client) => client,
            Err(e) => {
                return Err(Error::new(ErrorKind::Interrupted, e))
            }
        };
        let db = client.database(&database);

        let collection = db.collection(&collection);

        Ok(Box::new(MongodbWriter {
            connector_type: connector_type,
            inner: Cursor::new(Vec::default()),
            collection: collection
        }))
    }
}

#[async_trait]
impl Writer for MongodbWriter {
    /// See [`Writer::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    /// 
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "erase".into();
    ///     let mut writer = connector.writer().await?;
    ///
    ///     writer.write(r#"[{"column1":"value1"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     writer.flush_into(-1).await?;
    ///     writer.erase().await?;
    ///     let mut buffer = String::default();
    ///     let mut reader = connector.reader().await?;
    ///     reader.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[]"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn erase(&mut self) -> Result<()> {
        self.collection.delete_many(doc! {}, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        Ok(())
    }
    /// See [`Writer::flush_into`] for more details.
    ///
    /// # Example: Insert and update data
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
    ///     connector.collection = "flush_into".into();
    ///     let mut writer = connector.writer().await?;
    ///     writer.erase().await?;
    ///
    ///     writer.write(r#"[{"column1":"value1"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     writer.flush_into(-1).await?;
    ///     let mut buffer = String::default();
    ///     let mut reader = connector.reader().await?;
    ///     reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = serde_json::from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     writer.write(r#"[{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     writer.flush_into(-1).await?;
    ///     let mut buffer = String::default();
    ///     let mut reader = connector.reader().await?;
    ///     reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = serde_json::from_str(buffer.as_str())?;
    ///     assert_eq!("value1", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///     assert_eq!("value2", docs[1].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     writer.write(format!(r#"[{{"_id":"{}", "column1":"value3"}}]"#, docs[0].as_document().unwrap().get("_id").unwrap().as_object_id().unwrap().to_string()).to_string().into_bytes().as_slice()).await?;
    ///     writer.flush_into(-1).await?;
    ///     let mut buffer = String::default();
    ///     let mut reader = connector.reader().await?;
    ///     reader.read_to_string(&mut buffer).await?;
    ///     let docs: Vec<mongodb::bson::Bson> = serde_json::from_str(buffer.as_str())?;
    ///     assert_eq!("value3", docs[0].as_document().unwrap().get("column1").unwrap().as_str().unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn flush_into(&mut self, _position: i64) -> Result<()> {
        let connector = match &self.connector_type {
            ConnectorType::Mongodb(connector) =>  connector,
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Connector not handle"))
        };
        let docs: Vec<Document> = serde_json::from_slice(self.inner.get_ref())?;
        let update_options = connector.update_options.clone();
        let insert_options = connector.insert_options.clone();
        
        for doc in docs {
            if let Some(id) = doc.get("_id") {
                let mut doc_without_id = doc.clone();
                doc_without_id.remove("_id").unwrap();

                self
                .collection
                .update_one(doc! { "_id": ObjectId::with_string(id.as_str().unwrap()).unwrap() }, doc! {"$set": doc_without_id}, update_options.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                trace!(slog_scope::logger(), "Update the document in the collection");
            } else {
                self.collection.insert_one(doc.clone(), insert_options.clone())
                .await
                .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

                trace!(slog_scope::logger(), "Insert the document in the collection");
            }
        }
        self.inner = Cursor::new(Vec::default());
        self.flush().await
    }
    /// See [`Writer::connector_type`] for more details.
    fn connector_type(&self) -> &ConnectorType {
        &self.connector_type
    }
    /// See [`Writer::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
}

#[async_trait]
impl async_std::io::Write for MongodbWriter {
    /// See [`Write::poll_write`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use async_std::io::Write;
    /// use async_std::prelude::*;
    /// use std::io;
    /// 
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "erase".into();
    ///     let mut writer = connector.writer().await?;
    ///
    ///     let buffer = "My text";
    ///     let len = writer.write(buffer.to_string().into_bytes().as_slice()).await?;
    ///     assert_eq!(7, len);
    ///     assert_eq!("My text", format!("{}", writer));
    ///
    ///     let len = writer.write(buffer.to_string().into_bytes().as_slice()).await?;
    ///     assert_eq!(7, len);
    ///     assert_eq!("My textMy text", format!("{}", writer));
    ///
    ///     Ok(())
    /// }
    /// ```
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>{
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct MongodbPaginator {
    connector_type: ConnectorType,
    skip: i64,
    len: usize,
}

impl MongodbPaginator {
    pub async fn new(connector_type: ConnectorType) -> Result<Self> {
        let connector = match &connector_type {
            ConnectorType::Mongodb(connector) =>  connector.clone(),
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Connector not handle"))
        };
        Ok( MongodbPaginator {
            connector_type: connector_type,
            skip: -1,
            len: connector.len().await?,
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
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    /// 
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     let mut find_options: FindOptions = Default::default();
    ///     find_options.limit = Some(5);
    ///     connector.find_options = Some(find_options);
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     let mut reader = paginator.next_page().await.unwrap()?;     
    ///     let mut buffer1 = String::default();
    ///     let len1 = reader.read_to_string(&mut buffer1).await?;
    ///     assert!(0 < len1, "Can't read the content of the file.");
    ///
    ///     let mut reader = paginator.next_page().await.unwrap()?;     
    ///     let mut buffer2 = String::default();
    ///     let len2 = reader.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files is not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Option<Result<Box<dyn Reader>>> {
        let mut connector = match &self.connector_type {
            ConnectorType::Mongodb(connector) =>  connector.clone(),
            _ => return Some(Err(Error::new(ErrorKind::InvalidInput, "Connector not handle")))
        };

        let find_options = match connector.find_options {
            Some(ref mut find_options) => find_options,
            None => return Some(MongodbReader::new(ConnectorType::Mongodb(connector)).await)
        };
        
        let limit = match find_options.limit {
            Some(limit) => limit,
            None => return Some(MongodbReader::new(ConnectorType::Mongodb(connector)).await)
        };

        self.skip = limit + find_options.skip.unwrap_or(self.skip);

        find_options.skip = Some(self.skip);
        connector.find_options = Some(find_options.clone());

        if 0 < (self.skip - self.len as i64) + limit {
            return None;
        } else {
            return Some(MongodbReader::new(ConnectorType::Mongodb(connector)).await);
        }
    }
}
