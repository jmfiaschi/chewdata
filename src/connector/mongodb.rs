use super::{Connector, Paginator};
use crate::{
    document::Document as ChewdataDocument, helper::mustache::Mustache, DataSet, DataStream,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};
use std::{fmt, pin::Pin};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Mongodb {
    pub endpoint: String,
    #[serde(alias = "db")]
    pub database: String,
    #[serde(alias = "col")]
    pub collection: String,
    #[serde(alias = "params")]
    pub parameters: Value,
    pub filter: Box<Option<Value>>,
    pub find_options: Box<Option<FindOptions>>,
    #[serde(skip_serializing)]
    pub update_options: Box<Option<UpdateOptions>>,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
}

impl Default for Mongodb {
    fn default() -> Self {
        let mut update_option = UpdateOptions::default();
        update_option.upsert = Some(true);

        Mongodb {
            endpoint: Default::default(),
            database: Default::default(),
            collection: Default::default(),
            parameters: Default::default(),
            filter: Default::default(),
            find_options: Default::default(),
            update_options: Box::new(Some(update_option)),
            paginator_type: PaginatorType::default(),
            counter_type: None,
        }
    }
}

impl fmt::Display for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        futures::executor::block_on(async {
            let hostname = self.endpoint.clone();
            let database = self.database.clone();
            let collection = self.collection.clone();
            let options = *self.find_options.clone();
            let filter: Option<Document> = match self.filter(self.parameters.clone()) {
                Some(filter) => serde_json::from_str(filter.to_string().as_str()).unwrap(),
                None => None,
            };

            let client = Client::with_uri_str(&hostname).await.unwrap();
            let db = client.database(&database);
            let collection = db.collection::<Document>(&collection);
            let cursor = collection.find(filter, options).await.unwrap();
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
            let data = serde_json::to_string(&docs).unwrap();

            write!(f, "{}", data)
        })
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Mongodb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mongodb")
            .field("endpoint", &self.endpoint)
            .field("collection", &self.collection)
            .field("database", &self.database)
            .field("parameters", &self.parameters)
            .field("filter", &self.filter)
            .field("find_options", &self.find_options)
            .field("update_options", &self.update_options)
            .finish()
    }
}

impl Mongodb {
    /// Get new filter value link to the parameters in input
    fn filter(&self, parameters: Value) -> Option<Value> {
        let mut filter = match *self.filter.clone() {
            Some(filter) => filter,
            None => return None,
        };

        filter.replace_mustache(parameters);

        Some(filter)
    }
}

#[async_trait]
impl Connector for Mongodb {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        format!("{}/{}/{}", self.endpoint, self.database, self.collection)
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
    /// See [`Connector::is_variable`] for more details.
    fn is_variable(&self) -> bool {
        match *self.filter.clone() {
            Some(filter) => filter.has_mustache(),
            None => false,
        }
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(false)
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::document::json::Json;
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
    ///     let len = connector.len().await.unwrap();
    ///     assert!(
    ///         0 < len,
    ///         "The connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "mongodb::len")]
    async fn len(&mut self) -> Result<usize> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection_name = self.collection.clone();

        let client = match Client::with_uri_str(&hostname).await {
            Ok(client) => client,
            Err(e) => return Err(Error::new(ErrorKind::Interrupted, e)),
        };
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection_name);
        let len = collection
            .estimated_document_count(None)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!(len, "Number of records found in the resource");

        Ok(len as usize)
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     let datastream = connector.fetch(&document).await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "mongodb::fetch")]
    async fn fetch(
        &mut self,
        document: &dyn ChewdataDocument,
    ) -> std::io::Result<Option<DataStream>> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let options = *self.find_options.clone();
        let filter: Option<Document> = match self.filter(self.parameters.clone()) {
            Some(filter) => serde_json::from_str(filter.to_string().as_str())?,
            None => None,
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let cursor = collection
            .find(filter, options)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
        let data = serde_json::to_vec(&docs)?;

        info!("The connector fetch data with success");

        if !document.has_data(&data)? {
            return Ok(None);
        }

        let dataset = document.read(&data)?;

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
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use serde_json::from_str;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "send_1".into();
    ///     connector.erase().await.unwrap();
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     connector.send(&document, &dataset).await.unwrap();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "mongodb::send")]
    async fn send(
        &mut self,
        _document: &dyn ChewdataDocument,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();

        let mut docs: Vec<Document> = Vec::default();
        for data in dataset {
            docs.push(
                serde_json::from_value(data.to_value())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }

        let update_options = self.update_options.clone();

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let parameters = self.parameters.clone();

        for doc in docs {
            let mut doc_without_id = doc.clone();
            if doc_without_id.get("_id").is_some() {
                doc_without_id.remove("_id");
            }

            let filter_update = match self.filter(parameters.clone()) {
                Some(mut filter) => {
                    let json_doc: Value = serde_json::to_value(doc.clone())?;
                    filter.replace_mustache(json_doc.clone());
                    serde_json::from_str(filter.to_string().as_str())?
                }
                None => match doc.get("_id") {
                    Some(id) => doc! { "_id": id },
                    None => doc_without_id.clone(),
                },
            };

            trace!(
                filter = format!("{:?}", &filter_update).as_str(),
                update = format!("{:?}", doc! {"$set": &doc_without_id}).as_str(),
                "Query to update the collection"
            );

            let result = collection
                .update_many(
                    filter_update,
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

        info!("The connector send data into the collection with success");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Mongodb::default();
    ///     connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
    ///     connector.database = "tests".into();
    ///     connector.collection = "erase".into();
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1];
    ///     connector.send(&document, &dataset).await.unwrap();
    ///     connector.erase().await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.filter = Default::default();
    ///     let datastream = connector_read.fetch(&document).await.unwrap();
    ///     assert!(datastream.is_none(), "The datastream should be empty");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "mongodb::erase")]
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
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "metadata")]
    #[serde(skip_serializing)]
    Metadata(MetadataCounter),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Metadata(MetadataCounter::default())
    }
}

impl CounterType {
    pub async fn count(
        &self,
        connector: Mongodb,
        _document: Option<Box<dyn ChewdataDocument>>,
    ) -> Result<Option<usize>> {
        match self {
            CounterType::Metadata(counter) => counter.count(connector).await,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct MetadataCounter {}

impl MetadataCounter {
    /// Get the number of items from the metadata
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::mongodb::{Mongodb, MetadataCounter};
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
    ///     let counter = MetadataCounter::default();
    ///     assert!(counter.count(connector).await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "metadata_counter::count")]
    pub async fn count(&self, connector: Mongodb) -> Result<Option<usize>> {
        let count = connector.clone().len().await?;

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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{mongodb::{Mongodb, PaginatorType, OffsetPaginator, CounterType, MetadataCounter}, Connector};
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
    ///     assert!(paginator.count().await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::count")]
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
            counter_type = Some(CounterType::default());
        }

        if let Some(counter_type) = counter_type {
            self.count = counter_type.count(*connector.clone(), None).await?;

            info!(
                size = self.count,
                "The connector's counter count elements in the collection with success"
            );
            return Ok(self.count);
        }

        trace!(size = self.count, "The connector's counter not exist or can't count the number of elements in the collection");
        Ok(None)
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "offset_paginator::stream")]
    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let mut paginator = self.clone();
        let connector = match paginator.connector.clone() {
            Some(connector) => Ok(connector),
            None => Err(Error::new(
                ErrorKind::Interrupted,
                "The paginator can't paginate without a connector",
            )),
        }?;

        let mut has_next = true;
        let limit = self.limit;
        let mut skip = self.skip;

        let count_opt = match paginator.count {
            Some(count) => Some(count),
            None => paginator.count().await?,
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
    #[serde(skip)]
    pub connector: Option<Box<Mongodb>>,
}

impl Default for CursorPaginator {
    fn default() -> Self {
        CursorPaginator {
            limit: 100,
            skip: 0,
            connector: None,
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
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    ///     let mut stream = connector.paginator().await?.stream().await?;
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the first reader.");
    ///     assert!(stream.next().await.transpose()?.is_some(), "Can't get the second reader.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "cursor_paginator::stream")]
    async fn stream(
        &self,
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
        let parameters = connector.parameters.clone();
        let skip = self.skip;
        let batch_size = self.limit;

        let mut options = (*connector.find_options.clone()).unwrap_or_default();
        options.skip = Some(skip as u64);

        let filter: Option<Document> = match connector.filter(parameters) {
            Some(filter) => serde_json::from_str(filter.to_string().as_str())?,
            None => None,
        };

        let client = Client::with_uri_str(&hostname)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let db = client.database(&database);
        let collection = db.collection::<Document>(&collection);
        let cursor = collection
            .find(filter, Some(options))
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        let cursor_size = cursor.count().await;

        let stream = Box::pin(stream! {
            for i in 0..cursor_size {
                if 0 == i%batch_size || i == cursor_size {
                    let mut new_connector = connector.clone();

                    let mut options = (*new_connector.find_options.clone()).unwrap_or_default();
                    options.skip = Some(i as u64);
                    options.limit = Some(batch_size as i64);

                    new_connector.find_options = Box::new(Some(options.clone()));

                    trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                    yield Ok(new_connector as Box<dyn Connector>);
                }
            }
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
    use crate::document::json::Json;
    use crate::DataResult;
    use async_std::prelude::StreamExt;
    use json_value_merge::Merge;
    use json_value_search::Search;

    #[async_std::test]
    async fn is_empty() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        assert_eq!(false, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
    async fn len() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        let len = connector.len().await.unwrap();
        assert!(0 < len, "The connector should have a size upper than zero");
    }
    #[async_std::test]
    async fn fetch() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero"
        );
    }
    #[async_std::test]
    async fn send_new_data() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "tests".into();
        connector.collection = "send_1".into();
        connector.erase().await.unwrap();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.filter = Default::default();
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        assert_eq!(
            "value1",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("column1")
                .unwrap()
                .as_str()
                .unwrap()
        );
        assert_eq!(
            "value2",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("column1")
                .unwrap()
                .as_str()
                .unwrap()
        );
    }
    #[async_std::test]
    async fn update_existing_data() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "tests".into();
        connector.collection = "send_2".into();
        connector.erase().await.unwrap();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.filter = Default::default();
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();
        let data_1_id = data_1.to_value().search("/_id").unwrap().unwrap();

        let mut result3: Value = serde_json::from_str(r#"{"column1":"value3"}"#).unwrap();
        result3.merge_in("/_id", data_1_id).unwrap();
        let expected_result3 = DataResult::Ok(result3);
        let dataset = vec![expected_result3.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.filter = Default::default();
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        assert_eq!(
            "value3",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("column1")
                .unwrap()
                .as_str()
                .unwrap()
        );
        assert_eq!(
            "value2",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("column1")
                .unwrap()
                .as_str()
                .unwrap()
        );
    }
    #[async_std::test]
    async fn erase() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "tests".into();
        connector.collection = "erase".into();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1];
        connector.send(&document, &dataset).await.unwrap();
        connector.erase().await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.filter = Default::default();
        let datastream = connector_read.fetch(&document).await.unwrap();
        assert!(datastream.is_none(), "The datastream should be empty");
    }
    #[async_std::test]
    async fn paginator_scan_counter_count() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        let counter = MetadataCounter::default();
        assert!(counter.count(connector).await.unwrap().is_some());
    }
    #[async_std::test]
    async fn paginator_scan_counter_count_none() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "not_found".into();
        connector.collection = "startup_log".into();
        let counter = MetadataCounter::default();
        assert_eq!(Some(0), counter.count(connector).await.unwrap());
    }
    #[async_std::test]
    async fn paginator_offset_count() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        connector.paginator_type = PaginatorType::Offset(OffsetPaginator::default());
        let mut paginator = connector.paginator().await.unwrap();
        assert!(paginator.count().await.unwrap().is_some());
    }
    #[async_std::test]
    async fn paginator_offset_count_with_skip_and_limit() {
        let document = Json::default();

        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        connector.paginator_type = PaginatorType::Offset(OffsetPaginator {
            skip: 0,
            limit: 1,
            ..Default::default()
        });
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut paginate = paginator.stream().await.unwrap();
        let mut connector = paginate.next().await.transpose().unwrap().unwrap();

        let mut datastream = connector.fetch(&document).await.unwrap().unwrap();
        let data_1 = datastream.next().await.unwrap();

        let mut connector = paginate.next().await.transpose().unwrap().unwrap();
        let mut datastream = connector.fetch(&document).await.unwrap().unwrap();
        let data_2 = datastream.next().await.unwrap();
        assert!(
            data_1 != data_2,
            "The content of this two stream are not different."
        );
    }
    #[async_std::test]
    async fn paginator_cursor_stream() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
            skip: 0,
            limit: 1,
            ..Default::default()
        });
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
    }
    #[async_std::test]
    async fn paginator_cursor_stream_reach_end() {
        let mut connector = Mongodb::default();
        connector.endpoint = "mongodb://admin:admin@localhost:27017".into();
        connector.database = "local".into();
        connector.collection = "startup_log".into();
        connector.paginator_type = PaginatorType::Cursor(CursorPaginator {
            skip: 0,
            ..Default::default()
        });
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_some());
        let connector = stream.next().await.transpose().unwrap();
        assert!(connector.is_none());
    }
}
