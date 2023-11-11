//! Read and write data into mongodb database.
//!
//! ### Configuration
//!
//! | key            | alias      | Description                                                                                                                                                                   | Default Value | Possible Values                                                                      |
//! | -------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------ |
//! | type           | -          | Required in order to use this connector                                                                                                                                       | `mongodb`     | `mongodb` / `mongo`                                                                  |
//! | endpoint       | -          | Endpoint of the connector                                                                                                                                                     | `null`        | String                                                                               |
//! | database       | db         | The database name                                                                                                                                                             | `null`        | String                                                                               |
//! | collection     | col        | The collection name                                                                                                                                                           | `null`        | String                                                                               |
//! | query          | -          | Query to find an element into the collection                                                                                                                                  | `null`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.find/)       |
//! | find_options   | projection | Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter. For details, see Projection | `null`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.find/)       |
//! | update_options | -          | Options apply during the update)                                                                                                                                              | `null`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/) |
//! | paginator      | -          | Paginator parameters.                                       | [`crate::connector::paginator::mongodb::offset::Offset`]      | [`crate::connector::paginator::mongodb::offset::Offset`] / [`crate::connector::paginator::mongodb::cursor::Cursor`]        |
//! | counter        | count      | Use to find the total of elements in the resource. used for the paginator        | [`crate::connector::counter::psql::metadata::Metadata`]        | [`crate::connector::counter::psql::metadata::Metadata`]                |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "w",
//!         "connector":{
//!             "type": "mongodb",
//!             "endpoint": "mongodb://admin:admin@localhost:27017",
//!             "db": "tests",
//!             "collection": "test",
//!             "update_options": {
//!                 "upsert": true
//!             }
//!         },
//!         "thread_number":3
//!     }
//! ]
//! ```
use super::counter::mongodb::CounterType;
use super::Connector;
use crate::connector::paginator::mongodb::PaginatorType;
use crate::{
    document::Document as ChewdataDocument, helper::mustache::Mustache, DataSet, DataStream,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
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
    pub counter_type: CounterType,
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
            counter_type: CounterType::default(),
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
    pub fn filter(&self, parameters: Value) -> Option<Value> {
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
        match self.counter_type.count(self).await {
            Ok(count) => Ok(count),
            Err(e) => {
                warn!(
                    error = e.to_string(),
                    "The counter can't count the number of element, return 0."
                );

                Ok(0)
            }
        }
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

        info!("The connector fetch data with success.");

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
                "Query to update the collection."
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
                    "Document(s) updated into the connection."
                );
            }
            if result.upserted_id.is_some() {
                trace!(
                    result = format!("{:?}", result).as_str(),
                    "Document(s) inserted into the connection."
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

        info!("The connector erase data with success.");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        self.paginator_type.paginate(self).await
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
        assert!(0 < len, "The connector should have a size upper than zero.");
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
            "The inner connector should have a size upper than zero."
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
        result3.merge_in("/_id", &data_1_id).unwrap();
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
        assert!(datastream.is_none(), "The datastream should be empty.");
    }
}
