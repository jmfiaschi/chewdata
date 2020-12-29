use crate::connector::Connector;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions, InsertOneOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Cursor, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize, Clone)]
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
    pub can_truncate: bool,
    #[serde(skip)]
    inner: Cursor<Vec<u8>>,
    #[serde(skip)]
    is_truncated: bool,
}

impl Default for Mongodb {
    fn default() -> Self {
        Mongodb {
            endpoint: String::default(),
            database: String::default(),
            collection: String::default(),
            filter: None,
            find_options: None,
            update_options: None,
            insert_options: None,
            inner: Cursor::default(),
            can_truncate: false,
            is_truncated: false,
        }
    }
}

impl fmt::Display for Mongodb {
    /// Can't display the content.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    ///
    /// let connector = Mongodb::default();
    /// assert_eq!("", format!("{}", connector));
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &String::from_utf8_lossy(self.inner.get_ref()))
    }
}

impl Mongodb {
    /// Initilize the inner buffer.
    fn init_inner(&mut self) -> Result<()> {
        info!(slog_scope::logger(), "Init inner buffer"; "parameters" => format!("{:?}", self));
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let options = self.find_options.clone();
        let filter = self.filter.clone();
        let mut buffer = Vec::new();

        futures::executor::block_on(async {
            let client = match Client::with_uri_str(&hostname).await {
                Ok(client) => client,
                Err(e) => {
                    error!(slog_scope::logger(), "{}", e);
                    return ();
                }
            };
            let db = client.database(&database);
            let collection = db.collection(&collection);
            let cursor: mongodb::Cursor = match collection.find(filter, options).await
            {
                Ok(cursor) => cursor,
                Err(e) => {
                    error!(slog_scope::logger(), "{}", e);
                    return ();
                }
            };
            let docs: Vec<_> = cursor.map(|doc| doc.unwrap()).collect().await;
            let docs_string = match serde_json::to_string(&docs) {
                Ok(string) => string,
                Err(e) => {
                    error!(slog_scope::logger(), "{}", e);
                    return ();
                }
            };
            match buffer.write_all(docs_string.as_bytes()) {
                Ok(_) => (),
                Err(e) => {
                    error!(slog_scope::logger(), "{}", e);
                    return ();
                }
            };
        });

        self.inner.write_all(buffer.as_slice())?;
        // initialize the position of the cursor
        self.inner.set_position(0);
        info!(slog_scope::logger(), "Init inner buffer ended");

        Ok(())
    }
}

impl Connector for Mongodb {
    fn set_parameters(&mut self, _parameters: Value) {}
    fn path(&self) -> String {
        String::new()
    }
    /// Check if the inner buffer in the connector is empty.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Mongodb::default();
    /// assert_eq!(true, connector.is_empty().unwrap());
    /// ```
    fn is_empty(&self) -> Result<bool> {
        Ok(0 == self.inner.get_ref().len())
    }
    /// Return true because the connector truncate the inner when it write the data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Mongodb::default();
    /// assert_eq!(true, connector.will_be_truncated());
    /// ```
    fn will_be_truncated(&self) -> bool {
        self.can_truncate && !self.is_truncated
    }
    /// Get the document size 0.
    ///  
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    ///
    /// let mut connector = Mongodb::default();
    /// assert_eq!(0, connector.len().unwrap());
    /// ```
    fn len(&self) -> Result<usize> {
        Ok(0)
    }
    /// Get the connect buffer inner reference.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    /// use chewdata::connector::Connector;
    ///
    /// let connector = Mongodb::default();
    /// let vec: Vec<u8> = Vec::default();
    /// assert_eq!(&vec, connector.inner());
    /// ```
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
}

impl Read for Mongodb {
    /// Read the data from the stdin and write it into the buffer.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.inner.clone().into_inner().is_empty() {
            self.init_inner()?;
        }

        self.inner.read(buf)
    }
}

impl Write for Mongodb {
    /// Write the data into the inner buffer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::mongodb::Mongodb;
    /// use std::io::Write;
    ///
    /// let mut connector = Mongodb::default();
    /// let buffer = "My text";
    /// let len = connector.write(buffer.to_string().into_bytes().as_slice()).unwrap();
    /// assert_eq!(7, len);
    /// assert_eq!("My text", format!("{}", connector));
    /// ```
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }
    /// The flush send all the data into the stdout.
    fn flush(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Flush started");
        let hostname = self.endpoint.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let docs: Vec<Document> = serde_json::from_slice(self.inner.get_ref()).unwrap();
        let will_be_truncated = self.will_be_truncated();
        let update_options = self.update_options.clone();
        let insert_options = self.insert_options.clone();

        futures::executor::block_on(async {
            let client = match Client::with_uri_str(&hostname).await {
                Ok(client) => client,
                Err(e) => {
                    error!(slog_scope::logger(), "{}", e);
                    return ();
                }
            };
            let db = client.database(&database);
            let collection = db.collection(&collection);

            if will_be_truncated {
                collection.delete_many(doc! {}, None).await.unwrap();
                self.is_truncated = true;
                info!(slog_scope::logger(), "The collection link to the connector has been truncate");
            }
            for doc in docs {
                if let Some(id) = doc.get("_id") {
                    collection.update_one(doc! { "_id": id }, doc, update_options.clone()).await.unwrap();
                    trace!(slog_scope::logger(), "Update the document in the collection");
                } else {
                    collection.insert_one(doc, insert_options.clone()).await.unwrap();
                    trace!(slog_scope::logger(), "Insert the document in the collection");
                }
            }
        });

        self.inner = Cursor::new(Vec::default());
        debug!(slog_scope::logger(), "Flush ended");
        Ok(())
    }
}
