//! Read and write data into postgres database.
//!
//! ### Configuration
//!
//! | key        | alias           | Description                                      | Default Value | Possible Values         |
//! | ---------- | --------------- | ------------------------------------------------ | ------------- | ----------------------- |
//! | type       | -               | Required in order to use this connector          | `psql`        | `psql` / `pgsql` / `pg` |
//! | endpoint   | `url`           | Endpoint of the connector                        | ``            | String                  |
//! | database   | `db`            | The database name                                | ``            | String                  |
//! | collection | `col` / `table` | The collection name                              | ``            | String                  |
//! | query      | -               | SQL Query to find an element into the collection | ``            | String                  |
//! | parameters | `params`        | Parameters used to inject into the SQL query     | `null`        | Json structure          |
//! | paginator  | -               | Paginator parameters                             | [`self::Offset`]        | [`self::Offset`] |
//! | counter    | count           | Count the number of elements for pagination      | `null`        | [`self::Scan`] |
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
use super::{Connector, Paginator};
use crate::helper::json_pointer::JsonPointer;
use crate::{document::Document, helper::mustache::Mustache, DataResult};
use crate::{DataSet, DataStream};
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::postgres::{PgArguments, PgPoolOptions, PgRow};
use sqlx::{Arguments, Column, Pool, Postgres, Row, TypeInfo};
use std::io::{Error, ErrorKind, Result};
use std::{fmt, pin::Pin};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Psql {
    #[serde(alias = "url")]
    pub endpoint: String,
    #[serde(alias = "db")]
    pub database: String,
    #[serde(alias = "col")]
    #[serde(alias = "table")]
    pub collection: String,
    #[serde(alias = "params")]
    pub parameters: Value,
    pub query: Option<String>,
    #[serde(alias = "paginator")]
    pub paginator_type: PaginatorType,
    #[serde(alias = "counter")]
    #[serde(alias = "count")]
    pub counter_type: Option<CounterType>,
    #[serde(alias = "conn")]
    pub max_connections: u32,
    #[serde(skip)]
    pub inner: Box<Vec<Value>>,
    #[serde(skip)]
    client: Option<Pool<Postgres>>,
}

impl Default for Psql {
    fn default() -> Self {
        Psql {
            endpoint: Default::default(),
            database: Default::default(),
            collection: Default::default(),
            parameters: Default::default(),
            query: Default::default(),
            paginator_type: PaginatorType::default(),
            counter_type: None,
            max_connections: 5,
            inner: Default::default(),
            client: None,
        }
    }
}

impl fmt::Display for Psql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.inner
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Psql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Psql")
            .field("endpoint", &self.endpoint)
            .field("collection", &self.collection)
            .field("database", &self.database)
            .field("parameters", &self.parameters)
            .field("query", &self.query)
            .field("paginator_type", &self.paginator_type)
            .field("counter_type", &self.counter_type)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl Psql {
    /// Transform mustache query into sanitized psql query with his arguments
    /// Query: SELECT * FROM {{ collection }} WHERE "a"={{ a }};
    /// Return: (SELECT * FROM collection WHERE "a" = $1, "a")
    fn query_sanitized(&self, query: String, parameters: Value) -> Result<(String, PgArguments)> {
        let mut map = Map::default();
        let regex = regex::Regex::new("\\{{2}([^}]*)\\}{2}")
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let mut query_sanitized = query;
        let mut query_binding: PgArguments = Default::default();
        let mut count = 1;

        map.insert("table".to_string(), Value::String(self.collection.clone()));
        map.insert(
            "collection".to_string(),
            Value::String(self.collection.clone()),
        );
        query_sanitized.replace_mustache(Value::Object(map));

        for captured in regex.captures_iter(query_sanitized.clone().as_ref()) {
            let pattern_captured = captured[0].to_string();
            let value_captured = captured[1].trim().to_string();
            let json_pointer = value_captured.to_string().to_json_pointer();

            match parameters.pointer(&json_pointer) {
                Some(Value::Null) => {
                    let replace_by_is_null = regex::Regex::new(
                        format!(r"=\s*{}", regex::escape(pattern_captured.as_str())).as_str(),
                    )
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                    let replace_by_is_not_null = regex::Regex::new(
                        format!(r"(!=|<>)\s*{}", regex::escape(pattern_captured.as_str())).as_str(),
                    )
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

                    if replace_by_is_null.is_match(query_sanitized.as_str()) {
                        query_sanitized = replace_by_is_null
                            .replace(query_sanitized.as_str(), " IS NULL")
                            .to_string();
                        continue;
                    }

                    if replace_by_is_not_null.is_match(query_sanitized.as_str()) {
                        query_sanitized = replace_by_is_not_null
                            .replace(query_sanitized.as_str(), " IS NOT NULL")
                            .to_string();
                        continue;
                    }

                    query_binding.add("NULL");
                }
                Some(Value::String(string)) => {
                    let mut is_query_binded = false;
                    if let Ok(date) = string.parse::<NaiveDate>() {
                        query_binding.add(date);
                        is_query_binded = true;
                    }
                    if let Ok(date) = string.parse::<NaiveDateTime>() {
                        query_binding.add(date);
                        is_query_binded = true;
                    }
                    if let Ok(date) = string.parse::<DateTime<Utc>>() {
                        query_binding.add(date);
                        is_query_binded = true;
                    }
                    if !is_query_binded {
                        query_binding.add(string);
                    }
                }
                Some(Value::Number(number)) => {
                    if number.is_f64() {
                        query_binding.add(number.as_f64().unwrap_or_default());
                    } else if number.is_i64() {
                        query_binding.add(number.as_i64().unwrap_or_default());
                    } else if number.is_u64() {
                        query_binding.add(number.as_u64().unwrap_or_default() as i64);
                    }
                }
                Some(Value::Bool(boolean)) => {
                    query_binding.add(boolean);
                }
                Some(Value::Array(vec)) => {
                    query_binding.add(Value::Array(vec.clone()));
                }
                Some(Value::Object(map)) => {
                    query_binding.add(Value::Object(map.clone()));
                }
                None => {
                    warn!(
                        pattern = pattern_captured.as_str(),
                        value = value_captured.as_str(),
                        path = json_pointer.as_str(),
                        parameters = format!("{:?}", parameters).as_str(),
                        "The value can't be resolved",
                    );
                    continue;
                }
            };

            query_sanitized =
                query_sanitized.replace(pattern_captured.as_str(), format!("${}", count).as_str());
            count += 1;
        }

        Ok((query_sanitized, query_binding))
    }
    /// Get the current client
    async fn client(&mut self) -> Result<&Pool<Postgres>> {
        match self.client {
            Some(_) => (),
            None => {
                trace!("Create a new DB client");
                self.client = Some(
                    PgPoolOptions::new()
                        .max_connections(self.max_connections)
                        .connect(self.path().as_str())
                        .await
                        .map_err(|e| Error::new(ErrorKind::Interrupted, e))?,
                );
            }
        }

        match &self.client {
            Some(client) => {
                trace!("Reuse the DB client");
                Ok(client)
            }
            None => Err(Error::new(ErrorKind::Interrupted, "Client is empty")),
        }
    }
}

#[async_trait]
impl Connector for Psql {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        format!("{}/{}", self.endpoint, self.database)
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
    /// See [`Connector::is_variable`] for more details.
    fn is_variable(&self) -> bool {
        match self.query.clone() {
            Some(query) => query.has_mustache(),
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
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost:5432".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.read".into();
    ///     let len = connector.len().await.unwrap();
    ///     assert!(0 < len, "The connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "psql::len")]
    async fn len(&mut self) -> Result<usize> {
        let (query_sanitized, _) = self.query_sanitized(
            "SELECT COUNT(1) FROM {{ collection }}".to_string(),
            Value::Null,
        )?;

        let len: i64 = sqlx::query_scalar(query_sanitized.as_str())
            .fetch_one(self.client().await?)
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
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "postgres://admin:admin@localhost".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.send_with_key".into();
    ///     connector.query =
    ///         Some("SELECT * FROM {{ collection }} WHERE \"number\" = {{ number }} AND \"string\" = {{ string }} AND \"boolean\" = {{ boolean }} AND \"null\" = {{ null }} AND \"array\" = {{ array }} AND \"object\" = {{ object }} AND \"date\" = {{ date }} AND \"round\" = {{ round }};".to_string());
    ///     let data: Value = serde_json::from_str(
    ///         r#"{"number":1,"group":1,"string":"value to test 5416","boolean":false,"null":null,"array":[1,2],"object":{"field":"value"},"date":"2019-12-31T00:00:00.000Z","round":10.156}"#,
    ///     )
    ///     .unwrap();
    ///     connector.set_parameters(data);
    ///     let datastream = connector.fetch(&document).await.unwrap().unwrap();
    ///     assert!(
    ///         1 == datastream.count().await,
    ///         "The datastream must contain one record"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "psql::fetch")]
    async fn fetch(&mut self, _document: &dyn Document) -> std::io::Result<Option<DataStream>> {
        let parameters = self.parameters.clone();
        let (query_sanitized, binding) = match self.query.clone() {
            Some(query) => self.query_sanitized(query, parameters.clone()),
            None => self.query_sanitized(
                "SELECT * FROM {{ collection }}".to_string(),
                parameters.clone(),
            ),
        }?;

        let data = sqlx::query_with(query_sanitized.as_str(), binding)
            .map(|row: PgRow| {
                let mut map = Map::default();

                for col in row.columns() {
                    // See mapping here [`https://github.com/launchbadge/sqlx/blob/061fdcabd72896d9bc3abb4ea4af6712a04bc0a8/sqlx-core/src/postgres/types/mod.rs`]
                    let value = match col.type_info().name() {
                        "BOOL" => match row.try_get::<bool, usize>(col.ordinal()) {
                            Ok(val) => Value::Bool(val),
                            Err(_) => Value::Null,
                        },
                        "\"CHAR\"" => match row.try_get::<i8, usize>(col.ordinal()) {
                            Ok(val) => Value::Number(serde_json::Number::from(val)),
                            Err(_) => Value::Null,
                        },
                        "SMALLINT" | "SMALLSERIAL" | "INT2" => {
                            match row.try_get::<i16, usize>(col.ordinal()) {
                                Ok(val) => Value::Number(serde_json::Number::from(val)),
                                Err(_) => Value::Null,
                            }
                        }
                        "INT" | "SERIAL" | "INT4" => match row.try_get::<i32, usize>(col.ordinal())
                        {
                            Ok(val) => Value::Number(serde_json::Number::from(val)),
                            Err(_) => Value::Null,
                        },
                        "BIGINT" | "BIGSERIAL" | "INT8" => {
                            match row.try_get::<i64, usize>(col.ordinal()) {
                                Ok(val) => Value::Number(serde_json::Number::from(val)),
                                Err(_) => Value::Null,
                            }
                        }
                        "REAL" | "FLOAT4" => match row.try_get::<f32, usize>(col.ordinal()) {
                            Ok(val) => {
                                Value::Number(serde_json::Number::from_f64(val as f64).unwrap())
                            }
                            Err(_) => Value::Null,
                        },
                        "DOUBLE PRECISION" | "FLOAT8" => match row
                            .try_get::<f64, usize>(col.ordinal())
                        {
                            Ok(val) => Value::Number(serde_json::Number::from_f64(val).unwrap()),
                            Err(_) => Value::Null,
                        },
                        "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" => {
                            match row.try_get::<String, usize>(col.ordinal()) {
                                Ok(val) => Value::String(val),
                                Err(_) => Value::Null,
                            }
                        }
                        "BYTEA" => match row.try_get::<Vec<u8>, usize>(col.ordinal()) {
                            Ok(val) => Value::String(String::from_utf8(val).unwrap()),
                            Err(_) => Value::Null,
                        },
                        "DATE" => match row.try_get::<NaiveDate, usize>(col.ordinal()) {
                            Ok(val) => Value::String(val.to_string()),
                            Err(_) => Value::Null,
                        },
                        "TIME" => match row.try_get::<NaiveTime, usize>(col.ordinal()) {
                            Ok(val) => Value::String(val.to_string()),
                            Err(_) => Value::Null,
                        },
                        "TIMESTAMPTZ" => match row.try_get::<DateTime<Utc>, usize>(col.ordinal()) {
                            Ok(val) => Value::String(val.to_string()),
                            Err(_) => Value::Null,
                        },
                        "TIMESTAMP" => match row.try_get::<NaiveDateTime, usize>(col.ordinal()) {
                            Ok(val) => Value::String(val.to_string()),
                            Err(_) => Value::Null,
                        },
                        "JSON" | "JSONB" => match row.try_get::<Value, usize>(col.ordinal()) {
                            Ok(val) => val,
                            Err(_) => Value::Null,
                        },
                        _ => match row.try_get(col.ordinal()) {
                            Ok(val) => Value::String(val),
                            Err(_) => Value::Null,
                        },
                    };
                    map.insert(col.name().to_string(), value);
                }
                Value::Object(map)
            })
            .fetch_all(self.client().await?)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!("The connector fetch data with success");

        if data.is_empty() {
            return Ok(None);
        }

        let dataset: Vec<DataResult> = data.into_iter().map(DataResult::Ok).collect();

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
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use chewdata::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "postgres://admin:admin@localhost:5432".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.send".into();
    ///     connector.erase().await.unwrap();
    ///
    ///     let expected_result1 = DataResult::Ok(
    ///         serde_json::from_str(
    ///             r#"{"number":110,"string":"value1","boolean":true,"special_char":"€"}"#,
    ///         )
    ///         .unwrap(),
    ///     );
    ///     let dataset = vec![expected_result1.clone()];
    ///     connector.send(&document, &dataset).await.unwrap();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "psql::send")]
    async fn send(
        &mut self,
        _document: &dyn Document,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let query = match self.query.clone() {
            Some(query) => query,
            None => {
                let query_start = "INSERT INTO {{ collection }}".to_string();
                let mut query_fields = "".to_string();
                let mut query_values = "".to_string();
                let value = dataset[0].to_value();

                if let Value::Object(map) = value {
                    for (field, _) in map {
                        if !query_fields.is_empty() {
                            query_fields.push_str(", ");
                            query_values.push_str(", ");
                        }
                        query_fields.push_str(format!("\"{}\"", field).as_str());
                        query_values.push_str(format!("{{{{ {} }}}}", field).as_str());
                    }
                };

                format!(
                    "{} ({}) VALUES ({});",
                    query_start, query_fields, query_values
                )
            }
        };

        for data in dataset {
            let (query_sanitized, binding) =
                self.query_sanitized(query.clone(), data.to_value())?;

            match sqlx::query_with(query_sanitized.as_str(), binding)
                .execute(self.client().await?)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    warn!(
                        error = format!("{}", e).as_str(),
                        query = query.as_str(),
                        "Can't send the data through the connector"
                    );
                    Err(Error::new(ErrorKind::Interrupted, e))
                }
            }?;
        }

        info!("The connector send data into the collection with success");

        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use chewdata::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.erase".into();
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"data":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1];
    ///     connector.send(&document, &dataset).await.unwrap();
    ///     connector.erase().await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let datastream = connector_read.fetch(&document).await.unwrap();
    ///     assert!(datastream.is_none(), "The datastream should be empty");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "psql::erase")]
    async fn erase(&mut self) -> Result<()> {
        let (query_sanitized, _) =
            self.query_sanitized("DELETE FROM {{ collection }}".to_string(), Value::Null)?;

        sqlx::query(query_sanitized.as_str())
            .execute(self.client().await?)
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
        };

        Ok(Pin::new(paginator))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum CounterType {
    #[serde(alias = "scan")]
    #[serde(skip_serializing)]
    Scan(Scan),
}

impl Default for CounterType {
    fn default() -> Self {
        CounterType::Scan(Scan::default())
    }
}

impl CounterType {
    pub async fn count(
        &self,
        connector: Psql,
        _document: Option<Box<dyn Document>>,
    ) -> Result<Option<usize>> {
        match self {
            CounterType::Scan(scan) => scan.count(connector).await,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Scan {}

impl Scan {
    /// Get the number of items from the scan
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::psql::{Psql, Scan};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///
    ///     let counter = Scan::default();
    ///     assert!(counter.count(connector).await?.is_some());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "scan_counter::count")]
    pub async fn count(&self, connector: Psql) -> Result<Option<usize>> {
        let count = connector.clone().len().await?;

        info!(count = count, "The counter count with success");
        Ok(Some(count as usize))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "offset")]
    Offset(Offset),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Offset(Offset::default())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Offset {
    pub limit: usize,
    pub skip: usize,
    pub count: Option<usize>,
    #[serde(skip)]
    pub connector: Option<Box<Psql>>,
    #[serde(skip)]
    pub has_next: bool,
}

impl Default for Offset {
    fn default() -> Self {
        Offset {
            limit: 100,
            skip: 0,
            count: None,
            connector: None,
            has_next: true,
        }
    }
}

impl Offset {
    fn set_connector(&mut self, connector: Psql) -> &mut Self
    where
        Self: Paginator + Sized,
    {
        self.connector = Some(Box::new(connector));
        self
    }
}

#[async_trait]
impl Paginator for Offset {
    /// See [`Paginator::count`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{psql::{Psql, PaginatorType, Offset, CounterType, Scan}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(Offset::default());
    ///
    ///     let mut paginator = connector.paginator().await?;
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
            counter_type = Some(CounterType::Scan(Scan::default()));
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
    /// use chewdata::connector::{psql::{Psql, PaginatorType, Offset}, Connector};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost:27017".into();
    ///     connector.database = "local".into();
    ///     connector.collection = "startup_log".into();
    ///     connector.paginator_type = PaginatorType::Offset(Offset {
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
        let limit = paginator.limit;
        let mut skip = paginator.skip;
        let query = connector
            .query
            .clone()
            .unwrap_or_else(|| "SELECT * FROM {{ collection }}".to_string());
        let count_opt = match paginator.count {
            Some(count) => Some(count),
            None => paginator.count().await?,
        };

        let stream = Box::pin(stream! {
            while has_next {
                let mut new_connector = connector.clone();

                new_connector.query = Some(format!("SELECT * from ({}) as paginator LIMIT {} OFFSET {};", query.clone(), limit, skip));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::json::Json;
    use crate::DataResult;
    use async_std::prelude::StreamExt;

    #[async_std::test]
    async fn len() {
        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost:5432".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let len = connector.len().await.unwrap();
        assert!(0 < len, "The connector should have a size upper than zero");
    }
    #[async_std::test]
    async fn fetch() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost:5432".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero"
        );
    }
    #[async_std::test]
    async fn fetch_with_parameters() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        connector.query =
            Some("SELECT * FROM {{ collection }} WHERE \"number\" = {{ number }} AND \"string\" = {{ string }} AND \"boolean\" = {{ boolean }} AND \"null\" = {{ null }} AND \"array\" = {{ array }} AND \"object\" = {{ object }} AND \"date\" = {{ date }} AND \"round\" = {{ round }}".to_string());
        let data: Value = serde_json::from_str(
            r#"{"number":1,"group":1,"string":"value to test 5416","boolean":false,"null":null,"array":[1,2],"object":{"field":"value"},"date":"2019-12-31T00:00:00.000Z","round":10.156}"#,
        )
        .unwrap();
        connector.set_parameters(data);
        let datastream = connector.fetch(&document).await.unwrap().unwrap();
        assert!(
            1 == datastream.count().await,
            "The datastream must contain one record"
        );
    }
    #[async_std::test]
    async fn erase() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.erase".into();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"data":"value1"}"#).unwrap());
        let dataset = vec![expected_result1];
        connector.send(&document, &dataset).await.unwrap();
        connector.erase().await.unwrap();

        let mut connector_read = connector.clone();
        let datastream = connector_read.fetch(&document).await.unwrap();
        assert!(datastream.is_none(), "The datastream should be empty");
    }
    #[async_std::test]
    async fn send_new_data() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost:5432".into();
        connector.database = "postgres".into();
        connector.collection = "public.send".into();
        connector.erase().await.unwrap();

        let expected_result1 = DataResult::Ok(
            serde_json::from_str(
                r#"{"number":110,"string":"value1","boolean":true,"special_char":"€"}"#,
            )
            .unwrap(),
        );
        let expected_result2 = DataResult::Ok(
            serde_json::from_str(
                r#"{"number":111,"string":"value2","boolean":false,"special_char":null}"#,
            )
            .unwrap(),
        );
        let dataset = vec![expected_result1.clone(), expected_result2.clone()];
        connector.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        assert_eq!(
            110,
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("number")
                .unwrap()
                .as_u64()
                .unwrap()
        );
        assert_eq!(
            111,
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("number")
                .unwrap()
                .as_u64()
                .unwrap()
        );
    }
    #[async_std::test]
    async fn update_existing_data() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.send_update".into();
        connector.erase().await.unwrap();

        let expected_result1 = DataResult::Ok(
            serde_json::from_str(r#"{"number":110,"group":1,"string":"value1"}"#).unwrap(),
        );
        let dataset = vec![expected_result1.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&document, &dataset).await.unwrap();

        let expected_result2 = DataResult::Ok(
            serde_json::from_str(r#"{"number":111,"group":1,"string":"value2"}"#).unwrap(),
        );
        let dataset = vec![expected_result2.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&document, &dataset).await.unwrap();

        let data: Value =
            serde_json::from_str(r#"{"number":110,"group":1,"string":"value3"}"#).unwrap();
        let dataset = vec![DataResult::Ok(data.clone())];
        let mut connector_update = connector.clone();
        connector_update.set_parameters(data);
        connector_update.query = Some("UPDATE {{ collection }} SET \"group\" = {{ group }}, \"string\" = {{ string }} WHERE \"number\" = {{ number }}".to_string());
        connector_update.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.query =
            Some("SELECT * FROM {{ collection }} ORDER BY \"number\" ASC".to_string());
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        assert_eq!(
            "value3",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("string")
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
                .get("string")
                .unwrap()
                .as_str()
                .unwrap()
        );
    }
    #[async_std::test]
    async fn upsert() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.send_with_key".into();
        connector.erase().await.unwrap();

        let expected_result1 = DataResult::Ok(
            serde_json::from_str(
                r#"{"number":110,"group":1,"string":"value1","object":{"field":"value"}}"#,
            )
            .unwrap(),
        );
        let dataset = vec![expected_result1.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&document, &dataset).await.unwrap();

        let expected_result2 = DataResult::Ok(
            serde_json::from_str(r#"{"number":111,"group":1,"string":"value2"}"#).unwrap(),
        );
        let dataset = vec![expected_result2.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&document, &dataset).await.unwrap();

        let data: Value =
            serde_json::from_str(r#"{"number":110,"group":1,"string":"value3"}"#).unwrap();
        let dataset = vec![DataResult::Ok(data.clone())];
        let mut connector_update = connector.clone();
        connector_update.set_parameters(data);
        connector_update.query = Some("INSERT INTO {{ collection }} (\"group\",\"string\",\"number\") VALUES ({{ group }},{{ string }},{{ number }}) ON CONFLICT (\"number\") DO UPDATE SET \"group\"=excluded.group,\"string\"=excluded.string".to_string());
        connector_update.send(&document, &dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.query =
            Some("SELECT * FROM {{ collection }} ORDER BY \"number\" ASC".to_string());
        let mut datastream = connector_read.fetch(&document).await.unwrap().unwrap();
        assert_eq!(
            "value3",
            datastream
                .next()
                .await
                .unwrap()
                .to_value()
                .get("string")
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
                .get("string")
                .unwrap()
                .as_str()
                .unwrap()
        );
    }
    #[async_std::test]
    async fn sql_injection() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.send_with_key".into();
        connector.query =
            Some("SELECT * FROM {{ collection }} WHERE \"number\" = {{ number }} AND \"string\" = {{ string }}".to_string());
        let data: Value =
            serde_json::from_str(r#"{"number":1,"string":"value' OR 1=1;--"}"#).unwrap();
        connector.set_parameters(data);
        let datastream = connector.fetch(&document).await.unwrap();
        assert!(datastream.is_none(), "The sql injection return no data");
    }
    #[async_std::test]
    async fn paginator_scan_counter_count() {
        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let counter = Scan::default();
        assert!(counter.count(connector).await.unwrap().is_some());
    }
    #[async_std::test]
    async fn paginator_offset_count() {
        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        connector.paginator_type = PaginatorType::Offset(Offset::default());
        let mut paginator = connector.paginator().await.unwrap();
        assert!(paginator.count().await.unwrap().is_some());
    }
    #[async_std::test]
    async fn paginator_offset_count_with_skip_and_limit() {
        let document = Json::default();

        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        connector.paginator_type = PaginatorType::Offset(Offset {
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
            "The content of this two stream is not different."
        );
    }
}
