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
//!         "concurrency_limit":3
//!     }
//! ]
//! ```
use super::counter::psql::CounterType;
use super::paginator::psql::PaginatorType;
use super::Connector;
use crate::helper::json_pointer::JsonPointer;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{helper::mustache::Mustache, DataResult};
use crate::{DataSet, DataStream};
use async_lock::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::postgres::{PgArguments, PgPoolOptions, PgRow};
use sqlx::{Arguments, Column, Pool, Postgres, Row, TypeInfo};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

type SharedClients = Arc<Mutex<HashMap<String, Pool<Postgres>>>>;
static CLIENTS: OnceLock<SharedClients> = OnceLock::new();

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
    pub counter_type: CounterType,
    #[serde(alias = "conn")]
    pub max_connections: u32,
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
            counter_type: CounterType::default(),
            max_connections: 5,
        }
    }
}

impl fmt::Debug for Psql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Psql")
            // Can contain sensitive data
            .field("endpoint", &self.endpoint.display_only_for_debugging())
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("parameters", &self.parameters.display_only_for_debugging())
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
    pub fn query_sanitized(
        &self,
        query: &str,
        parameters: &Value,
    ) -> Result<(String, PgArguments)> {
        let mut map = Map::default();
        let regex = regex::Regex::new("\\{{2}([^}]*)\\}{2}")
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        let mut query_sanitized = query.to_owned();
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

                    query_binding
                        .add("NULL")
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                }
                Some(Value::String(string)) => {
                    let mut is_query_binded = false;
                    if let Ok(date) = string.parse::<NaiveDate>() {
                        query_binding
                            .add(date)
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                        is_query_binded = true;
                    }
                    if let Ok(date) = string.parse::<NaiveDateTime>() {
                        query_binding
                            .add(date)
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                        is_query_binded = true;
                    }
                    if let Ok(date) = string.parse::<DateTime<Utc>>() {
                        query_binding
                            .add(date)
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                        is_query_binded = true;
                    }
                    if !is_query_binded {
                        query_binding
                            .add(string)
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                    }
                }
                Some(Value::Number(number)) => {
                    if number.is_f64() {
                        query_binding
                            .add(number.as_f64().unwrap_or_default())
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                    } else if number.is_i64() {
                        query_binding
                            .add(number.as_i64().unwrap_or_default())
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                    } else if number.is_u64() {
                        query_binding
                            .add(number.as_u64().unwrap_or_default() as i64)
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                    }
                }
                Some(Value::Bool(boolean)) => {
                    query_binding
                        .add(boolean)
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                }
                Some(Value::Array(vec)) => {
                    query_binding
                        .add(Value::Array(vec.clone()))
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                }
                Some(Value::Object(map)) => {
                    query_binding
                        .add(Value::Object(map.clone()))
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
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
    pub async fn client(&self) -> Result<Pool<Postgres>> {
        let clients = CLIENTS.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let client_key = self.client_key();
        if let Some(client) = clients.lock().await.get(&self.client_key()) {
            trace!(client_key, "Retrieve the previous client");
            return Ok(client.clone());
        }

        trace!(client_key, "Create a new client");
        let mut map = clients.lock_arc().await;
        let client = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect(self.path().as_str())
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;
        map.insert(client_key, client.clone());

        Ok(client)
    }
    fn client_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let client_key = self.path().to_string();
        client_key.hash(&mut hasher);
        hasher.finish().to_string()
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
        match &self.query {
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
    /// ```
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
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
    async fn len(&self) -> Result<usize> {
        match self.counter_type.count(self).await {
            Ok(count) => Ok(count),
            Err(e) => {
                warn!(
                    error = e.to_string(),
                    "Can't count the number of element, return 0"
                );

                Ok(0)
            }
        }
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    /// use smol::prelude::*;
    /// use std::io;
    /// use smol::stream::StreamExt;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "postgres://admin:admin@localhost".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.read".into();
    ///     connector.query =
    ///         Some("SELECT * FROM {{ collection }} WHERE \"number\" = {{ number }} AND \"string\" = {{ string }} AND \"boolean\" = {{ boolean }} AND \"null\" = {{ null }} AND \"array\" = {{ array }} AND \"object\" = {{ object }} AND \"date\" = {{ date }} AND \"round\" = {{ round }};".to_string());
    ///     let data: Value = serde_json::from_str(
    ///         r#"{"number":1,"group":1,"string":"value to test 5416","boolean":false,"null":null,"array":[1,2],"object":{"field":"value"},"date":"2019-12-31T00:00:00.000Z","round":10.156}"#,
    ///     )
    ///     .unwrap();
    ///     connector.set_parameters(data);
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         1 == datastream.count().await,
    ///         "The datastream must contain one record"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "psql::fetch")]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let (query_sanitized, binding) = match &self.query {
            Some(query) => self.query_sanitized(query, &self.parameters),
            None => self.query_sanitized("SELECT * FROM {{ collection }}", &self.parameters),
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
            .fetch_all(&self.client().await?)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!("Fetch data with success");

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
    /// ```
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
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
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "psql::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let query = match &self.query {
            Some(query) => query.clone(),
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
            let (query_sanitized, binding) = self.query_sanitized(&query, &data.to_value())?;

            match sqlx::query_with(query_sanitized.as_str(), binding)
                .execute(&self.client().await?)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    warn!(
                        error = format!("{}", e).as_str(),
                        query = query.as_str(),
                        "Can't send data"
                    );
                    Err(Error::new(ErrorKind::Interrupted, e))
                }
            }?;
        }

        info!("Send data with success");

        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::psql::Psql;
    /// use chewdata::document::json::Json;
    /// use chewdata::connector::Connector;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Psql::default();
    ///     connector.endpoint = "psql://admin:admin@localhost".into();
    ///     connector.database = "postgres".into();
    ///     connector.collection = "public.erase".into();
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"data":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1];
    ///     connector.send(&dataset).await.unwrap();
    ///     connector.erase().await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let datastream = connector_read.fetch().await.unwrap();
    ///     assert!(datastream.is_none(), "The datastream should be empty");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "psql::erase")]
    async fn erase(&mut self) -> Result<()> {
        let (query_sanitized, _) =
            self.query_sanitized("DELETE FROM {{ collection }}", &Value::Null)?;

        sqlx::query(query_sanitized.as_str())
            .execute(&self.client().await?)
            .await
            .map_err(|e| Error::new(ErrorKind::Interrupted, e))?;

        info!("Erase data with success");
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
    use crate::DataResult;
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

    #[apply(test!)]
    async fn len() {
        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost:5432".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let len = connector.len().await.unwrap();
        assert!(0 < len, "The connector should have a size upper than zero.");
    }
    #[apply(test!)]
    async fn fetch() {
        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost:5432".into();
        connector.database = "postgres".into();
        connector.collection = "public.read".into();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn fetch_with_parameters() {
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
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            1 == datastream.count().await,
            "The datastream must contain one record."
        );
    }
    #[apply(test!)]
    async fn erase() {
        let mut connector = Psql::default();
        connector.endpoint = "psql://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.erase".into();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"data":"value1"}"#).unwrap());
        let dataset = vec![expected_result1];
        connector.send(&dataset).await.unwrap();
        connector.erase().await.unwrap();

        let mut connector_read = connector.clone();
        let datastream = connector_read.fetch().await.unwrap();
        assert!(datastream.is_none(), "The datastream should be empty.");
    }
    #[apply(test!)]
    async fn send_new_data() {
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
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
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
    #[apply(test!)]
    async fn update_existing_data() {
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
        connector_update.send(&dataset).await.unwrap();

        let expected_result2 = DataResult::Ok(
            serde_json::from_str(r#"{"number":111,"group":1,"string":"value2"}"#).unwrap(),
        );
        let dataset = vec![expected_result2.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&dataset).await.unwrap();

        let data: Value =
            serde_json::from_str(r#"{"number":110,"group":1,"string":"value3"}"#).unwrap();
        let dataset = vec![DataResult::Ok(data.clone())];
        let mut connector_update = connector.clone();
        connector_update.set_parameters(data);
        connector_update.query = Some("UPDATE {{ collection }} SET \"group\" = {{ group }}, \"string\" = {{ string }} WHERE \"number\" = {{ number }}".to_string());
        connector_update.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.query =
            Some("SELECT * FROM {{ collection }} ORDER BY \"number\" ASC".to_string());
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
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
    #[apply(test!)]
    async fn upsert() {
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
        connector_update.send(&dataset).await.unwrap();

        let expected_result2 = DataResult::Ok(
            serde_json::from_str(r#"{"number":111,"group":1,"string":"value2"}"#).unwrap(),
        );
        let dataset = vec![expected_result2.clone()];
        let mut connector_update = connector.clone();
        connector_update.send(&dataset).await.unwrap();

        let data: Value =
            serde_json::from_str(r#"{"number":110,"group":1,"string":"value3"}"#).unwrap();
        let dataset = vec![DataResult::Ok(data.clone())];
        let mut connector_update = connector.clone();
        connector_update.set_parameters(data);
        connector_update.query = Some("INSERT INTO {{ collection }} (\"group\",\"string\",\"number\") VALUES ({{ group }},{{ string }},{{ number }}) ON CONFLICT (\"number\") DO UPDATE SET \"group\"=excluded.group,\"string\"=excluded.string".to_string());
        connector_update.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        connector_read.query =
            Some("SELECT * FROM {{ collection }} ORDER BY \"number\" ASC".to_string());
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
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
    #[apply(test!)]
    async fn sql_injection() {
        let mut connector = Psql::default();
        connector.endpoint = "postgres://admin:admin@localhost".into();
        connector.database = "postgres".into();
        connector.collection = "public.send_with_key".into();
        connector.query =
            Some("SELECT * FROM {{ collection }} WHERE \"number\" = {{ number }} AND \"string\" = {{ string }}".to_string());
        let data: Value =
            serde_json::from_str(r#"{"number":1,"string":"value' OR 1=1;--"}"#).unwrap();
        connector.set_parameters(data);
        let datastream = connector.fetch().await.unwrap();
        assert!(datastream.is_none(), "The sql injection return no data.");
    }
}
