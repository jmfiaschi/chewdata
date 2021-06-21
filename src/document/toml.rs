use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use futures::AsyncReadExt;
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use async_trait::async_trait;

const DEFAULT_MIME: &str = "application/toml";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Toml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Toml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(DEFAULT_MIME.to_string()),
            ..Default::default()
        };
        Toml { metadata }
    }
}

#[async_trait]
impl Document for Toml {
    fn metadata(&self) -> Metadata {
        Toml::default().metadata
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read toml data.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Toml::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"[Title]
    ///     key_1 = "value_1"
    ///     key_2 = "value_2"
    ///     "#));
    ///     connector.fetch().await?;
    /// 
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap().to_json_value();
    ///     let expected_line: Value = serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#)?;
    ///     assert_eq!(expected_line, line);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Data> {
        let mut string = String::new();
        connector.read_to_string(&mut string).await?;

        let record: Value = toml::from_str(string.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let data = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            match record {
                Value::Array(records) => {
                    for record in records {
                        debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        co.yield_(DataResult::Ok(record)).await;
                    }
                }
                record => {
                    debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                    co.yield_(DataResult::Ok(record)).await;
                }
            };
            debug!(slog_scope::logger(), "End generator");
        });
        Ok(data)
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::toml::Toml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Toml::default();
    ///     let mut connector = InMemory::new(r#""#);
    /// 
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"column_1 = "line_1"
    /// "#, &format!("{}", connector));
    /// 
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"column_1 = "line_1"
    /// column_1 = "line_2"
    /// "#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        // Transform serde_json::Value to toml::Value
        let toml_value = toml::value::Value::try_from(&value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let mut toml = String::new();
        toml_value
            .serialize(&mut toml::Serializer::new(&mut toml))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Can't write the data into the connector. {}", e),
                )
            })?;
        connector.write_all(toml.as_bytes()).await
    }
}
