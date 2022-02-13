use crate::connector::Connector;
use crate::document::Document;
use crate::{Dataset, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use async_stream::stream;
use async_trait::async_trait;
use futures::AsyncReadExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_SUBTYPE: &str = "toml";

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
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Toml { metadata }
    }
}

#[async_trait]
impl Document for Toml {
    fn metadata(&self) -> Metadata {
        Toml::default().metadata.merge(self.metadata.clone())
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
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(r#"{"Title":{"key_1":"value_1","key_2":"value_2"}}"#)?;
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut string = String::new();
        connector.read_to_string(&mut string).await?;

        let record: Value = toml::from_str(string.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Box::pin(stream! {
            match record {
                Value::Array(records) => {
                    for record in records {
                        trace!(record = format!("{:?}",record).as_str(),  "Record deserialized");
                        yield DataResult::Ok(record);
                    }
                }
                record => {
                    trace!(record = format!("{:?}",record).as_str(),  "Record deserialized");
                    yield DataResult::Ok(record);
                }
            };
        }))
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
    #[instrument]
    async fn write_data(&mut self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
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
