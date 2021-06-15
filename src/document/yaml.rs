use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use async_trait::async_trait;
use futures::AsyncReadExt;
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, io};

const DEFAULT_MIME: &str = "application/x-yaml";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Yaml {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl fmt::Display for Yaml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Yaml {{ ... }}")
    }
}

impl Default for Yaml {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(DEFAULT_MIME.to_string()),
            ..Default::default()
        };
        Yaml { metadata }
    }
}

#[async_trait]
impl Document for Yaml {
    fn metadata(&self) -> Metadata {
        Yaml::default().metadata
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read the input data.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::yaml::Yaml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Yaml::default();
    ///     let yaml_str = r#"
    /// ---
    /// number: 10
    /// string: value to test
    /// long-string: "Long val\nto test"
    /// boolean: true
    /// special_char: é
    /// date: 2019-12-31
    /// "#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("{}", yaml_str.clone())));
    ///     connector.fetch().await?;
    ///
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap().to_json_value();
    ///     let expected_line: Value = serde_yaml::from_str(yaml_str).unwrap();
    ///     assert_eq!(expected_line, line);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Data> {
        let mut string = String::new();
        connector.read_to_string(&mut string).await?;
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self), "buf"=> format!("{:?}", string));

        let documents = serde_yaml::Deserializer::from_str(string.as_str());
        let mut records = Vec::<Value>::default();
        for document in documents {
            let value = Value::deserialize(document)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            records.push(value);
        }

        let data = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            for record in records {
                debug!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                co.yield_(DataResult::Ok(record)).await;
            }
            debug!(slog_scope::logger(), "End generator");
        });

        Ok(data)
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::yaml::Yaml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Yaml::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"---
    /// column_1: line_1
    /// "#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"---
    /// column_1: line_1
    /// ---
    /// column_1: line_2
    /// "#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        let mut buf: io::Cursor<Vec<_>> = io::Cursor::default();

        serde_yaml::to_writer(&mut buf, &value).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't write the data into the connector. {}", e),
            )
        })?;
        connector.write_all(buf.into_inner().as_slice()).await
    }
    /// See [`Document::flush`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::yaml::Yaml;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use std::io::Read;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Yaml::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     document.flush(&mut connector).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"---
    /// column_1: line_1
    /// "#, buffer);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     document.flush(&mut connector).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"---
    /// column_1: line_1
    /// ---
    /// column_1: line_2
    /// "#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn flush(&self, connector: &mut dyn Connector) -> io::Result<()> {
        let size = connector.len().await? as i64;
        connector.flush_into(size).await
    }
}
