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
use std::{fmt, io};

const DEFAULT_SUBTYPE: &str = "x-yaml";

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
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Yaml { metadata }
    }
}

#[async_trait]
impl Document for Yaml {
    fn metadata(&self) -> Metadata {
        Yaml::default().metadata.merge(self.metadata.clone())
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
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_json_value();
    ///     let expected_data: Value = serde_yaml::from_str(yaml_str).unwrap();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        info!("Start");

        let mut string = String::new();
        connector.read_to_string(&mut string).await?;

        let documents = serde_yaml::Deserializer::from_str(string.as_str());
        let mut records = Vec::<Value>::default();
        for document in documents {
            let value = Value::deserialize(document)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            records.push(value);
        }

        Ok(Box::pin(stream! {
            for record in records {
                yield DataResult::Ok(record);
            }
        }))
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
    #[instrument]
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        trace!("Start");
        
        let mut buf: io::Cursor<Vec<_>> = io::Cursor::default();

        serde_yaml::to_writer(&mut buf, &value).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't write the data into the connector. {}", e),
            )
        })?;
        connector.write_all(buf.into_inner().as_slice()).await
    }
}
