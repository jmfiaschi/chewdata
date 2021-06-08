use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use async_std::io::{prelude::WriteExt, ReadExt};
use async_trait::async_trait;
use genawaiter::sync::GenBoxed;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

const DEFAULT_MIME_TYPE: &str = "x-ndjson";

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Jsonl {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub entry_path: Option<String>,
}

impl Default for Jsonl {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_MIME_TYPE.to_string()),
            ..Default::default()
        };
        Jsonl {
            metadata,
            is_pretty: false,
            entry_path: None,
        }
    }
}

#[async_trait]
impl Document for Jsonl {
    fn metadata(&self) -> Metadata {
        Jsonl::default().metadata
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read the input data.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
    ///     let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("{}", json_str.clone())));
    ///     connector.fetch().await?;
    /// 
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap().to_json_value();
    ///     let expected_line: Value = serde_json::from_str(json_str)?;
    ///     assert_eq!(expected_line, line);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not read the input data.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new("My text"));
    ///     connector.fetch().await?;
    /// 
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap();
    ///     match line {
    ///         DataResult::Ok(_) => assert!(false, "The line readed by the json builder should be in error."),
    ///         DataResult::Err(_) => ()
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read specific array in the records and return each data.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
    ///     document.entry_path = Some("/array*/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"{"array1":[{"field":"value1"},{"field":"value2"}]}
    ///     {"array1":[{"field":"value3"},{"field":"value4"}]}"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#)?;
    /// 
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let data = data_iter.next().unwrap().to_json_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not found the entry path.
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
    ///     document.entry_path = Some("/not_found/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"{"array1":[{"field":"value1"},{"field":"value2"}]}"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"{"array1":[{"field":"value1"},{"field":"value2"}],"_error":"Entry path '/not_found/*' not found."}"#)?;
    /// 
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let data = data_iter.next().unwrap().to_json_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Data> {
        let mut buf = Vec::new();
        connector.read_to_end(&mut buf).await?;
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self), "buf"=> format!("{:?}", String::from_utf8(buf.clone())));

        let cursor = io::Cursor::new(buf);

        let deserializer = serde_json::Deserializer::from_reader(cursor);
        let iterator = deserializer.into_iter::<Value>();
        let entry_path_option = self.entry_path.clone();

        let data = GenBoxed::new_boxed(|co| async move {
            debug!(slog_scope::logger(), "Start generator");
            for record_result in iterator {
                match (record_result, entry_path_option.clone()) {
                    (Ok(record), Some(entry_path)) => {
                        match record.clone().search(entry_path.as_ref()) {
                            Ok(Some(Value::Array(values))) => {
                                for value in values {
                                    co.yield_(DataResult::Ok(value)).await;
                                }
                            }
                            Ok(Some(record)) => co.yield_(DataResult::Ok(record)).await,
                            Ok(None) => {
                                co.yield_(DataResult::Err((
                                    record,
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Entry path '{}' not found.", entry_path),
                                    ),
                                )))
                                .await
                            }
                            Err(e) => co.yield_(DataResult::Err((record, e))).await,
                        };
                    }
                    (Ok(record), None) => co.yield_(DataResult::Ok(record)).await,
                    (Err(e), _) => {
                        warn!(slog_scope::logger(), "Can't deserialize the record"; "error"=>format!("{:?}",e));
                        co.yield_(DataResult::Err((Value::Null, e.into()))).await;
                    }
                };
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
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
    ///     let mut connector = InMemory::new(r#""#);
    /// 
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"}
    /// "#, &format!("{}", connector));
    /// 
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"}
    /// {"column_1":"line_2"}
    /// "#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        let mut buf = Vec::new();
        match self.is_pretty {
            true => serde_json::to_writer_pretty(&mut buf, &value),
            false => serde_json::to_writer(&mut buf, &value),
        }?;
        connector.write_all(buf.clone().as_slice()).await?;
        connector.write_all(b"\n").await
    }
    /// See [`Document::flush`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::jsonl::Jsonl;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Jsonl::default();
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
    ///     assert_eq!(r#"{"column_1":"line_1"}
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
    ///     assert_eq!(r#"{"column_1":"line_1"}
    /// {"column_1":"line_2"}
    /// "#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn flush(&self, connector: &mut dyn Connector) -> io::Result<()> {
        connector.flush_into(connector.len().await? as i64).await
    }
}
