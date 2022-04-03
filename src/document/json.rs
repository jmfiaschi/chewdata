use crate::connector::Connector;
use crate::document::Document;
use crate::Metadata;
use crate::{DataResult, Dataset};
use async_std::io::prelude::WriteExt;
use async_std::io::ReadExt;
use async_stream::stream;
use async_trait::async_trait;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Json {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub is_pretty: bool,
    pub entry_path: Option<String>,
}

impl Default for Json {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(mime::JSON.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Json {
            metadata,
            is_pretty: false,
            entry_path: None,
        }
    }
}

#[async_trait]
impl Document for Json {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Json::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = Some(entry_path);
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read the array input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("[{}]", json_str.clone())));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(json_str)?;
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read the object input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("{}", json_str.clone())));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(json_str)?;
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not read the input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use chewdata::DataResult;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"My text"#));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap();
    ///     match data {
    ///         DataResult::Ok(_) => assert!(false, "The data readed by the json builder should be in error."),
    ///         DataResult::Err(_) => ()
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read specific array in the records and return each data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     document.entry_path = Some("/*/array*/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#)?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not found the entry path.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     document.entry_path = Some("/*/not_found/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"_error":"Entry path '/*/not_found/*' not found."}]"#)?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut buf = Vec::new();
        connector.read_to_end(&mut buf).await?;

        let cursor = io::Cursor::new(buf);

        let deserializer = serde_json::Deserializer::from_reader(cursor);
        let iterator = deserializer.into_iter::<Value>();
        let entry_path_option = self.entry_path.clone();

        Ok(Box::pin(stream! {
            for record_result in iterator {
                match (record_result, entry_path_option.clone()) {
                    (Ok(record), Some(entry_path)) => {
                        match record.clone().search(entry_path.as_ref()) {
                            Ok(Some(Value::Array(values))) => {
                                for value in values {
                                    yield DataResult::Ok(value);
                                }
                            }
                            Ok(Some(record)) => yield DataResult::Ok(record),
                            Ok(None) => {
                                yield DataResult::Err((
                                    record,
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Entry path '{}' not found.", entry_path),
                                    ),
                                ))
                            }
                            Err(e) => yield DataResult::Err((record, e)),
                        }
                    }
                    (Ok(Value::Array(records)), None) => {
                        for record in records {
                            yield DataResult::Ok(record);
                        }
                    }
                    (Ok(record), None) => yield DataResult::Ok(record),
                    (Err(e), _) => {
                        warn!(error = format!("{:?}",e).as_str(),  "Can't deserialize the record");
                        yield DataResult::Err((Value::Null, e.into()));
                    }
                };
            }
        }))
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn write_data(&mut self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        if !connector.inner().is_empty() {
            connector.write_all(b",").await?;
        }

        let mut buf = Vec::new();

        match self.is_pretty {
            true => serde_json::to_writer_pretty(&mut buf, &value),
            false => serde_json::to_writer(&mut buf, &value),
        }?;

        connector.write_all(buf.as_slice()).await
    }
    /// See [`Document::close`] for more details.
    ///
    /// # Example: Remote document don't have data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"[{"column_1":"line_1"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has empty data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"[{"column_1":"line_1"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[{"column_1":"line_1"}]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#",{"column_1":"line_2"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn close(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        let remote_len = connector.len().await?;
        let buff = connector.inner().clone();

        connector.clear();

        let header = self.header(connector).await?;
        let footer = self.footer(connector).await?;

        if remote_len == 0
            || remote_len == header.len() + footer.len()
        {
            connector.write_all(&header).await?;
            connector.write_all(&buff).await?;
            connector.write_all(&footer).await?;
        }

        if remote_len > header.len() + footer.len() {
            connector.write_all(",".as_bytes()).await?;
            connector.write_all(&buff).await?;
            connector.write_all(&footer).await?;
        }

        Ok(())
    }
    /// See [`Document::header`] for more details.
    async fn header(&self, _connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        Ok("[".as_bytes().to_vec())
    }
    /// See [`Document::footer`] for more details.
    async fn footer(&self, _connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        Ok("]".as_bytes().to_vec())
    }
    /// See [`Document::has_data`] for more details.
    fn has_data(&self, str: &str) -> io::Result<bool> {
        Ok(!matches!(str, "[]" | ""))
    }
}
