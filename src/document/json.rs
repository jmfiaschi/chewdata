use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use async_std::io::prelude::WriteExt;
use genawaiter::sync::GenBoxed;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use async_trait::async_trait;
use async_std::io::ReadExt;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
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
    fn metadata(&self) -> Metadata {
        Json::default().metadata
    }
    /// Read complex json data.
    ///
    /// # Example: Should read the array input data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Json::default();
    /// let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    /// let connector = InMemory::new(&format!("[{}]", json_str.clone()));
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(json_str).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    /// # Example: Should read the object input data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Json::default();
    /// let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    /// let connector = InMemory::new(&format!("{}", json_str.clone()));
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(json_str).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    /// # Example: Should not read the input data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// let connector = InMemory::new(r#"My text"#);
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line = data_iter.next().unwrap();
    /// match line {
    ///     DataResult::Ok(_) => assert!(false, "The line readed by the json builder should be in error."),
    ///     DataResult::Err(_) => ()
    /// };
    /// ```
    /// # Example: Should read specific array in the records and return each data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// document.entry_path = Some("/*/array*/*".to_string());
    /// let connector = InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#);
    /// let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#).unwrap();
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let data = data_iter.next().unwrap().to_json_value();
    /// assert_eq!(expected_data, data);
    /// ```
    /// # Example: Should not found the entry path.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// document.entry_path = Some("/*/not_found/*".to_string());
    /// let connector = InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#);
    /// let expected_data: Value = serde_json::from_str(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"_error":"Entry path '/*/not_found/*' not found."}]"#).unwrap();
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let data = data_iter.next().unwrap().to_json_value();
    /// assert_eq!(expected_data, data);
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
                        }
                    }
                    (Ok(Value::Array(records)), None) => {
                        for record in records {
                            co.yield_(DataResult::Ok(record)).await;
                        }
                    }
                    (Ok(record), None) => co.yield_(DataResult::Ok(record)).await,
                    (Err(e), _) => {
                        warn!(slog_scope::logger(), "Can't deserialize the record"; "error"=>format!("{:?}",e));
                        co.yield_(DataResult::Err((Value::Null, e.into()))).await;
                    }
                };
            }
        });

        debug!(slog_scope::logger(), "Read data ended"; "documents" => format!("{:?}", self));
        Ok(data)
    }
    /// Write complex json data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    /// ```
    /// # Example: Write multi data into inner document and document init with '[]'.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// let mut connector = InMemory::new(r#"[]"#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    /// ```
    async fn write_data(
        &self,
        writer: &mut dyn Connector,
        value: Value,
    ) -> io::Result<()> {
        if writer.inner().is_empty() && writer.is_empty().await?
        {
            writer.write_all(b"[").await?;
        } else if 2 < writer.inner().len() || 2 < writer.len().await? {
            writer.write_all(b",").await?;
        }

        let mut buf = Vec::new();
        match self.is_pretty {
            true => serde_json::to_writer_pretty(&mut buf, &value),
            false => serde_json::to_writer(&mut buf, &value),
        }?;
        writer.write_all(buf.clone().as_slice()).await?;
        Ok(())
    }
    /// flush json data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    /// use std::io::Read;
    ///
    /// let mut document = Json::default();
    /// let mut connector = InMemory::new(r#"[]"#);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"}]"#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// document.flush(&mut connector).unwrap();
    /// let mut buffer = String::default();
    /// connector.read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"},{"column_1":"line_2"}]"#, buffer);
    /// ```
    async fn flush(&self, writer: &mut dyn Connector) -> io::Result<()> {
        info!(slog_scope::logger(), "flush started");
        writer.write_all(b"]").await?;
        writer.flush_into(-1).await?;
        info!(slog_scope::logger(), "flush ended");
        Ok(())
    }
}
