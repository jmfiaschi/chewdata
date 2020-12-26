use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use genawaiter::sync::GenBoxed;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

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
            mime_type: Some(mime::APPLICATION_JSON.to_string()),
            ..Default::default()
        };
        Json {
            metadata,
            is_pretty: false,
            entry_path: None,
        }
    }
}

impl Document for Json {
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
    fn read_data(&self, connector: Box<dyn Connector>) -> io::Result<Data> {
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut connector = connector;
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::APPLICATION_JSON.to_string());
        connector.set_metadata(metadata.clone());

        let deserializer = serde_json::Deserializer::from_reader(connector);
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
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    /// ```
    /// # Example: Write multi data into truncate inner document and document init with '[]'.
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
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Json::default();
    /// let mut connector = InMemory::new(r#"[{"column_1":"line_1"}]"#);
    /// connector.can_truncate = true;
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_2"}"#, &format!("{}", connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// document.write_data_result(&mut connector, DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"[{"column_1":"line_2"},{"column_1":"line_3"}"#, &format!("{}", connector));
    /// ```
    fn write_data_result(
        &mut self,
        connector: &mut dyn Connector,
        data_result: DataResult,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();

        connector.set_parameters(value.clone());

        if connector.is_empty()? && connector.inner().is_empty()
            || connector.will_be_truncated() && connector.inner().is_empty()
        {
            connector.write_all(b"[")?;
        } else if 2 < connector.inner().len() || 2 < connector.len()? {
            connector.write_all(b",")?;
        }

        match self.is_pretty {
            true => serde_json::to_writer_pretty(connector, &value),
            false => serde_json::to_writer(connector, &value),
        }?;

        debug!(slog_scope::logger(), "Write data ended."; "data" => format!("{:?}", data_result));
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
    fn flush(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        debug!(slog_scope::logger(), "Flush called.");
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::APPLICATION_JSON.to_string());
        connector.set_metadata(metadata.clone());
        connector.write_all(b"]")?;
        connector.seek_and_flush(-1)?;
        debug!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
}
