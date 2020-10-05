use crate::connector::Connector;
use crate::document_builder::Build;
use crate::processor::{Data, DataResult};
use genawaiter::sync::GenBoxed;
use json_value_search::Search;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(default)]
pub struct Jsonl {
    pub connector: Connector,
    pub pretty: bool,
    pub entry_path: Option<String>,
}

impl PartialEq for Jsonl {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}

impl Default for Jsonl {
    fn default() -> Self {
        Jsonl {
            pretty: false,
            connector: Connector::default(),
            entry_path: None,
        }
    }
}

impl Build for Jsonl {
    /// Read complex json data.
    ///
    /// # Example: Should read the input data.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut db = Jsonl::default();
    /// let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    /// db.connector = Connector::Text(Text::new(&format!("{}", json_str.clone())));
    ///
    /// let mut data_iter = db.read_data().unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_json::from_str(json_str).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    /// # Example: Should not read the input data.
    /// ```
    /// use chewdata::connector::{Connector, text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut db = Jsonl::default();
    /// db.connector = Connector::Text(Text::new("My text"));
    ///
    /// let mut data_iter = db.read_data().unwrap().into_iter();
    /// let line = data_iter.next().unwrap();
    /// match line {
    ///     DataResult::Ok(_) => assert!(false, "The line readed by the json builder should be in error."),
    ///     DataResult::Err(_) => ()
    /// };
    /// ```
    /// # Example: Should read specific array in the records and return each data.
    /// ```
    /// use chewdata::connector::{Connector, text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut db = Jsonl::default();
    /// db.entry_path = Some("/array*/*".to_string());
    /// db.connector = Connector::Text(Text::new(r#"{"array1":[{"field":"value1"},{"field":"value2"}]}
    /// {"array1":[{"field":"value3"},{"field":"value4"}]}"#));
    /// let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#).unwrap();
    ///
    /// let mut data_iter = db.read_data().unwrap().into_iter();
    /// let data = data_iter.next().unwrap().to_json_value();
    /// assert_eq!(expected_data, data);
    /// ```
    /// # Example: Should not found the entry path.
    /// ```
    /// use chewdata::connector::{Connector, text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut db = Jsonl::default();
    /// db.entry_path = Some("/not_found/*".to_string());
    /// db.connector = Connector::Text(Text::new(r#"{"array1":[{"field":"value1"},{"field":"value2"}]}"#));
    /// let expected_data: Value = serde_json::from_str(r#"{"array1":[{"field":"value1"},{"field":"value2"}],"_error":"Entry path '/not_found/*' not found."}"#).unwrap();
    ///
    /// let mut data_iter = db.read_data().unwrap().into_iter();
    /// let data = data_iter.next().unwrap().to_json_value();
    /// assert_eq!(expected_data, data);
    /// ```
    fn read_data(&self) -> io::Result<Data> {
        trace!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let deserializer = serde_json::Deserializer::from_reader(self.connector.clone().inner());
        let iterator = deserializer.into_iter::<Value>();
        let entry_path_option = self.entry_path.clone();
        let data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator");
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
            trace!(slog_scope::logger(), "End generator");
        });

        trace!(slog_scope::logger(), "Read data ended");
        Ok(data)
    }
    /// Write complex jsonl data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut jsonl = Jsonl::default();
    /// jsonl.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#, &format!("{}", jsonl.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}{"column_1":"line_2"}"#, &format!("{}", jsonl.connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut jsonl = Jsonl::default();
    /// let mut text = Text::new(r#"{"column_1":"line_1"}"#);
    /// text.truncate = true;
    /// jsonl.connector = Connector::Text(text);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_2"}"#, &format!("{}", jsonl.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"{"column_1":"line_2"}{"column_1":"line_3"}"#, &format!("{}", jsonl.connector));
    /// ```
    fn write_data_result(&mut self, data_result: DataResult) -> io::Result<()> {
        trace!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        let pretty = self.pretty;

        self.connector
            .get_mut()
            .set_mime_type(mime::APPLICATION_JSON);
        self.connector.get_mut().set_path_parameters(value.clone());

        match pretty {
            true => serde_json::to_writer_pretty(self.connector.writer(), &value),
            false => serde_json::to_writer(self.connector.writer(), &value),
        }?;

        trace!(slog_scope::logger(), "Write data ended.");
        Ok(())
    }
    /// flush jsonl data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::jsonl::Jsonl;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut jsonl = Jsonl::default();
    /// jsonl.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// jsonl.flush().unwrap();
    /// let mut buffer = String::default();
    /// jsonl.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}"#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// jsonl.write_data_result(DataResult::Ok(value)).unwrap();
    /// jsonl.flush().unwrap();
    /// let mut buffer = String::default();
    /// jsonl.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"{"column_1":"line_1"}{"column_1":"line_2"}"#, buffer);
    /// ```
    fn flush(&mut self) -> io::Result<()> {
        trace!(slog_scope::logger(), "Flush called.");
        self.connector.writer().flush()?;
        trace!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
    fn connector(&self) -> &Connector {
        &self.connector
    }
}
