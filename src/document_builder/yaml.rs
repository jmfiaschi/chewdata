use crate::connector::Connector;
use crate::document_builder::Build;
use crate::processor::{Data, DataResult};
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::prelude::*;
use std::{fmt, io};

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(default)]
pub struct Yaml {
    pub connector: Connector,
}

impl fmt::Display for Yaml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Yaml {{ ... }}")
    }
}

impl Default for Yaml {
    fn default() -> Self {
        Yaml {
            connector: Connector::default(),
        }
    }
}

impl PartialEq for Yaml {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}

impl Build for Yaml {
    /// Read complex yaml data.
    ///
    /// # Example: Should read the input data.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::yaml::Yaml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    ///
    /// let mut yaml = Yaml::default();
    /// let yaml_str = r#"
    /// ---
    /// number: 10
    /// string: value to test
    /// long-string: "Long val\nto test"
    /// boolean: true
    /// special_char: é
    /// date: 2019-12-31
    /// "#;
    /// yaml.connector = Connector::Text(Text::new(&format!("{}", yaml_str.clone())));
    ///
    /// let mut data_iter = yaml.read_data().unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// let expected_line: Value = serde_yaml::from_str(yaml_str).unwrap();
    /// assert_eq!(expected_line, line);
    /// ```
    fn read_data(&self) -> io::Result<Data> {
        trace!(slog_scope::logger(), "Read data"; "documents" => format!("{}", self));
        let record: Value = serde_yaml::from_reader(self.connector.clone().reader())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator");
            match record {
                Value::Array(records) => {
                    for record in records {
                        trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                        co.yield_(DataResult::Ok(record)).await;
                    }
                }
                record => {
                    trace!(slog_scope::logger(), "Record deserialized"; "record" => format!("{:?}",record));
                    co.yield_(DataResult::Ok(record)).await;
                }
            };
            trace!(slog_scope::logger(), "End generator");
        });
        trace!(slog_scope::logger(), "Read data ended");
        Ok(data)
    }
    /// Write complex yaml data.
    ///
    /// # Example: Write multi data into empty inner document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::yaml::Yaml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut yaml = Yaml::default();
    /// yaml.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_1
    /// "#, &format!("{}", yaml.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_1
    /// ---
    /// column_1: line_2
    /// "#, &format!("{}", yaml.connector));
    /// ```
    /// # Example: Truncate and write into the document.
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::yaml::Yaml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut yaml = Yaml::default();
    /// let mut text = Text::new(r#"---
    /// column_1: line_1
    /// "#);
    /// text.truncate = true;
    /// yaml.connector = Connector::Text(text);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_2
    /// "#, &format!("{}", yaml.connector));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_3"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_2
    /// ---
    /// column_1: line_3
    /// "#, &format!("{}", yaml.connector));
    /// ```
    fn write_data_result(&mut self, data_result: DataResult) -> io::Result<()> {
        trace!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();

        self.connector.get_mut().set_path_parameters(value.clone());

        serde_yaml::to_writer(&mut self.connector.get_mut(), &value).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't write the data into the connector. {}", e),
            )
        })?;
        self.connector.get_mut().write("\n".as_bytes())?;

        trace!(slog_scope::logger(), "Write data ended.");
        Ok(())
    }
    /// flush data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector,text::Text};
    /// use chewdata::document_builder::yaml::Yaml;
    /// use chewdata::document_builder::Build;
    /// use serde_json::Value;
    /// use chewdata::processor::DataResult;
    ///
    /// let mut yaml = Yaml::default();
    /// yaml.connector = Connector::Text(Text::new(r#""#));
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// yaml.flush().unwrap();
    /// let mut buffer = String::default();
    /// yaml.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_1
    /// "#, buffer);
    ///
    /// let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap();
    /// yaml.write_data_result(DataResult::Ok(value)).unwrap();
    /// yaml.flush().unwrap();
    /// let mut buffer = String::default();
    /// yaml.connector.reader().read_to_string(&mut buffer).unwrap();
    /// assert_eq!(r#"---
    /// column_1: line_1
    /// ---
    /// column_1: line_2
    /// "#, buffer);
    /// ```
    fn flush(&mut self) -> io::Result<()> {
        trace!(slog_scope::logger(), "Flush called.");
        self.connector
            .get_mut()
            .set_mime_type("application/x-yaml".parse::<mime::Mime>().unwrap());
        self.connector.get_mut().flush()?;
        trace!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
    fn connector(&self) -> &Connector {
        &self.connector
    }
}
