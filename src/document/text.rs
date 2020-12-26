extern crate csv;

use crate::connector::Connector;
use crate::document::Document;
use crate::step::{Data, DataResult};
use crate::Metadata;
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default)]
pub struct Text {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
}

impl Default for Text {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::TEXT_PLAIN_UTF_8.to_string()),
            ..Default::default()
        };
        Text { metadata }
    }
}

impl Document for Text {
    /// Read complex csv data.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    ///
    /// let mut document = Text::default();
    /// let connector = InMemory::new(r#"My text1 \n My text 2"#);
    ///
    /// let mut data_iter = document.read_data(Box::new(connector)).unwrap().into_iter();
    /// let line = data_iter.next().unwrap().to_json_value();
    /// assert_eq!(r#"My text1 \n My text 2"#, line);
    /// ```
    fn read_data(&self, connector: Box<dyn Connector>) -> io::Result<Data> {
        debug!(slog_scope::logger(), "Read data"; "documents" => format!("{:?}", self));
        let mut text = String::default();
        let mut connector = connector;

        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_PLAIN_UTF_8.to_string());
        connector.set_metadata(metadata.clone());
        connector.read_to_string(&mut text)?;

        let data = GenBoxed::new_boxed(|co| async move {
            co.yield_(DataResult::Ok(Value::String(text))).await;
        });

        debug!(slog_scope::logger(), "Read data ended"; "documents" => format!("{:?}", self));
        Ok(data)
    }
    /// Write complex csv data.
    ///
    /// # Example: Add header if connector data empty or if the connector will truncate the previous data.
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Text::default();
    /// let mut connector = InMemory::new(r#""#);
    ///
    /// document.write_data_result(&mut connector, DataResult::Ok(Value::String("My text".to_string()))).unwrap();
    /// assert_eq!(r#"My text"#, &format!("{}", connector));
    /// ```
    fn write_data_result(
        &mut self,
        connector: &mut dyn Connector,
        data_result: DataResult,
    ) -> io::Result<()> {
        debug!(slog_scope::logger(), "Write data"; "data" => format!("{:?}", data_result));
        let value = data_result.to_json_value();
        connector.write_all(value.as_str().unwrap_or("").as_bytes())?;

        debug!(slog_scope::logger(), "Write data ended."; "data" => format!("{:?}", data_result));
        Ok(())
    }
    /// Push data from the inner buffer into the document and flush the connector.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use chewdata::step::DataResult;
    ///
    /// let mut document = Text::default();
    /// let mut connector = InMemory::new(r#"My Text"#);
    ///
    /// document.flush(&mut connector).unwrap();
    /// assert_eq!(r#""#, &format!("{}", connector));
    /// ```
    fn flush(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        debug!(slog_scope::logger(), "Flush called.");
        let mut metadata = self.metadata.clone();
        metadata.mime_type = Some(mime::TEXT_PLAIN_UTF_8.to_string());
        connector.set_metadata(metadata.clone());
        connector.flush()?;
        debug!(slog_scope::logger(), "Flush with success.");
        Ok(())
    }
}
