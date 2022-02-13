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
            mime_type: Some(mime::TEXT.to_string()),
            mime_subtype: Some(mime::PLAIN.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Text { metadata }
    }
}

#[async_trait]
impl Document for Text {
    fn metadata(&self) -> Metadata {
        Text::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Text::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"My text1 \n My text 2"#));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     assert_eq!(r#"My text1 \n My text 2"#, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut text = String::default();
        connector.read_to_string(&mut text).await?;

        Ok(Box::pin(stream! {
            yield DataResult::Ok(Value::String(text));
        }))
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::text::Text;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Text::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     document.write_data(&mut connector, Value::String("My text".to_string())).await?;
    ///     assert_eq!(r#"My text"#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn write_data(&mut self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        connector
            .write_all(value.as_str().unwrap_or("").as_bytes())
            .await
    }
}
