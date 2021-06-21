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
            mime_type: Some(mime::APPLICATION.to_string()),
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
        Text::default().metadata
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
    ///     let mut data_iter = document.read_data(&mut connector).await?.into_iter();
    ///     let line = data_iter.next().unwrap().to_json_value();
    ///     assert_eq!(r#"My text1 \n My text 2"#, line);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Data> {
        let mut text = String::default();
        connector.read_to_string(&mut text).await?;

        let data = GenBoxed::new_boxed(|co| async move {
            co.yield_(DataResult::Ok(Value::String(text))).await;
        });

        Ok(data)
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
    async fn write_data(&self, connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        connector
            .write_all(value.as_str().unwrap_or("").as_bytes())
            .await
    }
}
