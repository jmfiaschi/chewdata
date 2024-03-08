//! Read and write data through standard input and output.
//!
//! ### Configuration
//!
//! | key      | alias        | Description                                  | Default Value | Possible Values       |
//! | -------- | ------------ | -------------------------------------------- | ------------- | --------------------- |
//! | type     | -            | Required in order to use this connector      | `io`          | `io`                  |
//! | metadata | meta         | Override metadata information                | `null`        | [`crate::Metadata`] |
//! | eoi      | end_of_input | Last charater that stops the reading in stdin | ``            | string                |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "reader",
//!         "connector":{
//!             "type": "io",
//!             "eoi": "",
//!             "metadata": {
//!                 ...
//!             }
//!         }
//!     }
//! ]
//! ```
use super::Connector;
use crate::connector::paginator::once::Once;
use crate::document::Document;
use crate::{DataSet, DataStream, Metadata};
use async_std::io::BufReader;
use async_std::io::{stdin, stdout};
use async_std::prelude::*;
use async_stream::stream;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;

#[derive(Deserialize, Serialize, Clone, Default, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct Io {
    #[serde(skip)]
    document: Option<Box<dyn Document>>,
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(default = "default_eof")]
    #[serde(alias = "end_of_input")]
    pub eoi: String,
}

fn default_eof() -> String {
    "".to_string()
}

#[async_trait]
impl Connector for Io {
    /// See [`Connector::set_document`] for more details.
    fn set_document(&mut self, document: &Box<dyn Document>) -> Result<()> {
        self.document = Some(document.clone());

        Ok(())
    }
    /// See [`Connector::document`] for more details.
    fn document(&self) -> Result<&Box<dyn Document>> {
        match &self.document {
            Some(document) => Ok(document),
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "The document has not been set in the connector",
            )),
        }
    }
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        "stdout".to_string()
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, _parameters: Value) {}
    /// See [`Connector::is_variable`] for more details.
    fn is_variable(&self) -> bool {
        false
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    fn is_resource_will_change(&self, _new_parameters: Value) -> Result<bool> {
        Ok(false)
    }
    /// See [`Connector::fetch`] for more details.
    #[instrument(name = "io::fetch")]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let stdin = BufReader::new(stdin());
        let document = self.document()?;

        trace!("Retreive lines");
        let mut lines = stdin.lines();
        let mut buf = String::default();

        trace!("Read lines");
        while let Some(line) = lines.next().await {
            let current_line: String = line?;
            if current_line.eq(self.eoi.as_str()) {
                break;
            };
            buf = format!("{}{}\n", buf, current_line);
        }
        trace!("Save lines into the buffer");
        if !document.has_data(buf.as_bytes())? {
            return Ok(None);
        }

        let dataset = document.read(&buf.into_bytes())?;

        info!("Fetch data with success");

        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    #[instrument(name = "io::send", skip(dataset))]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let mut buffer = Vec::default();
        let document = self.document()?;

        buffer.append(&mut document.header(dataset)?);
        buffer.append(&mut document.write(dataset)?);
        buffer.append(&mut document.footer(dataset)?);

        trace!("Write data into stdout");
        stdout().write_all(&buffer).await?;
        // Force to send data
        trace!("Flush data into stdout");
        stdout().flush().await?;

        info!("Send data with success");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "IO connector can't erase data to the remote document. Use other connector type"
        )
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let paginator = Once {};
        paginator.paginate(self).await
    }
}

#[cfg(test)]
mod tests {
    use crate::document::{json::Json, DocumentClone};

    use super::*;
    use async_std::prelude::StreamExt;

    #[async_std::test]
    async fn paginate() {
        let document = Json::default();
        let mut connector = Io::default();
        connector.set_document(&document.clone_box()).unwrap();

        let mut paging = connector.paginate().await.unwrap();
        assert!(
            paging.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            paging.next().await.transpose().unwrap().is_none(),
            "Must return only on connector for IO."
        );
    }
}
