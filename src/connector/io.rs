//! Read and write data through standard input and output.
//!
//! ### Configuration
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
use super::{Connector, Paginator};
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
use std::pin::Pin;
use std::{fmt, io::Result};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Io {
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

impl fmt::Debug for Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Io")
            .field("metadata", &self.metadata)
            .field("eoi", &self.eoi)
            .finish()
    }
}

#[async_trait]
impl Connector for Io {
    /// See [`Connector::path`] for more details.
    fn path(&self) -> String {
        "stdout".to_string()
    }
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
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
    async fn fetch(&mut self, document: &dyn Document) -> std::io::Result<Option<DataStream>> {
        let stdin = BufReader::new(stdin());

        trace!("Retreive lines.");
        let mut lines = stdin.lines();
        let mut buf = String::default();

        trace!("Read lines.");
        while let Some(line) = lines.next().await {
            let current_line: String = line?;
            if current_line.eq(self.eoi.as_str()) {
                break;
            };
            buf = format!("{}{}\n", buf, current_line);
        }
        trace!("Save lines into the buffer.");
        if !document.has_data(buf.as_bytes())? {
            return Ok(None);
        }

        let dataset = document.read(&buf.into_bytes())?;

        info!("The connector fetch data successfully.");
        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    #[instrument(skip(dataset), name = "io::send")]
    async fn send(
        &mut self,
        document: &dyn Document,
        dataset: &DataSet,
    ) -> std::io::Result<Option<DataStream>> {
        let mut buffer = Vec::default();

        buffer.append(&mut document.header(dataset)?);
        buffer.append(&mut document.write(dataset)?);
        buffer.append(&mut document.footer(dataset)?);

        trace!("Write data into stdout");
        stdout().write_all(&buffer).await?;
        // Force to send data
        trace!("Flush data into stdout");
        stdout().flush().await?;

        info!("The connector send data into the resource successfully.");
        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    async fn erase(&mut self) -> Result<()> {
        unimplemented!(
            "IO connector can't erase data to the remote document. Use other connector type"
        )
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send + Sync>>> {
        Ok(Box::pin(Once::new(Box::new(self.clone()))?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::prelude::StreamExt;

    #[async_std::test]
    async fn paginator_stream() {
        let connector = Io::default();
        let paginator = connector.paginator().await.unwrap();
        assert!(!paginator.is_parallelizable());
        let mut stream = paginator.stream().await.unwrap();
        assert!(
            stream.next().await.transpose().unwrap().is_some(),
            "Can't get the first reader."
        );
        assert!(
            stream.next().await.transpose().unwrap().is_none(),
            "Must return only on connector for IO."
        );
    }
}
