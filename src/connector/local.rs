use super::{Connector, Paginator};
use crate::document::DocumentType;
use crate::helper::mustache::Mustache;
use crate::step::DataResult;
use crate::Metadata;
use async_trait::async_trait;
use glob::glob;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
};
use std::{fs, fs::OpenOptions};

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(default)]
pub struct Local {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    #[serde(alias = "document")]
    document_type: DocumentType,
    pub path: String,
    pub parameters: Value,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Local {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8(self.inner.clone().into_inner()).unwrap_or("".to_string()))
    }
}

impl Local {
    /// Set the current path
    fn set_path(&mut self, path: String) {
        self.path = path;
    }
}

#[async_trait]
impl Connector for Local {
    /// See [`Connector::path`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
    /// connector.set_parameters(params);
    /// assert_eq!("/dir/filename_value.ext", connector.path());
    /// ```
    fn path(&self) -> String {
        match (self.is_variable(), self.parameters.clone()) {
            (true, params) => self.path.clone().replace_mustache(params),
            _ => self.path.clone(),
        }
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./Cargo.toml".to_string();
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "./not_found_file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///     connector.path = "./*.ext".to_string();
    ///     assert_eq!(connector.len().await.map_err(|e| e.kind()), Err(io::ErrorKind::Other));
    ///     Ok(())
    /// }
    /// ```
    async fn len(&mut self) -> Result<usize> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::Other,
                "len() method not available for wildcard path.",
            ));
        }

        match fs::metadata(self.path()) {
            Ok(metadata) => Ok(metadata.len() as usize),
            Err(_) => Ok(0),
        }
    }
    /// See [`Connector::is_empty`] for more details.
    /// Not work for wildcard path.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./Cargo.toml".to_string();
    ///     assert_eq!(false, connector.is_empty().await?);
    ///     connector.path = "./null_file".to_string();
    ///     assert_eq!(true, connector.is_empty().await?);
    ///     connector.path = "./*.ext".to_string();
    ///     assert_eq!(connector.len().await.map_err(|e| e.kind()), Err(io::ErrorKind::Other));
    ///     Ok(())
    /// }
    /// ```
    async fn is_empty(&mut self) -> Result<bool> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::Other,
                "is_empty() method not available for wildcard path.",
            ));
        }

        match fs::metadata(self.path()) {
            Ok(metadata) => {
                if 0 < metadata.len() {
                    return Ok(false);
                }
            }
            Err(_) => {
                return Ok(true);
            }
        };

        Ok(true)
    }
    /// See [`Connector::set_parameters`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// assert_eq!(Value::Null, connector.parameters);
    /// let params: Value = Value::String("my param".to_string());
    /// connector.set_parameters(params.clone());
    /// assert_eq!(params.clone(), connector.parameters.clone());
    /// ```
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters.clone();
    }
    /// See [`Connector::is_variable_path`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// assert_eq!(false, connector.is_variable());
    /// connector.path = "/dir/filename_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_variable());
    /// ```
    fn is_variable(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    /// See [`Connector::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        self.metadata.clone()
    }
    /// See [`Connector::send`] for more details.
    async fn send(&mut self) -> Result<()> {
        self.document_type().document_inner().flush(self).await
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use serde_json::Value;
    ///
    /// let mut connector = Local::default();
    /// let params = serde_json::from_str(r#"{"field":"test"}"#).unwrap();
    /// assert_eq!(false, connector.is_resource_will_change(Value::Null).unwrap());
    /// connector.path = "/dir/static.ext".to_string();
    /// assert_eq!(false, connector.is_resource_will_change(Value::Null).unwrap());
    /// connector.path = "/dir/dynamic_{{ field }}.ext".to_string();
    /// assert_eq!(true, connector.is_resource_will_change(params).unwrap());
    /// ```
    fn is_resource_will_change(&self, new_parameters: Value) -> Result<bool> {
        if !self.is_variable() {
            return Ok(false);
        }

        let actuel_path = self.path.clone().replace_mustache(self.parameters.clone());
        let new_path = self.path.clone().replace_mustache(new_parameters);

        if actuel_path == new_path {
            return Ok(false);
        }

        Ok(true)
    }
    /// See [`Connector::document_type`] for more details.
    fn document_type(&self) -> DocumentType {
        self.document_type.clone()
    }
    /// See [`Connector::inner`] for more details.
    fn inner(&self) -> &Vec<u8> {
        self.inner.get_ref()
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     assert_eq!(0, connector.inner().len());
    ///     connector.path = "./Cargo.toml".to_string();
    ///     connector.fetch().await?;
    ///     assert!(0 < connector.inner().len(), "The inner connector should have a size upper than zero");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn fetch(&mut self) -> Result<()> {
        let mut buff = Vec::default();
        OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open(self.path())?
            .read_to_end(&mut buff)?;
        self.inner = Cursor::new(buff);

        Ok(())
    }
    /// See [`Connector::push_data`] for more details.
    async fn push_data(&mut self, data: DataResult) -> Result<()> {
        debug!(slog_scope::logger(), "push data"; "data" => format!("{}", data.to_json_value()));
        let connector = self;
        let document = connector.document_type().document_inner();

        document.write_data(connector, data.to_json_value()).await
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/out/test_local_flush_1".to_string();
    ///     connector.write(r#"{"column1":"value1"}"#.to_string().into_bytes().as_slice()).await?;
    ///     connector.flush().await?;
    ///     connector.erase().await?;
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#""#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn erase(&mut self) -> Result<()> {
        OpenOptions::new()
            .read(false)
            .create(true)
            .append(false)
            .write(true)
            .truncate(true)
            .open(self.path().as_str())?
            .write_all(String::default().as_bytes())
    }
    /// See [`Connector::flush_into`] for more details.
    ///
    /// # Example: Seek from the end
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector_write = Local::default();
    ///     connector_write.path = "./data/out/test_local_seek_and_flush_1".to_string();
    ///     connector_write.erase().await?;
    ///     connector_write.write(r#"[{"column1":"value1"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush_into(0).await?;
    ///     
    ///     let mut connector_read = Local::default();
    ///     connector_read.path = "./data/out/test_local_seek_and_flush_1".to_string();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    ///     connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush_into(-1).await?;
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Seek from the start
    /// ```
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector_write = Local::default();
    ///     connector_write.path = "./data/out/test_local_seek_and_flush_2".to_string();
    ///     connector_write.erase().await?;
    ///
    ///     let str = r#"[{"column1":"value1"}]"#;
    ///     connector_write.write(str.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush_into(0).await?;
    ///
    ///     let mut connector_read = Local::default();
    ///     connector_read.path = "./data/out/test_local_seek_and_flush_2".to_string();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"}]"#, buffer);
    ///
    ///     connector_write.write(r#",{"column1":"value2"}]"#.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush_into((str.len() as i64)-1).await?;
    ///
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"[{"column1":"value1"},{"column1":"value2"}]"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn flush_into(&mut self, position: i64) -> Result<()> {
        let mut position = position;

        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.path().as_str())?;

        if 0 >= (file.metadata()?.len() as i64 + position) {
            position = 0;
        }

        if 0 <= position {
            file.seek(SeekFrom::Start(position as u64))?;
        }
        if 0 > position {
            file.seek(SeekFrom::End(position as i64))?;
        }

        file.write_all(self.inner.get_ref())?;
        self.inner = Default::default();
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(LocalPaginator::new(Box::new(self.clone()))?))
    }
}

#[async_trait]
impl async_std::io::Read for Local {
    /// See [`Read::poll_read`] for more details.
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Read::read(&mut self.inner, buf))
    }
}

#[async_trait]
impl async_std::io::Write for Local {
    /// See [`Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`Write::poll_flush`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector_write = Local::default();
    ///     connector_write.path = "./data/out/test_local_flush_1".to_string();
    ///     connector_write.erase().await?;
    ///     connector_write.write(r#"{"column1":"value1"}"#.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush().await?;
    ///     
    ///     let mut connector_read = Local::default();
    ///     connector_read.path = "./data/out/test_local_flush_1".to_string();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}"#, buffer);
    ///
    ///     connector_write.write(r#",{"column1":"value2"}"#.to_string().into_bytes().as_slice()).await?;
    ///     connector_write.flush().await?;
    ///
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"},{"column1":"value2"}"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // initialize the position of the cursor in order to write the content.
        self.inner.set_position(0);

        if self.inner.get_ref().is_empty() {
            // Nothing to flush
            return Poll::Ready(Ok(()));
        }

        OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(self.path())?
            .write_all(self.inner.get_ref())?;

        self.inner = Default::default();

        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct LocalPaginator {
    connector: Box<dyn Connector>,
    paths: IntoIter<String>,
}

impl LocalPaginator {
    /// Paginate on each files.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::{Local, LocalPaginator};
    /// use chewdata::connector::{Connector, Paginator, ConnectorType};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/one_line.*".to_string();
    ///     let mut paginator = LocalPaginator::new(ConnectorType::Local(connector))?;
    ///     
    ///     assert_eq!(r#"data/one_line.csv"#, paginator.next_page().await?.unwrap().path());
    ///     assert_eq!(r#"data/one_line.json"#, paginator.next_page().await?.unwrap().path());
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new(connector: Box<dyn Connector>) -> Result<Self> {
        let paths: Vec<String> = match glob(connector.path().as_str()) {
            Ok(paths) => Ok(paths
                .filter(|p| p.is_ok())
                .map(|p| p.unwrap().display().to_string())
                .collect()),
            Err(e) => Err(Error::new(ErrorKind::InvalidInput, e)),
        }?;

        if paths.is_empty() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!(
                    "No files found with this path pattern '{}'.",
                    connector.path()
                ),
            ));
        }

        Ok(LocalPaginator {
            connector: connector,
            paths: paths.into_iter(),
        })
    }
}

#[async_trait]
impl Paginator for LocalPaginator {
    /// See [`Paginator::next_page`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/one_line.*".to_string();
    ///     let mut paginator = connector.paginator().await?;
    ///
    ///     let mut connector = paginator.next_page().await?.unwrap();     
    ///     let mut buffer1 = String::default();
    ///     let len1 = connector.read_to_string(&mut buffer1).await?;
    ///     assert!(0 < len1, "Can't read the content of the file.");
    ///
    ///     let mut connector = paginator.next_page().await?.unwrap();     
    ///     let mut buffer2 = String::default();
    ///     let len2 = connector.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files are not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn next_page(&mut self) -> Result<Option<Box<dyn Connector>>> {
        let mut connector = Local::default();

        Ok(match self.paths.next() {
            Some(path) => {
                info!(slog_scope::logger(), "next page"; "path"=> path.clone());
                connector.set_path(path);
                connector.fetch().await?;
                Some(Box::new(connector))
            }
            None => None,
        })
    }
}
