use super::{Connector, Paginator};
use crate::helper::mustache::Mustache;
use crate::Metadata;
use async_stream::stream;
use async_trait::async_trait;
use fs2::FileExt;
use futures::Stream;
use glob::glob;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufReader, BufRead};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;
use std::{
    fmt,
    io::{Cursor, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
};
use std::{fs, fs::OpenOptions};

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Local {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub path: String,
    #[serde(alias = "params")]
    pub parameters: Value,
    #[serde(skip)]
    pub inner: Cursor<Vec<u8>>,
}

impl fmt::Display for Local {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8(self.inner.clone().into_inner()).unwrap_or_default()
        )
    }
}

// Not display the inner for better performance with big data
impl fmt::Debug for Local {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Local")
            .field("metadata", &self.metadata)
            .field("path", &self.path)
            .field("parameters", &self.parameters)
            .finish()
    }
}

impl Local {
    pub fn new(path: String) -> Self {
        Local {
            path,
            ..Default::default()
        }
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
            (true, params) => {
                let mut path = self.path.clone();
                path.replace_mustache(params);
                path
            }
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
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn len(&mut self) -> Result<usize> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::Other,
                "len() method not available for wildcard path.",
            ));
        }

        let len = match fs::metadata(self.path()) {
            Ok(metadata) => {
                let len = metadata.len() as usize;
                info!(len = len, "The connector found data in the file");
                len
            }
            Err(_) => {
                let len = 0;
                info!(len = len, "The connector not found data in the file");
                len
            }
        };

        Ok(len)
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
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
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
                    info!("The connector checked a file with data");
                    return Ok(false);
                }
            }
            Err(_) => {
                info!(
                    "The connector checked an empty file, impossible to reach the file's metadata"
                );
                return Ok(true);
            }
        };

        info!("The connector checked an empty file");
        Ok(true)
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
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
    ///
    /// # Example:
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/out/test_local_send".to_string();
    ///     connector.erase().await?;
    ///     connector.write(r#"{"column1":"value1"}"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}"#, buffer);
    ///
    ///     connector.write(r#"{"column1":"value2"}"#.as_bytes()).await?;
    ///     connector.send(Some(0)).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}{"column1":"value2"}"#, buffer);
    ///
    ///     connector.write(r#"{"column1":"value3"}"#.as_bytes()).await?;
    ///     connector.send(Some(-20)).await?;
    ///
    ///     let mut connector_read = connector.clone();
    ///     connector_read.fetch().await?;
    ///     let mut buffer = String::default();
    ///     connector_read.read_to_string(&mut buffer).await?;
    ///     assert_eq!(r#"{"column1":"value1"}{"column1":"value3"}"#, buffer);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn send(&mut self, position: Option<isize>) -> Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.path().as_str())?;

        file.lock_exclusive()?;
        trace!("The connector lock the file");

        let file_len = file.metadata()?.len();

        match position {
            Some(pos) => match file_len as isize + pos {
                start if start > 0 => file.seek(SeekFrom::Start(start as u64)),
                _ => file.seek(SeekFrom::Start(0)),
            },
            None => file.seek(SeekFrom::Start(0)),
        }?;

        file.write_all(self.inner.get_ref())?;
        trace!("The connector write data into the file");

        file.unlock()?;
        trace!("The connector unlock the file");

        self.clear();

        info!("The connector send data into the file with success");
        Ok(())
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
    #[instrument]
    fn is_resource_will_change(&self, new_parameters: Value) -> Result<bool> {
        if !self.is_variable() {
            trace!("The connector stay link to the same file");
            return Ok(false);
        }

        let mut actuel_path = self.path.clone();
        actuel_path.replace_mustache(self.parameters.clone());

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if actuel_path == new_path {
            trace!("The connector stay link to the same file");
            return Ok(false);
        }

        info!("The connector will use another file, regarding the new parameters");
        Ok(true)
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
    #[instrument]
    async fn fetch(&mut self) -> Result<()> {
        // Avoid to fetch two times the same data in the same connector
        if !self.inner.get_ref().is_empty() {
            return Ok(());
        }

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

        info!("The connector fetch data with success");
        Ok(())
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/out/test_local_erase".to_string();
    ///     connector.write(r#"{"column1":"value1"}"#.as_bytes()).await?;
    ///     connector.send(None).await?;
    ///     connector.erase().await?;
    ///     connector.fetch().await?;
    ///     assert_eq!(true, connector.inner().is_empty());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn erase(&mut self) -> Result<()> {
        OpenOptions::new()
            .read(false)
            .create(true)
            .append(false)
            .write(true)
            .truncate(true)
            .open(self.path().as_str())?;

        info!("The connector erase the file with success");
        Ok(())
    }
    /// See [`Connector::paginator`] for more details.
    async fn paginator(&self) -> Result<Pin<Box<dyn Paginator + Send>>> {
        Ok(Box::pin(LocalPaginator::new(self.clone())?))
    }
    /// See [`Connector::clear`] for more details.
    #[instrument]
    fn clear(&mut self) {
        self.inner = Default::default();
        trace!("The connector is cleaned");
    }
    /// See [`Connector::chunk`] for more details.
    #[instrument]
    async fn chunk(&self, start: usize, end: usize) -> Result<Vec<u8>> {
        if end < start {
            return Err(Error::new(ErrorKind::InvalidInput, "The start 'value' parameter must be lower or equal to the 'end' value parameter"));
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open(self.path())?;

        file.seek(SeekFrom::Start(start as u64))?;

        let mut reader = BufReader::with_capacity(end - start, file);
        reader.fill_buf()?;

        Ok(reader.buffer().to_vec())
    }
}

#[async_trait]
impl async_std::io::Read for Local {
    /// See [`async_std::io::Read::poll_read`] for more details.
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
    /// See [`async_std::io::Write::poll_write`] for more details.
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Poll::Ready(std::io::Write::write(&mut self.inner, buf))
    }
    /// See [`async_std::io::Write::poll_flush`] for more details.
    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.inner))
    }
    /// See [`async_std::io::Write::poll_close`] for more details.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.poll_flush(cx)
    }
}

#[derive(Debug)]
pub struct LocalPaginator {
    pub connector: Local,
    pub paths: IntoIter<String>,
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
    ///     let mut paginator = LocalPaginator::new(connector)?;
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     assert_eq!(r#"data/one_line.csv"#, stream.next().await.transpose()?.unwrap().path());
    ///     assert_eq!(r#"data/one_line.json"#, stream.next().await.transpose()?.unwrap().path());
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new(connector: Local) -> Result<Self> {
        if connector.path().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "The field 'path' for a local connector can't be an empty string".to_string(),
            ));
        }

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
            connector,
            paths: paths.into_iter(),
        })
    }
}

#[async_trait]
impl Paginator for LocalPaginator {
    /// See [`Paginator::count`] for more details.
    async fn count(&mut self) -> Result<Option<usize>> {
        Ok(Some(self.paths.clone().count()))
    }
    /// See [`Paginator::stream`] for more details.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/one_line.*".to_string();
    ///
    ///     let mut paginator = connector.paginator().await?;
    ///     assert!(paginator.is_parallelizable());
    ///     let mut stream = paginator.stream().await?;
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;
    ///     let mut buffer1 = String::default();
    ///     let len1 = connector.read_to_string(&mut buffer1).await?;
    ///     assert!(0 < len1, "Can't read the content of the file.");
    ///
    ///     let mut connector = stream.next().await.transpose()?.unwrap();
    ///     connector.fetch().await?;
    ///     let mut buffer2 = String::default();
    ///     let len2 = connector.read_to_string(&mut buffer2).await?;
    ///     assert!(0 < len2, "Can't read the content of the file.");
    ///     assert!(buffer1 != buffer2, "The content of this two files are not different.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn stream(
        &mut self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        let connector = self.connector.clone();
        let mut paths = self.paths.clone();

        let stream = Box::pin(stream! {
            while let Some(path) = paths.next() {
                let mut new_connector = connector.clone();
                new_connector.path = path.clone();

                trace!(connector = format!("{:?}", new_connector).as_str(), "The stream return a new connector");
                yield Ok(Box::new(new_connector) as Box<dyn Connector>);
            }
            trace!("The stream stop to return new connectors");
        });

        Ok(stream)
    }
    /// See [`Paginator::is_parallelizable`] for more details.
    fn is_parallelizable(&mut self) -> bool {
        true
    }
}
