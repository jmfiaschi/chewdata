//! Local file connector
//!
//! This connector reads from and writes to **local filesystem files**.
//!
//! It supports:
//! - Reading **one or multiple files** using glob wildcards (`*`)
//! - Dynamic paths using **Mustache templates**
//! - Optional **checksum verification**
//! - Optional **in-memory caching**
//!
//! ---
//!
//! ## Configuration
//!
//! | Key | Alias | Description | Default | Possible Values |
//! |-----|-------|-------------|---------|-----------------|
//! | `type` | – | Required to select this connector | `local` | `local` |
//! | `metadata` | `meta` | Override or enrich resource metadata | `null` | [`crate::Metadata`] |
//! | `path` | – | File path or glob pattern. Supports `*` and Mustache variables | `null` | `String` |
//! | `parameters` | `params` | Variables injected into the path template | `null` | JSON object |
//! | `algo_with_checksum` | `checksum` | Checksum validation in the form `algorithm:checksum` | `null` | `sha224`, `sha256`, `sha384`, `sha512`, `sha3_*` |
//!
//! ---
//!
//! ## Example
//!
//! ```json
//! [
//!   {
//!     "type": "reader",
//!     "connector": {
//!       "type": "local",
//!       "path": "./{{ folder }}/*.json",
//!       "metadata": {
//!         "content-type": "application/json; charset=utf-8"
//!       },
//!       "parameters": {
//!         "folder": "my_folder"
//!       }
//!     }
//!   }
//! ]
//! ```
use super::paginator::local::wildcard::Wildcard;
use super::Connector;
use crate::document::Document;
use crate::helper::checksum::{hasher, str_to_algorithm_name_with_checksum};
use crate::helper::mustache::Mustache;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{DataResult, DataSet, DataStream, Metadata};
use async_fs::OpenOptions;
use async_lock::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use glob::glob;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::{
    fmt,
    io::{Error, ErrorKind, Result, SeekFrom},
};

type SharedCache = Arc<Mutex<HashMap<String, Vec<DataResult>>>>;
static CACHES: OnceLock<SharedCache> = OnceLock::new();

#[derive(Deserialize, Serialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Local {
    #[serde(skip)]
    document: Option<Box<dyn Document>>,
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub path: String,
    #[serde(alias = "params")]
    pub parameters: Value,
    pub is_cached: bool,
    #[serde(alias = "checksum")]
    pub algo_with_checksum: Option<String>,
}

impl fmt::Debug for Local {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Local")
            .field("metadata", &self.metadata)
            .field("path", &self.path)
            .field("parameters", &self.parameters.display_only_for_debugging())
            .field("is_cached", &self.is_cached)
            .field("algo_with_checksum", &self.algo_with_checksum)
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
    fn caches() -> &'static Arc<Mutex<HashMap<String, Vec<DataResult>>>> {
        CACHES.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
    }
    fn cache_key(&self) -> String {
        self.path()
    }
    pub async fn cache(&self) -> io::Result<Option<Vec<DataResult>>> {
        let key = self.cache_key();
        let caches = Self::caches();
        let guard = caches.lock().await;

        if let Some(dataset) = guard.get(&key) {
            info!(cache_key = key, "Cache hit");
            Ok(Some(dataset.clone()))
        } else {
            Ok(None)
        }
    }
    pub async fn set_cache(&self, dataset: &[DataResult]) {
        let key = self.cache_key();
        let caches = Self::caches();
        caches.lock().await.insert(key.clone(), dataset.to_vec());
        info!(cache_key = key, "Cache stored");
    }
}

#[async_trait]
impl Connector for Local {
    /// See [`Connector::set_document`] for more details.
    fn set_document(&mut self, document: Box<dyn Document>) -> Result<()> {
        self.document = Some(document);

        Ok(())
    }
    /// See [`Connector::document`] for more details.
    fn document(&self) -> Result<&dyn Document> {
        self.document.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "The document has not been set in the connector",
            )
        })
    }
    /// See [`Connector::path`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
        if !self.is_variable() {
            return self.path.clone();
        }

        let mut params = self.parameters.clone();
        params.merge(&serde_json::json!({
            "metadata": self.metadata()
        }));

        let mut path = self.path.clone();
        path.replace_mustache(params);
        path
    }
    /// See [`Connector::len`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Local::default();
    ///     connector.path = "./Cargo.toml".to_string();
    ///     assert!(0 < connector.len().await?, "The length of the document is not greather than 0");
    ///     connector.path = "./not_found_file".to_string();
    ///     assert_eq!(0, connector.len().await?);
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "local::len")]
    async fn len(&self) -> Result<usize> {
        if self.path.contains('*') {
            return Err(Error::other("len() method not available for wildcard path"));
        }

        let len = match async_fs::metadata(self.path()).await {
            Ok(metadata) => {
                let len = metadata.len() as usize;
                info!(len = len, "Find the length");
                len
            }
            Err(_) => {
                let len = 0;
                info!(len = len, "Can't find the length");
                len
            }
        };

        Ok(len)
    }
    /// See [`Connector::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
    /// See [`Connector::is_variable`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
        self.path.has_mustache()
    }
    /// See [`Connector::is_resource_will_change`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
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
            trace!("Stay link to the same resource");
            return Ok(false);
        }

        let mut metadata_kv = Map::default();
        metadata_kv.insert("metadata".to_string(), self.metadata().into());
        let metadata = Value::Object(metadata_kv);

        let mut new_parameters = new_parameters;
        new_parameters.merge(&metadata);
        let mut old_parameters = self.parameters.clone();
        old_parameters.merge(&metadata);

        let mut previous_path = self.path.clone();
        previous_path.replace_mustache(old_parameters);

        let mut new_path = self.path.clone();
        new_path.replace_mustache(new_parameters);

        if previous_path == new_path {
            trace!(path = previous_path, "Stay link to the same resource");
            return Ok(false);
        }

        info!(
            previous_path = previous_path,
            new_path = new_path,
            "Will use another resource, regarding the new parameters"
        );
        Ok(true)
    }
    /// See [`Connector::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        match &self.document {
            Some(document) => self.metadata.clone().merge(&document.metadata()),
            None => self.metadata.clone(),
        }
    }
    /// See [`Connector::fetch`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use smol::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    /// use smol::prelude::*;
    /// use smol::stream::StreamExt;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///     let mut connector = Local::default();
    ///     connector.set_document(document);
    ///     connector.path = "./data/one_line.json".to_string();
    ///     let datastream = connector.fetch().await.unwrap().unwrap();
    ///     assert!(
    ///         0 < datastream.count().await,
    ///         "The inner connector should have a size upper than zero"
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "local::fetch")]
    async fn fetch(&mut self) -> std::io::Result<Option<DataStream>> {
        let document = self.document()?;
        let mut buff = Vec::default();
        let path = self.path();
        let algo_with_checksum_opt = self.algo_with_checksum.clone();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        if self.is_cached {
            if let Some(dataset) = self.cache().await? {
                return Ok(Some(Box::pin(stream! {
                    for data in dataset {
                        yield data;
                    }
                })));
            }
        }

        OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open(&path)
            .await?
            .read_to_end(&mut buff)
            .await?;

        info!(path = path, "Fetch data with success");

        if !document.has_data(&buff)? {
            return Ok(None);
        }

        if let Some(algorithm_name_with_checksum) = &algo_with_checksum_opt {
            if let (algorithm_name, Some(checksum)) =
                str_to_algorithm_name_with_checksum(algorithm_name_with_checksum)?
            {
                let mut hasher = hasher(algorithm_name)?;
                hasher.update(&buff);

                let digest = base16ct::lower::encode_string(&hasher.finalize());

                if !digest.eq(checksum) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "Checksum verification failed. {}({}) != configuration({})",
                            path, digest, checksum
                        ),
                    ));
                }
            };
        }

        let dataset = document.read(&buff)?;

        if self.is_cached {
            self.set_cache(&dataset).await;
        }

        Ok(Some(Box::pin(stream! {
            for data in dataset {
                yield data;
            }
        })))
    }
    /// See [`Connector::send`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let expected_result1 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result1.clone()];
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/out/test_local_send".to_string();
    ///     connector.set_document(document)?;
    ///     connector.erase().await.unwrap();
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());
    ///
    ///     let expected_result2 =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
    ///     let dataset = vec![expected_result2.clone()];
    ///     connector.send(&dataset).await.unwrap();
    ///
    ///     let mut connector_read = connector.clone();
    ///     let mut datastream = connector_read.fetch().await.unwrap().unwrap();
    ///     assert_eq!(expected_result1, datastream.next().await.unwrap());
    ///     assert_eq!(expected_result2, datastream.next().await.unwrap());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(dataset), name = "local::send")]
    async fn send(&mut self, dataset: &DataSet) -> std::io::Result<Option<DataStream>> {
        let document = self.document()?;
        let terminator = document.terminator()?;
        let footer = document.footer(dataset)?;
        let header = document.header(dataset)?;
        let body = document.write(dataset)?;
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let position = match document.can_append() {
            true => Some(-(footer.len() as isize)),
            false => None,
        };

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path.as_str())
            .await?;

        trace!(path = path, "Lock the resource");

        let file_len = file.metadata().await?.len();

        match position {
            Some(pos) => match file_len as isize + pos {
                start if start > 0 => file.seek(SeekFrom::Start(start as u64)).await,
                _ => file.seek(SeekFrom::Start(0)).await,
            },
            None => file.seek(SeekFrom::Start(0)).await,
        }?;

        if 0 == file_len {
            file.write_all(&header).await?;
        }
        if 0 < file_len && file_len > (header.len() as u64 + footer.len() as u64) {
            file.write_all(&terminator).await?;
        }
        file.write_all(&body).await?;
        file.write_all(&footer).await?;
        file.flush().await?;
        trace!(path = path, "Write data into the resource");

        let checksum = match &self.algo_with_checksum {
            Some(algorithm_name_with_checksum) => {
                let (algorithm_name, _) =
                    str_to_algorithm_name_with_checksum(algorithm_name_with_checksum)?;
                let mut hasher = hasher(algorithm_name)?;
                let mut buff = Vec::default();
                file.seek(SeekFrom::Start(0)).await?;
                file.read_to_end(&mut buff).await?;
                hasher.update(&buff);
                base16ct::lower::encode_string(&hasher.finalize())
            }
            None => "algorithm undefined".to_string(),
        };

        info!(path = path, checksum = checksum, "Send data with success");

        Ok(None)
    }
    /// See [`Connector::erase`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::local::Local;
    /// use chewdata::connector::Connector;
    /// use chewdata::document::json::Json;
    /// use chewdata::DataResult;
    /// use smol::prelude::*;
    /// use std::io;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let mut connector = Local::default();
    ///     connector.path = "./data/out/test_local_erase".to_string();
    ///     let expected_result =
    ///         DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
    ///     let dataset = vec![expected_result];
    ///     connector.set_document(document);
    ///
    ///     connector.send(&dataset).await.unwrap();
    ///     connector.erase().await.unwrap();
    ///     let datastream = connector.fetch().await.unwrap();
    ///     assert!(datastream.is_none(), "No datastream with empty body");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "local::erase")]
    async fn erase(&mut self) -> Result<()> {
        let path = self.path();

        if path.has_mustache() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("This path '{}' is not fully resolved", path),
            ));
        }

        let paths = glob(path.as_str()).map_err(|e| Error::new(ErrorKind::NotFound, e))?;
        for path_result in paths {
            match path_result {
                Ok(path) => {
                    OpenOptions::new()
                        .read(false)
                        .create(true)
                        .append(false)
                        .write(true)
                        .truncate(true)
                        .open(path.display().to_string())
                        .await?
                }
                Err(e) => return Err(Error::new(ErrorKind::NotFound, e)),
            };
        }

        info!(path = path, "Erase data with success");
        Ok(())
    }
    /// See [`Connector::paginate`] for more details.
    async fn paginate(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn Connector>>> + Send>>> {
        Wildcard::new(self)?.paginate(self).await
    }
}

#[cfg(test)]
mod tests {
    use macro_rules_attribute::apply;
    use smol::stream::StreamExt;
    use smol_macros::test;

    use super::*;
    use crate::document::json::Json;
    use crate::DataResult;

    #[test]
    fn is_variable() {
        let mut connector = Local::default();
        assert_eq!(false, connector.is_variable());
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        assert_eq!(true, connector.is_variable());
    }
    #[test]
    fn is_resource_will_change() {
        let mut connector = Local::default();
        let params = serde_json::from_str(r#"{"field":"test"}"#).unwrap();
        assert_eq!(
            false,
            connector.is_resource_will_change(Value::Null).unwrap()
        );
        connector.path = "/dir/static.ext".to_string();
        assert_eq!(
            false,
            connector.is_resource_will_change(Value::Null).unwrap()
        );
        connector.path = "/dir/dynamic_{{ field }}.ext".to_string();
        assert_eq!(true, connector.is_resource_will_change(params).unwrap());
    }
    #[test]
    fn path() {
        let mut connector = Local::default();
        connector.path = "/dir/filename_{{ field }}.ext".to_string();
        let params: Value = serde_json::from_str(r#"{"field":"value"}"#).unwrap();
        connector.set_parameters(params);
        assert_eq!("/dir/filename_value.ext", connector.path());
    }
    #[apply(test!)]
    async fn len() {
        let mut connector = Local::default();
        connector.path = "./data/one_line.json".to_string();
        assert!(
            0 < connector.len().await.unwrap(),
            "The length of the document is not greather than 0."
        );
        connector.path = "./not_found_file".to_string();
        assert_eq!(0, connector.len().await.unwrap());
    }
    #[apply(test!)]
    async fn is_empty() {
        let mut connector = Local::default();
        connector.path = "./data/one_line.json".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "./null_file".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[apply(test!)]
    async fn fetch() {
        let document = Json::default();
        let mut connector = Local::default();
        connector.path = "./data/one_line.json".to_string();
        connector.set_document(Box::new(document)).unwrap();
        let datastream = connector.fetch().await.unwrap().unwrap();
        assert!(
            0 < datastream.count().await,
            "The inner connector should have a size upper than zero."
        );
    }
    #[apply(test!)]
    async fn send() {
        let document = Json::default();

        let expected_result1 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result1.clone()];
        let mut connector = Local::default();
        connector.path = "./data/out/test_local_send".to_string();
        connector.erase().await.unwrap();
        connector.set_document(Box::new(document)).unwrap();
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1.clone(), datastream.next().await.unwrap());

        let expected_result2 =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value2"}"#).unwrap());
        let dataset = vec![expected_result2.clone()];
        connector.send(&dataset).await.unwrap();

        let mut connector_read = connector.clone();
        let mut datastream = connector_read.fetch().await.unwrap().unwrap();
        assert_eq!(expected_result1, datastream.next().await.unwrap());
        assert_eq!(expected_result2, datastream.next().await.unwrap());
    }
    #[apply(test!)]
    async fn erase() {
        let document = Json::default();

        let mut connector = Local::default();
        connector.path = "./data/out/test_local_erase".to_string();
        let expected_result =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result];
        connector.set_document(Box::new(document)).unwrap();
        connector.send(&dataset).await.unwrap();
        connector.erase().await.unwrap();
        let datastream = connector.fetch().await.unwrap();
        assert!(datastream.is_none(), "No datastream with empty body.");
    }
    #[apply(test!)]
    async fn erase_with_wildcard() {
        let document = Json::default();

        let mut connector = Local::default();
        connector.path = "./data/out/test_local_erase_with_wildcard".to_string();
        let expected_result =
            DataResult::Ok(serde_json::from_str(r#"{"column1":"value1"}"#).unwrap());
        let dataset = vec![expected_result];
        connector.set_document(Box::new(document)).unwrap();
        connector.send(&dataset).await.unwrap();
        connector.erase().await.unwrap();
        let datastream = connector.fetch().await.unwrap();
        assert!(datastream.is_none(), "No datastream with empty body.");
    }
}
