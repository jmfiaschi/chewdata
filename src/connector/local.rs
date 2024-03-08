//! Read and write data in local files.
//! It is possible to read multiple files with wildcards.
//! If you want to write dynamically in different files,
//! use the [mustache](http://mustache.github.io/) variable that will be replaced with the data in input.
//!
//! ### Configuration
//!
//! | key        | alias  | Description                                                                                      | Default Value | Possible Values       |
//! | ---------- | ------ | ------------------------------------------------------------------------------------------------ | ------------- | --------------------- |
//! | type       | -      | Required in order to use this connector                                                          | `local`       | `local`               |
//! | metadata   | meta   | Override metadata information                                                                    | `null`        | [`crate::Metadata`] |
//! | path       | -      | Path of a file or list of files. Allow wildcard charater `*` and mustache variables              | `null`        | String                |
//! | parameters | params | Variable that can be use in the path. Parameters of the connector is merged with the current data | `null`       | List of key and value |
//! | algo_with_checksum   | checksum | Text corresponding to '[algorithm]:[checksum to check]'                              | `null`        | 'sha224' / 'sha256' / 'sha384' / 'sha512'  / 'sha3_224'  / 'sha3_256'  / 'sha3_384'  / 'sha3_512' |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "reader",
//!         "connector":{
//!             "type": "local",
//!             "path": "./{{ folder }}/*.json",
//!             "metadata": {
//!                 "content-type": "application/json; charset=utf-8"
//!             },
//!             "parameters": {
//!                 "folder": "my_folder"
//!             }
//!         }
//!     }
//! ]
//! ```
use super::paginator::local::wildcard::Wildcard;
use super::Connector;
use crate::document::Document;
use crate::helper::checksum::str_to_hasher_with_checksum;
use crate::helper::mustache::Mustache;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::{DataResult, DataSet, DataStream, Metadata};
use async_std::sync::Mutex;
use async_stream::stream;
use async_trait::async_trait;
use fs2::FileExt;
use futures::Stream;
use glob::glob;
use json_value_merge::Merge;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::{
    fmt,
    io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
};
use std::{fs, fs::OpenOptions};

static CACHES: OnceLock<Arc<Mutex<HashMap<String, Vec<DataResult>>>>> = OnceLock::new();

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

impl fmt::Display for Local {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buffer = String::default();
        OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open(self.path())
            .unwrap()
            .read_to_string(&mut buffer)
            .unwrap();

        write!(f, "{}", buffer)
    }
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
    fn cache_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let key = self.path().to_string();
        key.hash(&mut hasher);
        hasher.finish().to_string()
    }
    pub async fn cache(&mut self) -> std::io::Result<Option<Vec<DataResult>>> {
        let caches = CACHES.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let cache_key = self.cache_key();
        if let Some(data_results) = caches.lock().await.get(&self.cache_key()) {
            info!(cache_key, "Retrieve entries in the cache");
            return Ok(Some(data_results.clone()));
        }

        Ok(None)
    }
    pub async fn set_cache(&mut self, dataset: &[DataResult]) {
        let caches = CACHES.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let cache_key = self.cache_key();
        let mut map = caches.lock_arc().await;
        map.insert(cache_key.clone(), dataset.to_owned());
        info!(cache_key, "create entries in the cache");
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
        let mut path: String = self.path.clone();

        match self.is_variable() {
            true => {
                let mut params = self.parameters.clone();
                let mut metadata = Map::default();

                metadata.insert("metadata".to_string(), self.metadata().into());
                params.merge(&Value::Object(metadata));

                path.replace_mustache(params.clone());
                path
            }
            false => path,
        }
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
    #[instrument(name = "local::len")]
    async fn len(&self) -> Result<usize> {
        let reg = Regex::new("[*]").unwrap();
        if reg.is_match(self.path.as_ref()) {
            return Err(Error::new(
                ErrorKind::Other,
                "len() method not available for wildcard path",
            ));
        }

        let len = match fs::metadata(self.path()) {
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
    /// use async_std::io::{Read, Write};
    /// use async_std::prelude::*;
    /// use futures::StreamExt;
    /// use std::io;
    ///
    /// #[async_std::main]
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
        let document = self.document()?.clone();
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
            .open(&path)?
            .read_to_end(&mut buff)?;

        info!(path = path, "Fetch data with success");

        if !document.has_data(&buff)? {
            return Ok(None);
        }

        if let Some(algo_with_checksum) = &algo_with_checksum_opt {
            if let (hasher, Some(checksum)) = str_to_hasher_with_checksum(algo_with_checksum)? {
                let mut hasher = hasher.clone();
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
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
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
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(path.as_str())?;

        file.lock_exclusive()?;
        trace!(path = path, "Lock the resource");

        let file_len = file.metadata()?.len();

        match position {
            Some(pos) => match file_len as isize + pos {
                start if start > 0 => file.seek(SeekFrom::Start(start as u64)),
                _ => file.seek(SeekFrom::Start(0)),
            },
            None => file.seek(SeekFrom::Start(0)),
        }?;

        if 0 == file_len {
            file.write_all(&header)?;
        }
        if 0 < file_len && file_len > (header.len() as u64 + footer.len() as u64) {
            file.write_all(&terminator)?;
        }
        file.write_all(&body)?;
        file.write_all(&footer)?;
        trace!(path = path, "Write data into the resource");

        file.unlock()?;
        trace!(path = path, "Unlock the resource");

        info!(
            path = path,
            checksum = match &self.algo_with_checksum {
                Some(algo_with_checksum) => {
                    let (hasher, _) = str_to_hasher_with_checksum(algo_with_checksum)?;
                    let mut hasher = hasher.clone();
                    let mut buff = Vec::default();
                    file.seek(SeekFrom::Start(0))?;
                    file.read_to_end(&mut buff)?;
                    hasher.update(&buff);
                    base16ct::lower::encode_string(&hasher.finalize())
                }
                None => "algorithm undefined".to_string(),
            },
            "Send data with success"
        );

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
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
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
                Ok(path) => OpenOptions::new()
                    .read(false)
                    .create(true)
                    .append(false)
                    .write(true)
                    .truncate(true)
                    .open(path.display().to_string())?,
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
    use futures::StreamExt;

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
    #[async_std::test]
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
    #[async_std::test]
    async fn is_empty() {
        let mut connector = Local::default();
        connector.path = "./data/one_line.json".to_string();
        assert_eq!(false, connector.is_empty().await.unwrap());
        connector.path = "./null_file".to_string();
        assert_eq!(true, connector.is_empty().await.unwrap());
    }
    #[async_std::test]
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
    #[async_std::test]
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
    #[async_std::test]
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
    #[async_std::test]
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
