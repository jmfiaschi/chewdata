use crate::connector::Connector;
use crate::document::Document;
use crate::Metadata;
use crate::{DataResult, Dataset};
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder};
use async_std::io::prelude::WriteExt;
use async_std::io::ReadExt;
use async_stream::stream;
use async_trait::async_trait;
use byteorder::{LittleEndian, ByteOrder};
use json_value_search::Search;
use parquet::arrow::{ArrowWriter, parquet_to_arrow_schema};
use parquet::basic::{Compression, Encoding};
use parquet::file::footer::parse_metadata;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use parquet::file::writer::InMemoryWriteableCursor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Seek, SeekFrom, Read};
use std::sync::Arc;
use arrow::datatypes::Schema;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct Parquet {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub entry_path: Option<String>,
    pub schema: Option<Schema>,
    // List of temporary values used to write into the connector
    #[serde(skip)]
    pub inner: Vec<Value>,
}

const DEFAULT_SUBTYPE: &str = "parquet";
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];
const DEFAULT_FOOTER_READ_SIZE: usize = 64 * 1024;
const FOOTER_SIZE: usize = 8;

impl Default for Parquet {
    fn default() -> Self {
        let metadata = Metadata {
            mime_type: Some(mime::APPLICATION.to_string()),
            mime_subtype: Some(DEFAULT_SUBTYPE.to_string()),
            charset: Some(mime::UTF_8.to_string()),
            ..Default::default()
        };
        Parquet {
            metadata,
            entry_path: None,
            schema: None,
            inner: Vec::default(),
        }
    }
}

#[async_trait]
impl Document for Parquet {
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Parquet::default().metadata.merge(self.metadata.clone())
    }
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        self.entry_path = Some(entry_path);
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Example: Should read the array input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("[{}]", json_str.clone())));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(json_str)?;
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read the object input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let json_str = r#"{"string":"My text","string_backspace":"My text with \nbackspace","special_char":"€","int":10,"float":9.5,"bool":true}"#;
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(&format!("{}", json_str.clone())));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     let expected_data: Value = serde_json::from_str(json_str).unwrap();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not read the input data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use chewdata::DataResult;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"My text"#));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap();
    ///     match data {
    ///         DataResult::Ok(_) => assert!(false, "The data readed by the json builder should be in error."),
    ///         DataResult::Err(_) => ()
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should read specific array in the records and return each data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     document.entry_path = Some("/*/array*/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"{"field":"value1"}"#)?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Should not found the entry path.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     document.entry_path = Some("/*/not_found/*".to_string());
    ///     let mut connector: Box<dyn Connector> = Box::new(InMemory::new(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]}]"#));
    ///     connector.fetch().await?;
    ///     let expected_data: Value = serde_json::from_str(r#"[{"array1":[{"field":"value1"},{"field":"value2"}]},{"_error":"Entry path '/*/not_found/*' not found."}]"#)?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
    ///     assert_eq!(expected_data, data);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn read_data(&self, connector: &mut Box<dyn Connector>) -> io::Result<Dataset> {
        let mut buf = Vec::new();
        connector.read_to_end(&mut buf).await?;

        let cursor = SliceableCursor::new(buf);

        let read_from_cursor = SerializedFileReader::new(cursor)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let rows = read_from_cursor
            .get_row_iter(None)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into_iter();

        let entry_path_option = self.entry_path.clone();

        let records: Vec<Value> = rows.map(|row| row.to_json_value()).collect();

        Ok(Box::pin(stream! {
            for record in records {
                match entry_path_option.clone() {
                    Some(entry_path) => {
                        match record.clone().search(entry_path.as_ref()) {
                            Ok(Some(Value::Array(values))) => {
                                for value in values {
                                    yield DataResult::Ok(value);
                                }
                            }
                            Ok(Some(record)) => yield DataResult::Ok(record),
                            Ok(None) => {
                                yield DataResult::Err((
                                    record,
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        format!("Entry path '{}' not found.", entry_path),
                                    ),
                                ))
                            }
                            Err(e) => yield DataResult::Err((record, e)),
                        }
                    },
                    None => {
                        yield DataResult::Ok(record);
                    }
                }
            }
        }))
    }
    /// See [`Document::write_data`] for more details.
    ///
    /// Write data in jsonl format to append data into the buffer.
    ///
    /// # Example
    /// ```rust
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"}"#, &format!("{}", connector));
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///     document.write_data(&mut connector, value).await?;
    ///     assert_eq!(r#"{"column_1":"line_1"},{"column_1":"line_2"}"#, &format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn write_data(&mut self, _connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        self.inner.push(value);

        Ok(())
    }
    /// See [`Document::close`] for more details.
    ///
    /// Read the remote file and append the new data.
    ///
    /// # Example: Remote document don't have data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#""#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"[{"column_1":"line_1"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has empty data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_1"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#"[{"column_1":"line_1"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Remote document has data.
    /// ```rust
    /// use chewdata::connector::{Connector, in_memory::InMemory};
    /// use chewdata::document::json::Json;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Json::default();
    ///     let mut connector = InMemory::new(r#"[{"column_1":"line_1"}]"#);
    ///
    ///     let value: Value = serde_json::from_str(r#"{"column_1":"line_2"}"#)?;
    ///
    ///     document.write_data(&mut connector, value).await?;
    ///     document.close(&mut connector).await?;
    ///     assert_eq!(r#",{"column_1":"line_2"}]"#, format!("{}", connector));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    async fn close(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        let header = self.header(connector).await?;
        let footer = self.footer(connector).await?;

        let values_schema = infer_json_schema_from_iterator(self.inner.clone().into_iter().map(|value| Ok(value.clone())))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let config_schema = self.schema.clone();
        let mut schema_arrow = config_schema.unwrap_or(values_schema).clone();

        // If the footer contain metadata, we will merge the previous schema and the new one
        if PARQUET_MAGIC.len() < footer.len() {
            println!("merge");
            let cursor = SliceableCursor::new(footer);
            let parquet_metadata = parse_metadata(&cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let file_metadata = parquet_metadata.file_metadata();
            let old_schema = parquet_to_arrow_schema(file_metadata.schema_descr(), file_metadata.key_value_metadata())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            schema_arrow = arrow::datatypes::Schema::try_merge(vec![schema_arrow, old_schema])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let cursor_writer = InMemoryWriteableCursor::default();
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(false)
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_encoding(Encoding::PLAIN)
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        let mut writer = ArrowWriter::try_new(cursor_writer.clone(), Arc::new(schema_arrow.clone()), Some(properties))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let decoder = Decoder::new(Arc::new(schema_arrow.clone()), self.inner.len(), None);
        let mut value_iter = self.inner.clone().into_iter().map(|value| Ok(value.clone()));

        while let Ok(Some(batch)) = decoder.next_batch(&mut value_iter) {
            writer.write(&batch.clone())?;
        }

        writer.close()?;

        let mut buf = cursor_writer.data();

        // remove the header from the cursor if the remote document contain data
        if !connector.is_empty().await? {
            println!("append");
            let mut cursor_reader = std::io::Cursor::new(buf.clone());
            buf = Default::default();
            cursor_reader.seek(SeekFrom::Start(header.len() as u64))?;
            cursor_reader.read_to_end(&mut buf)?;
        }

        connector.write_all(&buf).await?;

        self.inner = Default::default();

        Ok(())
    }
    /// See [`Document::header`] for more details.
    async fn header(&self, _connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        Ok(PARQUET_MAGIC.to_vec())
    }
    /// See [`Document::footer`] for more details.
    ///
    /// If the remote document is not empty, try to fetch the footer bytes.
    async fn footer(&self, connector: &mut dyn Connector) -> io::Result<Vec<u8>> {
        if connector.is_empty().await? {
            return Ok(PARQUET_MAGIC.to_vec());
        }

        let file_len = connector.len().await?;
        let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, file_len);
        let last_chunk = connector.chunk(file_len - default_end_len, file_len).await?;

        // Check last_chunk if it's the latest parquet data in the file
        if last_chunk[last_chunk.len() - 4..] != PARQUET_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData,"Invalid Parquet file. Corrupt footer"));
        }

        // Get the metadata length from the footer
        let metadata_len = LittleEndian::read_i32(
            &last_chunk[last_chunk.len() - 8..last_chunk.len() - 4],
        ) as i64;

        if metadata_len < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("Invalid Parquet file. Metadata length is less than zero ({})", metadata_len)
            ));
        }

        // Get the footer size
        let footer_metadata_len = FOOTER_SIZE + metadata_len as usize;
        let footer = last_chunk[last_chunk.len() - footer_metadata_len..].to_vec();

        Ok(footer)
    }
    /// See [`Document::has_data`] for more details.
    fn has_data(&self, str: &str) -> io::Result<bool> {
        Ok(!matches!(str, ""))
    }
}
