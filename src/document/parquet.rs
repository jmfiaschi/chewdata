//! Read and write data in **Parquet** format.
//!
//! This document handler provides **high-performance reading** of Parquet files.
//! However, **writing is slower by design** and **cannot be parallelized**
//! due to the sequential nature of the Parquet writer.
//!
//! ---
//!
//! ## Configuration
//!
//! | Key        | Alias | Description                                                                 | Default Value   | Possible Values                                                                                                      |
//! |------------|-------|-----------------------------------------------------------------------------|-----------------|----------------------------------------------------------------------------------------------------------------------|
//! | `type`     | —     | Required to enable this document type.                                      | `parquet`       | `parquet`                                                                                                            |
//! | `metadata` | `meta`| Metadata describing the resource.                                           | `null`          | [`crate::Metadata`]                                                                                                  |
//! | `entry_path` | —   | Targets a specific field in the input object.                               | `/root/*/item`  | JSON Pointer string ([RFC 6901](https://datatracker.ietf.org/doc/html/rfc6901))                                       |
//! | `schema`   | —     | Overrides the schema inferred from the first entry.                          | `null`          | JSON schema definition (https://github.com/apache/arrow-rs/blob/main/arrow-schema/src/schema.rs)                                                                                               |
//! | `batch_size` | —   | Number of records per page written to Parquet.                              | `1000`          | Unsigned integer                                                                                                      |
//! | `options`  | —     | Advanced Parquet writer options.                                            | `null`          | [`crate::document::parquet::ParquetOptions`]                                                                          |
//!
//! ---
//!
//! ## Example
//!
//! ```json
//! [
//!   {
//!     "type": "read",
//!     "connector":{
//!         "type": "local",
//!         "path": "./data/multi_lines.json"
//!     }
//!   },
//!   {
//!     "type": "write",
//!     "document": {
//!       "type": "parquet",
//!       "schema":{
//!          "fields":[
//!                {
//!                    "name": "number",
//!                    "nullable": false,
//!                    "type": {
//!                        "name": "int",
//!                        "bitWidth": 8,
//!                        "isSigned": false
//!                    }
//!                },
//!                {
//!                    "name": "string",
//!                    "nullable": false,
//!                    "type": {
//!                        "name": "utf8"
//!                    }
//!                },
//!                {
//!                    "name": "boolean",
//!                    "nullable": false,
//!                    "type": {
//!                        "name": "bool"
//!                    }
//!                },
//!                {
//!                    "name": "date",
//!                    "nullable": false,
//!                    "type": {
//!                        "name": "date",
//!                        "unit": "DAY"
//!                    }
//!                }
//!            ]
//!        }
//!     }
//!   }
//! ]
//! ```
//!
//! ## Output
//!
//! The output is a Parquet file.
//! Use tools such as [parquet-tools](https://github.com/hangxie/parquet-tools) to inspect or analyze the generated file.
//!
//! ---
//!
//! ## Parquet Options
//!
//! | Key                    | Alias | Description                                                                 | Default Value | Possible Values                                                                                                               |
//! |------------------------|-------|-----------------------------------------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------|
//! | `version`              | —     | Parquet file format version.                                                | `2`           | `1`, `2`                                                                                                                      |
//! | `data_page_size_limit` | —     | Maximum size of a data page (bytes).                                        | `null`        | Unsigned integer                                                                                                              |
//! | `max_row_group_size`   | —     | Maximum number of rows per row group.                                       | `null`        | Unsigned integer                                                                                                              |
//! | `created_by`           | —     | Application or user that created the file.                                 | `chewdata`    | String                                                                                                                        |
//! | `encoding`             | —     | Encoding used for column data.                                              | `PLAIN`       | `PLAIN`, `PLAIN_DICTIONARY`, `RLE`, `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY`, `RLE_DICTIONARY`, `BYTE_STREAM_SPLIT` |
//! | `compression`          | —     | Compression algorithm.                                                      | `GZIP`        | `GZIP`, `UNCOMPRESSED`, `SNAPPY`, `LZO`, `BROTLI`, `LZ4`, `LZ4_RAW`, `ZSTD`                                                     |
//! | `compression_level`    | —     | Compression level (depends on algorithm).                                   | `null`        | `0..11`                                                                                                                       |
//! | `has_dictionary`       | —     | Enables dictionary encoding.                                                | `null`        | `true`, `false`                                                                                                               |
//! | `has_statistics`       | —     | Enables column statistics.                                                  | `null`        | `true`, `false`                                                                                                               |
//!
//! ---
//!
//! ## Performance Notes
//!
//! - **Reading** is optimized for speed and low memory usage.
//! - **Writing** is sequential and cannot be parallelized.
//! - Increasing `batch_size` may improve throughput at the cost of memory usage.
//! - Compression and encoding choices can significantly impact write performance.
//!

use crate::document::Document;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::DataResult;
use crate::{DataSet, Metadata};
use arrow_integration_test::schema_from_json;
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;
use bytes::Bytes;
use json_value_search::Search;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::sync::Arc;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Parquet {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub entry_path: Option<String>,
    pub schema: Option<Value>,
    pub batch_size: usize,
    pub options: Option<ParquetOptions>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ParquetOptions {
    pub version: Option<usize>,
    pub data_page_size_limit: Option<usize>,
    pub dictionary_page_size_limit: Option<usize>,
    pub max_row_group_size: Option<usize>,
    pub created_by: Option<String>,
    pub encoding: Option<EncodingType>,
    pub compression: Option<CompressionType>,
    pub compression_level: Option<usize>,
    pub has_dictionary: Option<bool>,
    pub has_statistics: Option<StatisticsType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompressionType {
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Zstd,
    Lz4,
    Lz4Raw,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EncodingType {
    Plain,
    PlainDictionary,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RleDictionary,
    ByteStreamSplit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatisticsType {
    None,
    Chunk,
    Page,
}

const DEFAULT_SUBTYPE: &str = "parquet";

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
            batch_size: 1000,
            options: Some(ParquetOptions::default()),
        }
    }
}

impl Default for ParquetOptions {
    fn default() -> Self {
        ParquetOptions {
            created_by: Some("chewdata".to_string()),
            encoding: Some(EncodingType::Plain),
            compression: None,
            compression_level: None,
            has_statistics: Some(StatisticsType::Chunk),
            has_dictionary: Some(false),
            max_row_group_size: Some(128 * 1024 * 1024),
            dictionary_page_size_limit: None,
            data_page_size_limit: Some(1024 * 1024),
            version: Some(2),
        }
    }
}

impl Document for Parquet {
    /// See [`Document::set_metadata`] for more details.
    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata.clone();
    }
    /// See [`Document::metadata`] for more details.
    fn metadata(&self) -> Metadata {
        Parquet::default().metadata.merge(&self.metadata)
    }
    /// See [`Document::set_entry_path`] for more details.
    fn set_entry_path(&mut self, entry_path: String) {
        if entry_path.is_empty() {
            self.entry_path = None;
            return;
        }

        self.entry_path = Some(entry_path);
    }
    /// See [`Document::can_append`] for more details.
    fn can_append(&self) -> bool {
        false
    }
    /// See [`Document::read`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::document::parquet::Parquet;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use std::io::Read;
    ///
    /// let document = Parquet::default();
    /// let mut buffer = Vec::default();
    /// std::fs::OpenOptions::new()
    ///     .read(true)
    ///     .write(false)
    ///     .create(false)
    ///     .append(false)
    ///     .truncate(false)
    ///     .open("./data/multi_lines.parquet").unwrap()
    ///     .read_to_end(&mut buffer).unwrap();
    /// let mut dataset = document.read(&buffer).unwrap().into_iter();
    /// let data = dataset.next().unwrap().to_value();
    /// let json_expected_str = r#"{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove"}"#;
    /// let expected_data: Value = serde_json::from_str(json_expected_str).unwrap();
    /// assert_eq!(expected_data, data);
    /// ```
    #[instrument(skip(buffer), name = "parquet::read")]
    fn read(&self, buffer: &[u8]) -> io::Result<DataSet> {
        let mut dataset = Vec::default();
        let bytes = Bytes::copy_from_slice(buffer);
        let read_from_cursor = SerializedFileReader::new(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let rows = read_from_cursor
            .get_row_iter(None)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        for row in rows {
            let record = row
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                .to_json_value();
            match &self.entry_path {
                Some(entry_path) => match record.clone().search(entry_path)? {
                    Some(Value::Array(records)) => {
                        for record in records {
                            trace!(
                                record = record.display_only_for_debugging(),
                                "Record deserialized"
                            );
                            dataset.push(DataResult::Ok(record));
                        }
                    }
                    Some(record) => {
                        trace!(
                            record = record.display_only_for_debugging(),
                            "Record deserialized"
                        );
                        dataset.push(DataResult::Ok(record));
                    }
                    None => {
                        warn!(
                            entry_path = format!("{:?}", entry_path).as_str(),
                            record = record.display_only_for_debugging(),
                            "Entry path not found"
                        );
                        dataset.push(DataResult::Err((
                            record,
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Entry path '{}' not found", entry_path),
                            ),
                        )));
                    }
                },
                None => {
                    trace!(
                        record = record.display_only_for_debugging(),
                        "Record deserialized"
                    );
                    dataset.push(DataResult::Ok(record));
                }
            }
        }

        Ok(dataset)
    }
    /// See [`Document::write`] for more details.
    #[instrument(skip(dataset), name = "parquet::write")]
    fn write(&self, dataset: &DataSet) -> io::Result<Vec<u8>> {
        let Some(first) = dataset.first() else {
            return Ok(vec![]);
        };

        let schema = match &self.schema {
            Some(schema) => schema_from_json(schema),
            None => infer_json_schema_from_iterator(std::iter::once(Ok(first.to_value()))),
        }
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
            .build_decoder()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        for chunk in dataset.chunks(self.batch_size) {
            let values: Vec<Value> = chunk.iter().map(|d| d.to_value()).collect();

            decoder
                .serialize(&values)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let Some(batch) = decoder
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))?
        else {
            return Ok(vec![]);
        };

        let properties = build_writer_properties(self.batch_size, self.options.as_ref())?;

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        Ok(buffer)
    }
}

fn build_compression(options: &ParquetOptions) -> io::Result<Compression> {
    Ok(match options.compression {
        Some(CompressionType::Snappy) => Compression::SNAPPY,
        Some(CompressionType::Gzip) => Compression::GZIP(
            options
                .compression_level
                .map(|l| GzipLevel::try_new(l as u32))
                .transpose()?
                .unwrap_or_default(),
        ),
        Some(CompressionType::Brotli) => Compression::BROTLI(
            options
                .compression_level
                .map(|l| BrotliLevel::try_new(l as u32))
                .transpose()?
                .unwrap_or_default(),
        ),
        Some(CompressionType::Zstd) => Compression::ZSTD(
            options
                .compression_level
                .map(|l| ZstdLevel::try_new(l as i32))
                .transpose()?
                .unwrap_or_default(),
        ),
        Some(CompressionType::Lz4) => Compression::LZ4,
        Some(CompressionType::Lz4Raw) => Compression::LZ4_RAW,
        _ => Compression::UNCOMPRESSED,
    })
}

fn build_encoding(encoding: &EncodingType) -> Encoding {
    match encoding {
        EncodingType::Plain => Encoding::PLAIN,
        EncodingType::PlainDictionary => Encoding::PLAIN_DICTIONARY,
        EncodingType::Rle => Encoding::RLE,
        EncodingType::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
        EncodingType::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        EncodingType::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
        EncodingType::RleDictionary => Encoding::RLE_DICTIONARY,
        EncodingType::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
    }
}

fn build_writer_properties(
    batch_size: usize,
    options: Option<&ParquetOptions>,
) -> io::Result<WriterProperties> {
    let mut builder = WriterProperties::builder().set_write_batch_size(batch_size);

    let Some(options) = options else {
        return Ok(builder.build());
    };

    builder = builder.set_compression(build_compression(options)?);

    if let Some(by) = &options.created_by {
        builder = builder.set_created_by(by.clone());
    }
    if let Some(limit) = options.data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    if let Some(limit) = options.dictionary_page_size_limit {
        builder = builder.set_dictionary_page_size_limit(limit);
    }
    if let Some(encoding) = &options.encoding {
        builder = builder.set_encoding(build_encoding(encoding));
    }
    if let Some(enabled) = options.has_dictionary {
        builder = builder.set_dictionary_enabled(enabled);
    }
    if let Some(stats) = &options.has_statistics {
        builder = builder.set_statistics_enabled(match stats {
            StatisticsType::None => EnabledStatistics::None,
            StatisticsType::Chunk => EnabledStatistics::Chunk,
            StatisticsType::Page => EnabledStatistics::Page,
        });
    }
    if let Some(size) = options.max_row_group_size {
        builder = builder.set_max_row_group_size(size);
    }
    if let Some(version) = options.version {
        builder = builder.set_writer_version(match version {
            2 => WriterVersion::PARQUET_2_0,
            _ => WriterVersion::PARQUET_1_0,
        });
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn read_data() {
        let document = Parquet::default();
        let mut buffer = Vec::default();
        std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open("./data/multi_lines.parquet")
            .unwrap()
            .read_to_end(&mut buffer)
            .unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let json_expected_str = r#"{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove"}"#;
        let expected_data: Value = serde_json::from_str(json_expected_str).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_data_in_target_position() {
        let mut document = Parquet::default();
        document.entry_path = Some("/string".to_string());
        let mut buffer = Vec::default();
        std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open("./data/multi_lines.parquet")
            .unwrap()
            .read_to_end(&mut buffer)
            .unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data = Value::String("value to test".to_string());
        assert_eq!(expected_data, data);
    }
    #[test]
    fn read_data_without_finding_entry_path() {
        let mut document = Parquet::default();
        document.entry_path = Some("/not_found".to_string());
        let mut buffer = Vec::default();
        std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .append(false)
            .truncate(false)
            .open("./data/multi_lines.parquet")
            .unwrap()
            .read_to_end(&mut buffer)
            .unwrap();
        let mut dataset = document.read(&buffer).unwrap().into_iter();
        let data = dataset.next().unwrap().to_value();
        let expected_data: Value = serde_json::from_str(r#"{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove","_error":"Entry path '/not_found' not found"}"#).unwrap();
        assert_eq!(expected_data, data);
    }
    #[test]
    fn write() {
        let document = Parquet::default();
        let dataset = vec![
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_1"}"#).unwrap()),
            DataResult::Ok(serde_json::from_str(r#"{"column_1":"line_2"}"#).unwrap()),
        ];
        let buffer = document.write(&dataset).unwrap();
        assert!(0 < buffer.len(), "The buffer size must be upper than 0");
    }
}
