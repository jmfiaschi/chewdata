//! Read and Write in Parquet format.
//! this class read the resource with good performence but the writing will take time. It is not possible to parallize the writing with multi threads.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                                         | Default Value  | Possible Values                                                                                                    |
//! | ---------- | ----- | ----------------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------ |
//! | type       | -     | Required in order to use this document.                                             | `parquet`      | `parquet`                                                                                                          |
//! | metadata   | meta  | Metadata describe the resource.                                                     | `null`         | [`crate::Metadata`]                                                                                                |
//! | entry_path | -     | Use this field if you want to target a specific field in the object.                | `/root/*/item` | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901)                                     |
//! | schema     | -     | Schema that override the default schema found on the first entry found.             | `null`         | `"fields":[{"name": "my_field", "type": {"name": "int", "bitWidth": 64, "isSigned": false}, "nullable": false},...]` |
//! | batch_size | -     | Number of items per page.                                                           | `1000`         | unsigned number                                                                                                    |
//! | options    | -     | Parquet options.                                                                    | `null`         | [`crate::document::parquet::ParquetOptions`]                                                                        |
//!
//! examples:
//!
//! ```json
//! [
//!     {
//!         "type": "read"
//!     },
//!     {
//!         "type": "write",
//!         "document":{
//!             "type":"parquet"
//!         }
//!     }
//! ]
//! ```
//!
//! input:
//!
//! ```json
//! [
//!     {"field1":"value1"},
//!     ...
//! ]
//! ```
//!
//! output:
//!
//! You need to use a `parquet-tools` in order to analyse the file.
//!
//! #### ParquetOption
//!
//! | key                  | alias | Description                            | Default Value | Possible Values                                                                                                                                                       |
//! | -------------------- | ----- | -------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
//! | version              | -     | Parquet version.                       | `2`           | `1` / `2`                                                                                                                                                             |
//! | data_page_size_limit | -     | Page size limit.                       | `null`        | unsigned number                                                                                                                                                       |
//! | max_row_group_size   | -     | Max row group size.                    | `null`        | unsigned number                                                                                                                                                       |
//! | created_by           | -     | App/User that the create the resource. | `chewdata`    | String                                                                                                                                                                |
//! | encoding             | -     | Resource encoding.                     | `PLAIN`       | `PLAIN` / `BIT_PACKED` / `PLAIN_DICTIONARY` / `RLE` / `DELTA_BINARY_PACKED` / `DELTA_LENGTH_BYTE_ARRAY` / `DELTA_BYTE_ARRAY` / `RLE_DICTIONARY` / `BYTE_STREAM_SPLIT` |
//! | compression          | -     | Resource compression.                  | `GZIP`        | `GZIP` / `UNCOMPRESSED` / `SNAPPY` / `LZO` / `BROTLI` / `LZ4` / `LZ4_RAW` / `ZSTD`                                                                                    |
//! | compression level    | -     | Level of the compression. The value depend on the type of the compression | `null`        | 0..11                                                                                                                              |
//! | has_dictionary       | -     | Use a dictionary.                      | `null`        | `true` / `false`                                                                                                                                                      |
//! | has_statistics       | -     | Use statistics.                        | `null`        | `true` / `false`                                                                                                                                                      |
//! | max_statistics_size  | -     | Max statistics size.                   | `null`        | unsigned number                                                                                                                                                       |
//!
use crate::document::Document;
use crate::helper::string::DisplayOnlyForDebugging;
use crate::DataResult;
use crate::{DataSet, Metadata};
use arrow_integration_test::{schema_from_json, schema_to_json};
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;
use bytes::Bytes;
use json_value_merge::Merge;
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
    version: Option<usize>,
    data_page_size_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
    max_row_group_size: Option<usize>,
    created_by: Option<String>,
    encoding: Option<String>,
    compression: Option<String>,
    compression_level: Option<usize>,
    has_dictionary: Option<bool>,
    has_statistics: Option<String>,
    max_statistics_size: Option<usize>,
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
            encoding: Some("PLAIN".to_string()),
            compression: Some("GZIP".to_string()),
            compression_level: None,
            has_statistics: None,
            has_dictionary: Some(false),
            max_statistics_size: None,
            max_row_group_size: None,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            version: Some(2),
        }
    }
}

impl Document for Parquet {
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
        let schema = match (&self.schema, dataset.first()) {
            (Some(schema_value_params), Some(data_result)) => {
                let schema_from_data =
                    infer_json_schema_from_iterator(vec![Ok(data_result.to_value())].into_iter())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                let schema_value_from_data = schema_to_json(&schema_from_data);

                // Override the guessed schema by the schema in parameter.
                let mut schema_merged = schema_value_params.clone();
                schema_merged.merge(&schema_value_from_data);
                schema_from_json(&schema_merged)
            }
            (Some(schema_value_params), _) => schema_from_json(schema_value_params),
            (None, Some(data_result)) => {
                // Fetch the first data in order to guess the schema.
                infer_json_schema_from_iterator(vec![Ok(data_result.to_value())].into_iter())
            }
            (_, None) => return Ok(vec![]),
        }
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
            .build_decoder()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let values: Vec<Value> = dataset.iter().map(|data| data.to_value()).collect();
        decoder
            .serialize(&values)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let batch_opt = decoder
            .flush()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let batch = match batch_opt {
            Some(batch) => batch,
            None => return Ok(vec![]),
        };

        let mut properties_builder = WriterProperties::builder();
        properties_builder = properties_builder.set_write_batch_size(self.batch_size);

        if let Some(options) = &self.options {
            if let Some(compression) = &options.compression {
                properties_builder =
                    properties_builder.set_compression(match compression.to_uppercase().as_str() {
                        "UNCOMPRESSED" => Compression::UNCOMPRESSED,
                        "SNAPPY" => Compression::SNAPPY,
                        "GZIP" => Compression::GZIP(match options.compression_level {
                            Some(level) => GzipLevel::try_new(level as u32)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            None => GzipLevel::default(),
                        }),
                        "LZO" => Compression::LZO,
                        "BROTLI" => Compression::BROTLI(match options.compression_level {
                            Some(level) => BrotliLevel::try_new(level as u32)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            None => BrotliLevel::default(),
                        }),
                        "LZ4" => Compression::LZ4,
                        "LZ4_RAW" => Compression::LZ4_RAW,
                        "ZSTD" => Compression::ZSTD(match options.compression_level {
                            Some(level) => ZstdLevel::try_new(level as i32)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            None => ZstdLevel::default(),
                        }),
                        _ => Compression::UNCOMPRESSED,
                    });
            }
            if let Some(by) = &options.created_by {
                properties_builder = properties_builder.set_created_by(by.clone());
            }
            if let Some(limit) = options.data_page_size_limit {
                properties_builder = properties_builder.set_data_page_size_limit(limit);
            }
            if let Some(limit) = options.dictionary_page_size_limit {
                properties_builder = properties_builder.set_dictionary_page_size_limit(limit);
            }
            if let Some(encoding) = &options.encoding {
                properties_builder =
                    properties_builder.set_encoding(match encoding.to_uppercase().as_str() {
                        "BIT_PACKED" => Encoding::BIT_PACKED,
                        "PLAIN" => Encoding::PLAIN,
                        "PLAIN_DICTIONARY" => Encoding::PLAIN_DICTIONARY,
                        "RLE" => Encoding::RLE,
                        "DELTA_BINARY_PACKED" => Encoding::DELTA_BINARY_PACKED,
                        "DELTA_LENGTH_BYTE_ARRAY" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
                        "DELTA_BYTE_ARRAY" => Encoding::DELTA_BYTE_ARRAY,
                        "RLE_DICTIONARY" => Encoding::RLE_DICTIONARY,
                        "BYTE_STREAM_SPLIT" => Encoding::BYTE_STREAM_SPLIT,
                        _ => Encoding::PLAIN,
                    });
            }
            if let Some(has_dictionary) = options.has_dictionary {
                properties_builder = properties_builder.set_dictionary_enabled(has_dictionary);
            }
            if let Some(has_statistics) = &options.has_statistics {
                properties_builder = properties_builder.set_statistics_enabled(
                    match has_statistics.to_uppercase().as_str() {
                        "NONE" => EnabledStatistics::None,
                        "CHUNK" => EnabledStatistics::Chunk,
                        "PAGE" => EnabledStatistics::Page,
                        _ => EnabledStatistics::default(),
                    },
                );
            }
            if let Some(size) = options.max_row_group_size {
                properties_builder = properties_builder.set_max_row_group_size(size);
            }
            if let Some(size) = options.max_statistics_size {
                properties_builder = properties_builder.set_max_statistics_size(size);
            }
            if let Some(version) = options.version {
                properties_builder = properties_builder.set_writer_version(match version {
                    1 => WriterVersion::PARQUET_1_0,
                    2 => WriterVersion::PARQUET_2_0,
                    _ => WriterVersion::PARQUET_1_0,
                });
            }
        }

        let properties = properties_builder.build();
        let mut buffer = Vec::new();

        {
            let mut writer = ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(properties))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            writer.write(&batch)?;

            writer.close()?;
        }

        Ok(buffer)
    }
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
