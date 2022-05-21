use crate::connector::Connector;
use crate::document::Document;
use crate::Metadata;
use crate::{DataResult, Dataset};
use arrow::datatypes::Schema;
use arrow::json::reader::{infer_json_schema_from_iterator, Decoder, DecoderOptions};
use async_std::io::prelude::WriteExt;
use async_std::io::ReadExt;
use async_stream::stream;
use async_trait::async_trait;
use json_value_search::Search;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::SliceableCursor;
use parquet::file::writer::InMemoryWriteableCursor;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::sync::Arc;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Parquet {
    #[serde(rename = "metadata")]
    #[serde(alias = "meta")]
    pub metadata: Metadata,
    pub entry_path: Option<String>,
    pub schema: Option<Box<Value>>,
    pub batch_size: usize,
    pub options: Option<ParquetOptions>,
    // List of temporary values used to write into the connector
    #[serde(skip)]
    pub inner: Vec<Value>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ParquetOptions {
    version: Option<usize>,
    data_page_size_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
    max_row_group_size: Option<usize>,
    created_by: Option<String>,
    encoding: Option<String>,
    compression: Option<String>,
    has_dictionary: Option<bool>,
    has_statistics: Option<bool>,
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
            inner: Vec::default(),
        }
    }
}

impl Default for ParquetOptions {
    fn default() -> Self {
        ParquetOptions {
            created_by: Some("chewdata".to_string()),
            encoding: Some("PLAIN".to_string()),
            compression: Some("GZIP".to_string()),
            has_statistics: Some(false),
            has_dictionary: Some(false),
            max_statistics_size: None,
            max_row_group_size: None,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            version: Some(2),
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
    /// See [`Document::can_append`] for more details.
    fn can_append(&self) -> bool {
        false
    }
    /// See [`Document::read_data`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{Connector, local::Local};
    /// use chewdata::document::parquet::Parquet;
    /// use chewdata::document::Document;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut document = Parquet::default();
    ///     let mut connector: Box<dyn Connector> = Box::new(Local::new("./data/multi_lines.parquet".to_string()));
    ///     connector.fetch().await?;
    ///
    ///     let mut dataset = document.read_data(&mut connector).await?;
    ///     let data = dataset.next().await.unwrap().to_value();
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
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

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
    async fn write_data(&mut self, _connector: &mut dyn Connector, value: Value) -> io::Result<()> {
        self.inner.push(value);

        Ok(())
    }
    /// See [`Document::close`] for more details.
    async fn close(&mut self, connector: &mut dyn Connector) -> io::Result<()> {
        let connector_is_empty = connector.is_empty().await?;

        let mut values = Vec::default();

        // In parquet, if the document exist, we need to create another document. A paquet file is immutable.
        if !connector_is_empty && connector.inner().is_empty() {
            connector.fetch().await?;
            let mut buf = Vec::new();
            connector.read_to_end(&mut buf).await?;

            let cursor = SliceableCursor::new(buf);

            let mut records = SerializedFileReader::new(cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                .get_row_iter(None)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
                .into_iter()
                .map(|row| row.to_json_value())
                .collect();

            values.append(&mut records);
            connector.clear();
        }

        values.append(&mut self.inner.clone());

        let mut arrow_value = values.clone().into_iter().map(Ok);

        let schema = match self.schema.clone() {
            Some(value) => Schema::from(&value),
            None => infer_json_schema_from_iterator(arrow_value.clone()),
        }
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let cursor_writer = InMemoryWriteableCursor::default();

        let mut properties_builder = WriterProperties::builder();
        properties_builder = properties_builder.set_write_batch_size(self.batch_size);

        if let Some(options) = &self.options {
            if let Some(compression) = &options.compression {
                properties_builder =
                    properties_builder.set_compression(match compression.to_uppercase().as_str() {
                        "UNCOMPRESSED" => Compression::UNCOMPRESSED,
                        "SNAPPY" => Compression::SNAPPY,
                        "GZIP" => Compression::GZIP,
                        "LZO" => Compression::LZO,
                        "BROTLI" => Compression::BROTLI,
                        "LZ4" => Compression::LZ4,
                        "ZSTD" => Compression::ZSTD,
                        _ => Compression::UNCOMPRESSED,
                    });
            }
            if let Some(by) = &options.created_by {
                properties_builder = properties_builder.set_created_by(by.clone());
            }
            if let Some(limit) = options.data_page_size_limit {
                properties_builder = properties_builder.set_data_pagesize_limit(limit);
            }
            if let Some(limit) = options.dictionary_page_size_limit {
                properties_builder = properties_builder.set_dictionary_pagesize_limit(limit);
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
            if let Some(has_statistics) = options.has_statistics {
                properties_builder = properties_builder.set_statistics_enabled(has_statistics);
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

        {
            let mut writer = ArrowWriter::try_new(
                cursor_writer.clone(),
                Arc::new(schema.clone()),
                Some(properties),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let decoder_options = DecoderOptions::new().with_batch_size(self.batch_size);

            let decoder = Decoder::new(Arc::new(schema.clone()), decoder_options);

            while let Ok(Some(batch)) = decoder.next_batch(&mut arrow_value) {
                writer.write(&batch.clone())?;
            }

            writer.close()?;
        }

        connector.write_all(&cursor_writer.data()).await?;

        self.inner = Default::default();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::local::Local;
    use async_std::prelude::StreamExt;

    use super::*;

    #[async_std::test]
    async fn read_data() {
        let document = Parquet::default();
        let mut connector: Box<dyn Connector> =
            Box::new(Local::new("./data/multi_lines.parquet".to_string()));
        connector.fetch().await.unwrap();
        let mut dataset = document.read_data(&mut connector).await.unwrap();
        let data = dataset.next().await.unwrap().to_value();
        let json_expected_str = r#"{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove"}"#;
        let expected_data: Value = serde_json::from_str(json_expected_str).unwrap();
        assert_eq!(expected_data, data);
    }
    #[async_std::test]
    async fn read_data_in_target_position() {
        let mut document = Parquet::default();
        document.entry_path = Some("/string".to_string());
        let mut connector: Box<dyn Connector> =
            Box::new(Local::new("./data/multi_lines.parquet".to_string()));
        connector.fetch().await.unwrap();
        let mut dataset = document.read_data(&mut connector).await.unwrap();
        let data = dataset.next().await.unwrap().to_value();
        let expected_data = Value::String("value to test".to_string());
        assert_eq!(expected_data, data);
    }
    #[async_std::test]
    async fn read_data_without_finding_entry_path() {
        let mut document = Parquet::default();
        document.entry_path = Some("/not_found".to_string());
        let mut connector: Box<dyn Connector> =
            Box::new(Local::new("./data/multi_lines.parquet".to_string()));
        connector.fetch().await.unwrap();
        let mut dataset = document.read_data(&mut connector).await.unwrap();
        let data = dataset.next().await.unwrap().to_value();
        let expected_data: Value = serde_json::from_str(r#"{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove","_error":"Entry path '/not_found' not found."}"#).unwrap();
        assert_eq!(expected_data, data);
    }
}
