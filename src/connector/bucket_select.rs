use crate::connector::{resolve_path, Connect};
use regex::Regex;
use rusoto_core::{credential::ChainProvider, Region};
use rusoto_s3::{
    InputSerialization, JSONInput, JSONOutput, OutputSerialization, PutObjectRequest, S3Client,
    SelectObjectContentRequest, S3 as RusotoS3,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct BucketSelect {
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: String,
    bucket: String,
    path: String,
    #[serde(skip)]
    buffer: Cursor<Vec<u8>>,
    parameters: Value,
    // Truncate fetch or not the content of the file in the S3 bucket.
    //  true:   Not fetch the files into the bucket.
    //  false:  Fetch the files into the bucket and add the content.
    truncate: bool,
    #[serde(skip)]
    // State if the path will changed.
    is_path_will_change: bool,
}

impl Default for BucketSelect {
    fn default() -> Self {
        BucketSelect {
            endpoint: None,
            access_key_id: "".to_owned(),
            secret_access_key: "".to_owned(),
            region: Region::default().name().to_owned(),
            bucket: "".to_owned(),
            path: "".to_owned(),
            buffer: Cursor::new(Vec::default()),
            parameters: Value::Null,
            truncate: false,
            is_path_will_change: false,
        }
    }
}

impl Clone for BucketSelect {
    fn clone(&self) -> Self {
        BucketSelect {
            endpoint: self.endpoint.to_owned(),
            access_key_id: self.access_key_id.to_owned(),
            secret_access_key: self.secret_access_key.to_owned(),
            region: self.region.to_owned(),
            bucket: self.bucket.to_owned(),
            path: self.path.to_owned(),
            buffer: Cursor::new(Vec::default()),
            parameters: self.parameters.to_owned(),
            truncate: self.truncate.to_owned(),
            is_path_will_change: self.is_path_will_change.to_owned(),
        }
    }
}

impl BucketSelect {
    fn s3_client(&self) -> S3Client {
        match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
            (Some(access_key_id), Some(secret_access_key)) => {
                env::set_var("AWS_ACCESS_KEY_ID", access_key_id);
                env::set_var("AWS_SECRET_ACCESS_KEY", secret_access_key);
            }
            (_, _) => (),
        }

        let chain_provider = ChainProvider::new();

        S3Client::new_with(
            rusoto_core::request::HttpClient::new().expect("Failed to create HTTP client"),
            chain_provider,
            Region::Custom {
                name: self.region.to_owned(),
                endpoint: match self.endpoint.to_owned() {
                    Some(endpoint) => endpoint,
                    None => format!("https://s3-{}.amazonaws.com", self.region),
                },
            },
        )
    }
    fn is_variable_path(&self) -> bool {
        let reg = Regex::new("\\{\\{[^}]*\\}\\}").unwrap();
        reg.is_match(self.path.as_ref())
    }
    fn path(&self) -> String {
        match (self.is_variable_path(), self.path_parameters()) {
            (true, params) => resolve_path(self.path.clone(), params),
            _ => self.path.clone(),
        }
    }
    fn init_buffer(&mut self) -> Result<()> {
        log::trace!("Init buffer called.");
        let connector = self.clone();
        let s3_client = connector.s3_client();
        let select = SelectObjectContentRequest {
            bucket: connector.bucket.to_owned(),
            key: connector.path().to_owned(),
            expression: "SELECT * FROM S3Object[*]".to_owned(),
            expression_type: "SQL".to_owned(),
            input_serialization: InputSerialization {
                json: Some(JSONInput {
                    type_: Some("LINES".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            output_serialization: OutputSerialization {
                json: Some(JSONOutput {
                    record_delimiter: Some("\n".to_string()),
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = s3_client
            .select_object_content(select)
            .sync()
            .map_err(|e| Error::new(ErrorKind::NotFound, e.to_string()))?;

        let stream = match result.payload {
            Some(records) => Ok(format!("{:?}", records)),
            None => Err(Error::new(
                ErrorKind::NotFound,
                format!(
                    "BucketSelect body not found for this path '{}{}'.",
                    self.bucket, self.path
                ),
            )),
        }?;

        self.buffer.write(stream.as_ref())?;
        // initialize the position of the cursor
        self.buffer.set_position(0);
        log::info!("Init buffer with success.");

        Ok(())
    }
}

impl Connect for BucketSelect {
    fn is_path_will_change(&self) -> bool {
        self.is_path_will_change
    }
    fn path_parameters(&self) -> Value {
        self.parameters.clone()
    }
    fn set_path_parameters(&mut self, parameters: Value) {
        let old_parameters = self.parameters.clone();
        self.parameters = parameters.clone();

        if Value::Null != old_parameters {
            self.is_path_will_change =
                super::resolve_path(self.path.clone(), old_parameters) != self.path();
        }
    }
}

impl Read for BucketSelect {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if 0 == self.buffer.clone().into_inner().len() {
            self.init_buffer()?;
        }

        self.buffer.read(buf)
    }
}

impl Write for BucketSelect {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buffer.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        let mut content_file = Vec::default();
        let path_resolved = self.path();

        // Try to fetch the content of the document if exist in the bucket.
        if false == self.truncate {
            log::info!(
                "Fetch the old data into S3 file '{}' and it will updated.",
                path_resolved
            );
            let mut connector_clone = self.clone();
            connector_clone.buffer.set_position(0);
            match connector_clone.read_to_end(&mut content_file) {
                Ok(_) => (),
                Err(_) => log::info!("this file '{}' actualy not exist.", connector_clone.path()),
            }
        }

        // if the content_file is not empty, append the buffer into the content_file.
        content_file.append(&mut self.buffer.clone().into_inner());

        // initialize the position of the cursor
        self.buffer.set_position(0);

        let s3_client = self.s3_client();
        let put_request = PutObjectRequest {
            bucket: self.bucket.to_owned(),
            key: path_resolved.to_owned(),
            body: Some(content_file.into()),
            ..Default::default()
        };

        match s3_client.put_object(put_request).sync() {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::new(ErrorKind::NotFound, e)),
        }?;

        self.buffer.flush()?;
        self.buffer = Cursor::new(Vec::default());
        log::info!("Data pushed into '{}' with success.", path_resolved);
        Ok(())
    }
}
