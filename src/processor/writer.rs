use super::{Context, Data};
use crate::document_builder::DocumentBuilder;
use crate::processor::{DataResult, Process};
use genawaiter::sync::GenBoxed;
use serde::{Deserialize, Serialize};
use std::{fmt, io::Error, io::ErrorKind, io::Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Writer {
    #[serde(alias = "builder")]
    document_builder: DocumentBuilder,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub data_type: String,
    pub enable: bool,
    pub batch_byte_size: usize,
    pub batch_record_size: usize,
}

impl Default for Writer {
    fn default() -> Self {
        Writer {
            document_builder: DocumentBuilder::default(),
            alias: None,
            description: None,
            data_type: DataResult::OK.to_string(),
            enable: true,
            batch_byte_size: 1000000,
            batch_record_size: 1000,
        }
    }
}

impl fmt::Display for Writer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Writer {{'{}','{}'}}",
            self.alias.to_owned().unwrap_or("No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or("No description".to_string())
        )
    }
}

// This Process write data from somewhere into another stream.
impl Process for Writer {
    fn exec(&self, input_data: Option<Data>) -> Result<Context> {
        info!(slog_scope::logger(), "Exec"; "processor" => format!("{}", self));

        let data = input_data.ok_or(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "The writer need data to write, none given. {current_object:?}",
                current_object = [&self.alias]
            ),
        ))?;

        let mut document_builder = self.document_builder.to_owned().inner();
        let data_type = self.data_type.to_owned();
        let process_cloned = self.to_owned();
        let batch_byte_size = self.batch_byte_size;
        let batch_record_size = self.batch_record_size;
        let mut batch_record_size_count = 0;
        let data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator"; "processor" => format!("{}", &process_cloned));
            for data_result in data {
                let json_value = match (data_result.clone(), data_type.as_ref()) {
                    (DataResult::Ok(_), DataResult::OK) => data_result.to_json_value(),
                    (DataResult::Err(_), DataResult::ERR) => data_result.to_json_value(),
                    _ => {
                        info!(slog_scope::logger(),
                            "This processor handle only this data type";
                            "data_type" => &data_type,
                            "data" => format!("{:?}", data_result),
                            "processor" => format!("{}", &process_cloned)
                        );

                        co.yield_(data_result).await;

                        continue;
                    }
                };

                {
                    let mut connector_tmp = document_builder.connector().clone().inner();
                    connector_tmp.set_path_parameters(json_value.clone());
                    let new_path = connector_tmp.path();
                    let current_path = document_builder.connector().clone().inner().path();
                    if current_path != new_path
                        && !document_builder
                            .connector()
                            .clone()
                            .inner()
                            .inner()
                            .is_empty()
                    {
                        info!(slog_scope::logger(), "Document will change"; "current_path"=>current_path,"new_path"=>new_path);
                        match document_builder.flush() {
                            Ok(_) => (),
                            Err(e) => error!(slog_scope::logger(), "Can't flush data. {}", e),
                        };
                    }
                }

                match document_builder.write_data_result(data_result.clone()) {
                    Ok(_) => (),
                    Err(e) => {
                        error!(slog_scope::logger(),
                            "Can't write into the document";
                            "data" => format!("{}", &json_value),
                            "error" => format!("{}", e)
                        );
                        co.yield_(DataResult::Err((json_value.clone(), e))).await;
                        continue;
                    }
                };
                batch_record_size_count += 1;

                if batch_byte_size
                    <= document_builder
                        .connector()
                        .clone()
                        .inner()
                        .len()
                        .map_or(0, |len| len)
                    || batch_record_size <= batch_record_size_count
                {
                    trace!(slog_scope::logger(), "Batch size in byte achieved");
                    match document_builder.flush() {
                        Ok(_) => (),
                        Err(e) => error!(slog_scope::logger(), "Can't flush data. {}", e),
                    };
                    batch_record_size_count = 0;
                }

                co.yield_(data_result).await;
            }

            match document_builder.flush() {
                Ok(_) => (),
                Err(e) => error!(slog_scope::logger(), "Can't flush data. {}", e),
            };
            trace!(slog_scope::logger(), "End generator"; "processor" => format!("{}", process_cloned));
        });

        info!(slog_scope::logger(), "Exec ended"; "processor" => format!("{}", self));
        Ok(Context { data: Some(data) })
    }
    fn is_enable(&self) -> bool {
        self.enable
    }
    fn get_alias(&self) -> Option<String> {
        self.alias.to_owned()
    }
    fn disable(&mut self) {
        self.enable = false;
    }
}
