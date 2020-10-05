use super::{Context, Data, DataResult};
use crate::processor::reader::Reader;
use crate::processor::Process;
use crate::updater::Updater;
use genawaiter::sync::GenBoxed;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fmt, io::Error, io::ErrorKind, io::Result};

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Transformer {
    updater: Updater,
    #[serde(alias = "refs")]
    referentials: Option<Vec<Reader>>,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub enable: bool,
}

impl Default for Transformer {
    fn default() -> Self {
        Transformer {
            updater: Updater::default(),
            referentials: None,
            alias: None,
            description: None,
            enable: true,
        }
    }
}

impl fmt::Display for Transformer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Transformer {{'{}','{}' }}",
            self.alias.to_owned().unwrap_or("No alias".to_string()),
            self.description
                .to_owned()
                .unwrap_or("No description".to_string())
        )
    }
}

/// Return the content of referential documents. that contain a tuple of (referential_alias, referential_document).
/// This method exec all referential readers and cache the result in memory.
fn get_mapping<'a>(referentials: &Vec<Reader>) -> Result<HashMap<String, Vec<Value>>> {
    let mut mapping = HashMap::new();

    // For each reader, try to build the referential.
    for reader in referentials {
        let alias: String = match &reader.alias {
            Some(alias) => Ok(alias.to_string()),
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Alias required for this reader, {}", reader),
            )),
        }?;

        let context = reader.exec(None)?;

        let mut referential_data: Vec<Value> = Vec::new();
        match context.data {
            Some(data) => {
                for data_result in data {
                    let record = match data_result {
                        DataResult::Ok(record) => record,
                        DataResult::Err(_) => continue,
                    };
                    referential_data.push(record);
                }
            }
            None => (),
        }
        mapping.insert(alias, referential_data);
    }

    Ok(mapping)
}

/// This Process transform data
impl Process for Transformer {
    fn exec(&self, input_data: Option<Data>) -> Result<Context> {
        info!(slog_scope::logger(), "Exec"; "processor" => format!("{}", self));
        let input_data = input_data.ok_or(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "The transformer need data in input, none given. {:?}",
                [&self.alias.clone().unwrap_or("No alias".to_string())]
            ),
        ))?;

        let mut mapping = None;
        if let Some(referentials) = &self.referentials {
            mapping = Some(get_mapping(referentials)?);
        }

        let process_cloned = self.to_owned();
        let updater = self.updater.to_owned().get();
        let output_data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator"; "processor" => format!("{}", process_cloned));
            for data_result in input_data {
                let record = match data_result {
                    DataResult::Ok(record) => record,
                    DataResult::Err(_) => continue,
                };

                match updater.update(record.clone(), mapping.clone()) {
                    Ok(new_record) => {
                        info!(slog_scope::logger(), "Record transformation success"; "record" => format!("{}", new_record));

                        // Skip empty records
                        if Value::Null == new_record {
                            info!(slog_scope::logger(), "Record skip because the value si null"; "record" => format!("{}", new_record));

                            continue;
                        }
                        co.yield_(DataResult::Ok(new_record)).await;
                    }
                    Err(e) => {
                        error!(slog_scope::logger(), "Record transformation failed"; "error" => format!("{}",e));
                        co.yield_(DataResult::Err((record.clone(), e))).await;
                    }
                };
            }
            trace!(slog_scope::logger(), "End generator"; "processor" => format!("{}", process_cloned));
        });

        info!(slog_scope::logger(), "Exec ended"; "processor" => format!("{}", self));
        Ok(Context {
            data: Some(output_data),
        })
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
