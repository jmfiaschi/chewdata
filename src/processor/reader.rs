use super::{Context, Data};
use crate::document_builder::DocumentBuilder;
use crate::processor::Process;
use serde::Deserialize;
use std::{io, fmt};
use genawaiter::sync::GenBoxed;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct Reader {
    #[serde(alias = "builder")]
    document_builder: DocumentBuilder,
    pub alias: Option<String>,
    pub description: Option<String>,
    pub enable: bool,
}

impl Default for Reader {
    fn default() -> Self {
        Reader {
            document_builder: DocumentBuilder::default(),
            alias: None,
            description: None,
            enable: true,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, 
            "Reader {{'{}','{}'}}", 
            self.alias.to_owned().unwrap_or("No alias".to_string()), self.description.to_owned().unwrap_or("No description".to_string())
        )
    }
}

// This Process read data from somewhere
impl Process for Reader {
    fn exec(&self, _input_data: Option<Data>) -> io::Result<Context> {
        info!(slog_scope::logger(), "Exec"; "processor" => format!("{}", self));
        let data = self.document_builder.to_owned().inner().read_data()?;

        let process_cloned = self.to_owned();
        let data = GenBoxed::new_boxed(|co| async move {
            trace!(slog_scope::logger(), "Start generator"; "processor" => format!("{}", process_cloned));
            for data_result in data {
                co.yield_(data_result).await;
            }
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
