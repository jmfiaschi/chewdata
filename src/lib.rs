#[macro_use]
extern crate slog;
extern crate glob;
extern crate json_value_merge;
extern crate json_value_resolve;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate serde;
extern crate serde_json;

pub mod connector;
pub mod document;
pub mod helper;
pub mod step;
pub mod updater;

use self::step::{Dataset, StepType};
use serde::{Deserialize, Serialize};
use std::io;

pub fn exec(steps: Vec<StepType>, dataset_opt: Option<Dataset>) -> io::Result<()> {
    match steps.len() {
        0 => {
            if let Some(data) = dataset_opt {
                for _data_result in data {}
            }
            return Ok(());
        }
        _ => {
            let mut steps = steps;
            match steps.remove(0).step().exec(dataset_opt)? {
                Some(data) => exec(steps, Some(data))?,
                None => exec(steps, None)?,
            };
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct Metadata {
    pub has_headers: Option<bool>,
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub comment: Option<String>,
    pub terminator: Option<String>,
    pub mime_type: Option<String>,
    pub compression: Option<String>,
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            has_headers: None,
            delimiter: None,
            quote: None,
            escape: None,
            comment: None,
            terminator: None,
            mime_type: None,
            compression: None,
        }
    }
}
