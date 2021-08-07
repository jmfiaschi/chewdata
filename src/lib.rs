#[macro_use]
extern crate slog;
extern crate glob;
extern crate json_value_merge;
extern crate json_value_resolve;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate serde;
extern crate serde_json;
extern crate multiqueue2 as multiqueue;

pub mod connector;
pub mod document;
pub mod helper;
pub mod step;
pub mod updater;

use self::step::StepType;
use serde::{Deserialize, Serialize};
use std::io;
use multiqueue::MPMCReceiver;
use async_std::task;
use std::pin::Pin;
use futures::stream::Stream;
use serde_json::Value;
use json_value_merge::Merge;

pub async fn exec(step_types: Vec<StepType>, mut previous_step_pipe_outbound: Option<MPMCReceiver<DataResult>>) -> io::Result<()> {
    let mut steps = Vec::default();
    let mut handles = Vec::default();
    let step_types_len = step_types.len();

    for (pos, step_type) in step_types.into_iter().enumerate() {
        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        let step = step_type.step_inner().clone();
        let thread_number = step.thread_number();

        let mut pipe_inbound_option = None;   
        if pos != step_types_len-1 {
            pipe_inbound_option = Some(pipe_inbound.clone());
        }

        for _pos in 0..thread_number {
            steps.push((step.clone(), previous_step_pipe_outbound.clone(), pipe_inbound_option.clone()));
        }
        previous_step_pipe_outbound = Some(pipe_outbound);
    }

    for (step, inbound, outbound) in steps {
        handles.push(task::spawn(async move { 
            step.exec(inbound, outbound).await 
        }));
    }

    for result in futures::future::join_all(handles).await {
        result?;
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
    pub mime_subtype: Option<String>,
    pub charset: Option<String>,
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
            mime_subtype: None,
            charset: None,
            compression: None,
        }
    }
}

impl Metadata {
    fn merge(self, metadata: Metadata) -> Metadata {
        Metadata {
            has_headers: metadata.has_headers.or(self.has_headers),
            delimiter: metadata.delimiter.or(self.delimiter),
            quote: metadata.quote.or(self.quote),
            escape: metadata.escape.or(self.escape),
            comment: metadata.comment.or(self.comment),
            terminator: metadata.terminator.or(self.terminator),
            mime_type: metadata.mime_type.or(self.mime_type),
            mime_subtype: metadata.mime_subtype.or(self.mime_subtype),
            charset: metadata.charset.or(self.charset),
            compression: metadata.compression.or(self.compression),
        }
    }
}

#[derive(Debug)]
pub enum DataResult {
    Ok(Value),
    Err((Value, io::Error)),
}

impl Clone for DataResult {
    fn clone(&self) -> Self {
        match self {
            DataResult::Ok(value) => DataResult::Ok(value.clone()),
            DataResult::Err((value, e)) => {
                DataResult::Err((value.clone(), io::Error::new(e.kind(), e.to_string())))
            }
        }
    }
}

impl DataResult {
    pub const OK: &'static str = "ok";
    pub const ERR: &'static str = "err";
    const FIELD_ERROR: &'static str = "_error";

    pub fn to_json_value(&self) -> Value {
        match self {
            DataResult::Ok(value) => value.to_owned(),
            DataResult::Err((value, error)) => {
                let mut json_value = value.to_owned();
                json_value.merge_in(
                    format!("/{}", DataResult::FIELD_ERROR).as_ref(),
                    Value::String(format!("{}", error)),
                );
                json_value
            }
        }
    }
    pub fn is_type(&self, data_type: &str) -> bool {
        matches!((self, data_type), (DataResult::Ok(_), DataResult::OK) | (DataResult::Err(_), DataResult::ERR))
    }
}

pub type Dataset = Pin<Box<dyn Stream<Item = DataResult> + Send>>;
