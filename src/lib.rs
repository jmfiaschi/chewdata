extern crate glob;
extern crate json_value_merge;
extern crate json_value_resolve;
extern crate multiqueue2 as multiqueue;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate tracing;

pub mod connector;
pub mod document;
pub mod helper;
pub mod step;
pub mod updater;

use self::step::StepType;
use async_std::task;
use futures::stream::Stream;
use json_value_merge::Merge;
use multiqueue::MPMCReceiver;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::{collections::HashMap, io};
use tracing::Instrument;
use tracing_futures::WithSubscriber;

pub async fn exec(
    step_types: Vec<StepType>,
    mut previous_step_pipe_outbound: Option<MPMCReceiver<DataResult>>,
) -> io::Result<()> {
    let mut steps = Vec::default();
    let mut handles = Vec::default();
    let step_types_len = step_types.len();

    for (pos, step_type) in step_types.into_iter().enumerate() {
        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        let step = step_type.step_inner().clone();
        let thread_number = step.thread_number();

        let mut pipe_inbound_option = None;
        if pos != step_types_len - 1 {
            pipe_inbound_option = Some(pipe_inbound.clone());
        }

        for _pos in 0..thread_number {
            steps.push((
                step.clone(),
                previous_step_pipe_outbound.clone(),
                pipe_inbound_option.clone(),
            ));
        }
        previous_step_pipe_outbound = Some(pipe_outbound);
    }

    for (step, inbound, outbound) in steps {
        handles.push(task::spawn(
            async move { step.exec(inbound, outbound).await }
                .instrument(tracing::info_span!("exec"))
                .with_current_subscriber(),
        ));
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
    pub language: Option<String>,
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
            language: None,
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
            language: metadata.language.or(self.language),
        }
    }
    fn content_type(&self) -> String {
        let mut content_type = String::default();

        if let (Some(mime_type), Some(mime_subtype)) = (&self.mime_type, &self.mime_subtype) {
            content_type = format!("{}/{}", mime_type, mime_subtype);

            if let Some(charset) = &self.charset {
                content_type += &format!("; charset={}", charset);
            }
        }

        content_type
    }
    fn content_language(&self) -> String {
        self.language.clone().unwrap_or_default()
    }
    fn to_hashmap(&self) -> HashMap<String, String> {
        let mut hashmap: HashMap<String, String> = HashMap::default();
        if let Some(has_headers) = self.has_headers {
            hashmap.insert("has_headers".to_string(), has_headers.to_string());
        }
        if let Some(delimiter) = self.delimiter.clone() {
            hashmap.insert("delimiter".to_string(), delimiter);
        }
        if let Some(quote) = self.quote.clone() {
            hashmap.insert("quote".to_string(), quote);
        }
        if let Some(escape) = self.escape.clone() {
            hashmap.insert("escape".to_string(), escape);
        }
        if let Some(comment) = self.comment.clone() {
            hashmap.insert("comment".to_string(), comment);
        }
        if let Some(terminator) = self.terminator.clone() {
            hashmap.insert("terminator".to_string(), terminator);
        }
        if let (Some(_), Some(_)) = (self.mime_type.clone(), self.mime_subtype.clone()) {
            hashmap.insert("content_type".to_string(), self.content_type());
        }
        if let Some(compression) = self.compression.clone() {
            hashmap.insert("compression".to_string(), compression);
        }
        if let Some(language) = self.language.clone() {
            hashmap.insert("Content-Language".to_string(), language);
        }
        hashmap
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
                match json_value {
                    Value::Array(_) => json_value
                        .merge_in(
                            format!("/*/{}", DataResult::FIELD_ERROR).as_ref(),
                            Value::String(format!("{}", error)),
                        )
                        .unwrap(),
                    _ => json_value
                        .merge_in(
                            format!("/{}", DataResult::FIELD_ERROR).as_ref(),
                            Value::String(format!("{}", error)),
                        )
                        .unwrap(),
                }

                json_value
            }
        }
    }
    pub fn is_type(&self, data_type: &str) -> bool {
        matches!(
            (self, data_type),
            (DataResult::Ok(_), DataResult::OK) | (DataResult::Err(_), DataResult::ERR)
        )
    }
}

pub type Dataset = Pin<Box<dyn Stream<Item = DataResult> + Send>>;
