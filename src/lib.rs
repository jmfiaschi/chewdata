//! This crate is a Rust ETL to Manipulate data everywhere. You can use the program or use the library in your code.
//! 
//! # How/Why to use this ETL ?
//! 
//! You can find the detail of this project in the [repository](https://github.com/jmfiaschi/chewdata).
#![forbid(unsafe_code)]

extern crate glob;
extern crate json_value_merge;
extern crate json_value_resolve;
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
use async_channel::{Receiver, Sender};
use futures::stream::Stream;
use json_value_merge::Merge;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::Result;
use std::pin::Pin;
use std::{collections::HashMap, io};

pub async fn exec(
    step_types: Vec<StepType>,
    input_receiver: Option<Receiver<Context>>,
    output_sender: Option<Sender<Context>>,
) -> io::Result<()> {
    let mut steps = Vec::default();
    let mut handles = Vec::default();
    let step_types_len = step_types.len();
    let mut previous_step_receiver = input_receiver;

    for (pos, step_type) in step_types.into_iter().enumerate() {
        let (sender, receiver) = async_channel::unbounded();
        let mut step = step_type.step_inner().clone();
        let thread_number = step.thread_number();

        let mut sender_option = None;
        if pos != step_types_len - 1 {
            sender_option = Some(sender.clone());
        } else if let Some(external_sender) = &output_sender {
            sender_option = Some(external_sender.clone());
        }

        if let Some(receiver) = previous_step_receiver {
            step.set_receiver(receiver.clone());
        }

        if let Some(sender) = sender_option {
            step.set_sender(sender.clone());
        }

        for _pos in 0..thread_number {
            steps.push(step.clone());
        }
        previous_step_receiver = Some(receiver);
    }

    for step in steps {
        handles.push(task::spawn(
            async move { step.exec().await },
        ));
    }

    for result in futures::future::join_all(handles).await {
        result?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(default, deny_unknown_fields)]
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

impl From<Metadata> for Value {
    fn from(metadata: Metadata) -> Value {
        let mut options = Map::default();
        if let Some(has_headers) = metadata.has_headers {
            options.insert("has_headers".to_string(), Value::Bool(has_headers));
        }
        if let Some(delimiter) = metadata.delimiter.clone() {
            options.insert("delimiter".to_string(), Value::String(delimiter));
        }
        if let Some(quote) = metadata.quote.clone() {
            options.insert("quote".to_string(), Value::String(quote));
        }
        if let Some(escape) = metadata.escape.clone() {
            options.insert("escape".to_string(), Value::String(escape));
        }
        if let Some(comment) = metadata.comment.clone() {
            options.insert("comment".to_string(), Value::String(comment));
        }
        if let Some(compression) = metadata.compression.clone() {
            options.insert("compression".to_string(), Value::String(compression));
        }
        if let Some(mime_type) = metadata.mime_type.clone() {
            options.insert("mime_type".to_string(), Value::String(mime_type));
        }
        if let Some(mime_subtype) = metadata.mime_subtype.clone() {
            options.insert("mime_subtype".to_string(), Value::String(mime_subtype));
        }
        if let Some(charset) = metadata.charset.clone() {
            options.insert("charset".to_string(), Value::String(charset));
        }
        if let Some(language) = metadata.language {
            options.insert("language".to_string(), Value::String(language));
        }

        Value::Object(options)
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

impl PartialEq for DataResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataResult::Ok(value1), DataResult::Ok(value2)) => value1 == value2,
            (DataResult::Err((value1, e1)),DataResult::Err((value2, e2))) => value1 == value2 && e1.to_string() == e2.to_string(),
            (_, _) => false,
        }
    }
}

impl DataResult {
    pub const OK: &'static str = "ok";
    pub const ERR: &'static str = "err";
    const FIELD_ERROR: &'static str = "_error";

    pub fn to_value(&self) -> Value {
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
    pub fn merge(&mut self, data_result: DataResult) {
        let new_json_value = data_result.to_value();

        match self {
            DataResult::Ok(value) => {
                value.merge(new_json_value);
            }
            DataResult::Err((value, _e)) => {
                value.merge(new_json_value);
            }
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Context {
    history: Value,
    data_result: DataResult,
}

impl Context {
    pub fn new(step_name: String, data_result: DataResult) -> Result<Self> {
        let mut map = Map::default();
        map.insert(step_name, data_result.to_value());

        let mut history = Value::default();
        history.merge_in("/steps", Value::Object(map))?;

        Ok(Context {
            history,
            data_result,
        })
    }
    pub fn insert_step_result(&mut self, step_name: String, data_result: DataResult) -> Result<()> {
        let mut map = Map::default();
        map.insert(step_name, data_result.to_value());

        self.history.merge_in("/steps", Value::Object(map))?;
        self.data_result = data_result;

        Ok(())
    }
    pub fn data_result(&self) -> DataResult {
        self.data_result.clone()
    }
    pub fn history(&self) -> Value {
        self.history.clone()
    }
    pub fn to_value(&self) -> Result<Value> {
        let mut value = self.data_result.to_value();
        let history: Value = self.history.clone();
        value.merge_in("steps", history)?;

        Ok(value)
    }
}

pub type DataStream = Pin<Box<dyn Stream<Item = DataResult> + Send>>;
pub type DataSet = Vec<DataResult>;
