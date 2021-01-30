mod reader;
mod transformer;
mod writer;
mod eraser;

use super::step::reader::Reader;
use super::step::transformer::Transformer;
use super::step::writer::Writer;
use super::step::eraser::Eraser;
use genawaiter::sync::GenBoxed;
use json_value_merge::Merge;
use serde::Deserialize;
use serde_json::Value;
use std::io;
use multiqueue::{MPMCReceiver, MPMCSender};
use async_trait::async_trait;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StepType {
    #[serde(rename = "reader")]
    #[serde(alias = "r")]
    Reader(Reader),
    #[serde(rename = "writer")]
    #[serde(alias = "w")]
    Writer(Writer),
    #[serde(rename = "transformer")]
    #[serde(alias = "t")]
    Transformer(Transformer),
    #[serde(rename = "erase")]
    #[serde(alias = "e")]
    #[serde(alias = "truncate")]
    Eraser(Eraser),
}

impl StepType {
    pub fn step_inner(self) -> Box<dyn Step> {
        match self {
            StepType::Reader(step) => Box::new(step),
            StepType::Writer(step) => Box::new(step),
            StepType::Transformer(step) => Box::new(step),
            StepType::Eraser(step) => Box::new(step),
        }
    }
    pub fn step(&self) -> &dyn Step {
        match self {
            StepType::Reader(ref step) => step,
            StepType::Writer(ref step) => step,
            StepType::Transformer(ref step) => step,
            StepType::Eraser(ref step) => step,
        }
    }
    pub fn step_mut(&mut self) -> &mut dyn Step {
        match self {
            StepType::Reader(ref mut step) => step,
            StepType::Writer(ref mut step) => step,
            StepType::Transformer(ref mut step) => step,
            StepType::Eraser(ref mut step) => step,
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
        match (self, data_type.as_ref()) {
            (DataResult::Ok(_), DataResult::OK) => true,
            (DataResult::Err(_), DataResult::ERR) => true,
            _ => false
        }
    }
}

pub type Data = GenBoxed<DataResult>;
pub type Dataset = GenBoxed<Vec<DataResult>>;
#[async_trait]
pub trait Step: Send + Sync + std::fmt::Debug + std::fmt::Display + StepClone {
    async fn exec(&self, pipe_outbound_option: Option<MPMCReceiver<DataResult>>, pipe_inbound_option: Option<MPMCSender<DataResult>>) -> io::Result<()>;
    fn thread_number(&self) -> i32 {
        1
    }
}

pub trait StepClone {
    fn clone_box(&self) -> Box<dyn Step>;
}

impl<T> StepClone for T
where
    T: 'static + Step + Clone,
{
    fn clone_box(&self) -> Box<dyn Step> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Step> {
    fn clone(&self) -> Box<dyn Step> {
        self.clone_box()
    }
}
