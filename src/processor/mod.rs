mod reader;
mod transformer;
mod writer;

use super::processor::reader::Reader;
use super::processor::transformer::Transformer;
use super::processor::writer::Writer;
use genawaiter::sync::GenBoxed;
use json_value_merge::Merge;
use serde::Deserialize;
use serde_json::Value;
use std::io;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Processor {
    #[serde(rename = "reader")]
    #[serde(alias = "r")]
    Reader(Reader),
    #[serde(rename = "writer")]
    #[serde(alias = "w")]
    Writer(Writer),
    #[serde(rename = "transformer")]
    #[serde(alias = "t")]
    Transformer(Transformer),
}

impl Processor {
    pub fn get(self) -> Box<dyn Process> {
        match self {
            Processor::Reader(reader) => Box::new(reader),
            Processor::Writer(writer) => Box::new(writer),
            Processor::Transformer(transformer) => Box::new(transformer),
        }
    }
    pub fn get_mut(&mut self) -> Box<&mut dyn Process> {
        match *self {
            Processor::Reader(ref mut reader) => Box::new(reader),
            Processor::Writer(ref mut writer) => Box::new(writer),
            Processor::Transformer(ref mut transformer) => Box::new(transformer),
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
}

pub type Data = GenBoxed<DataResult>;
pub struct Context {
    pub data: Option<Data>,
}

pub trait Process {
    /// Exec the processor that implement this trait.
    fn exec(&self, input_data: Option<Data>) -> io::Result<Context>;
    fn is_enable(&self) -> bool;
    fn disable(&mut self);
    fn get_alias(&self) -> Option<String>;
}
