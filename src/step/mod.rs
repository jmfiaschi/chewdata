mod eraser;
mod reader;
mod transformer;
mod writer;
mod validator;

use super::step::eraser::Eraser;
use super::step::reader::Reader;
use super::step::validator::Validator;
use super::step::transformer::Transformer;
use super::step::writer::Writer;
use crate::{DataResult, StepContext};
use serde::Deserialize;

use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use serde_json::Value;
use std::{io, collections::HashMap};

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StepType {
    #[serde(rename = "reader")]
    #[serde(alias = "read")]
    #[serde(alias = "r")]
    Reader(Reader),
    #[serde(rename = "writer")]
    #[serde(alias = "write")]
    #[serde(alias = "w")]
    Writer(Writer),
    #[serde(rename = "transformer")]
    #[serde(alias = "transform")]
    #[serde(alias = "t")]
    Transformer(Transformer),
    #[serde(rename = "eraser")]
    #[serde(alias = "erase")]
    #[serde(alias = "truncate")]
    #[serde(alias = "e")]
    Eraser(Eraser),
    #[serde(rename = "validator")]
    #[serde(alias = "validate")]
    #[serde(alias = "v")]
    Validator(Validator),
}

impl StepType {
    pub fn step_inner(self) -> Box<dyn Step> {
        match self {
            StepType::Reader(step) => Box::new(step),
            StepType::Writer(step) => Box::new(step),
            StepType::Transformer(step) => Box::new(step),
            StepType::Eraser(step) => Box::new(step),
            StepType::Validator(step) => Box::new(step),
        }
    }
    pub fn step(&self) -> &dyn Step {
        match self {
            StepType::Reader(ref step) => step,
            StepType::Writer(ref step) => step,
            StepType::Transformer(ref step) => step,
            StepType::Eraser(ref step) => step,
            StepType::Validator(ref step) => step,
        }
    }
    pub fn step_mut(&mut self) -> &mut dyn Step {
        match self {
            StepType::Reader(ref mut step) => step,
            StepType::Writer(ref mut step) => step,
            StepType::Transformer(ref mut step) => step,
            StepType::Eraser(ref mut step) => step,
            StepType::Validator(ref mut step) => step,
        }
    }
}

#[async_trait]
pub trait Step: Send + Sync + std::fmt::Debug + std::fmt::Display + StepClone {
    async fn exec(
        &self,
        receiver_option: Option<Receiver<StepContext>>,
        sender_option: Option<Sender<StepContext>>,
    ) -> io::Result<()>;
    fn thread_number(&self) -> usize {
        1
    }
    #[instrument]
    fn send(&self, step_context: StepContext, pipe: &Sender<StepContext>) -> io::Result<()> {
        trace!("Send context to the queue");
        pipe.send(step_context)
            .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e))?;

        Ok(())
    }
    fn alias(&self) -> String {
        "default".to_string()
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

/// Return a referentials hashmap indexed by the alias of the referential.
async fn referentials_reader_into_value(
    referentials: HashMap<String, Reader>,
) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_vec = HashMap::new();

    // For each reader, try to build the referential.
    for (alias, referential) in referentials {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let mut values: Vec<Value> = Vec::new();

        referential.exec(None, Some(sender)).await?;

        for step_context in receiver {
            values.push(step_context.data_result().to_value());
        }
        referentials_vec.insert(alias, values);
    }

    Ok(referentials_vec)
}
