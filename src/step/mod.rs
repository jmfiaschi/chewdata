pub mod eraser;
pub mod reader;
pub mod transformer;
pub mod validator;
pub mod writer;

use crate::{DataResult, StepContext};
use eraser::Eraser;
use reader::Reader;
use serde::Deserialize;
use transformer::Transformer;
use validator::Validator;
use writer::Writer;

use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use std::io;

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
    fn name(&self) -> String {
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
