//! A step is a simple action.
pub mod eraser;
pub mod generator;
pub mod reader;
pub mod referential;
pub mod transformer;
pub mod validator;
pub mod writer;

use crate::helper::string::DisplayOnlyForDebugging;
use crate::{Context, DataResult};
use async_channel::{Receiver, Sender};
use async_std::stream;
use async_stream::stream;
use async_trait::async_trait;
use eraser::Eraser;
use futures::Stream;
use reader::Reader;
use serde::Deserialize;
use std::{io, pin::Pin};
use transformer::Transformer;
use validator::Validator;
use writer::Writer;

use self::generator::Generator;

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
    #[serde(rename = "generator")]
    #[serde(alias = "g")]
    Generator(Generator),
}

impl StepType {
    pub fn step_inner(self) -> Box<dyn Step> {
        match self {
            StepType::Reader(step) => Box::new(step),
            StepType::Writer(step) => Box::new(step),
            StepType::Transformer(step) => Box::new(step),
            StepType::Eraser(step) => Box::new(step),
            StepType::Validator(step) => Box::new(step),
            StepType::Generator(step) => Box::new(step),
        }
    }
    pub fn step(&self) -> &dyn Step {
        match self {
            StepType::Reader(ref step) => step,
            StepType::Writer(ref step) => step,
            StepType::Transformer(ref step) => step,
            StepType::Eraser(ref step) => step,
            StepType::Validator(ref step) => step,
            StepType::Generator(ref step) => step,
        }
    }
    pub fn step_mut(&mut self) -> &mut dyn Step {
        match self {
            StepType::Reader(ref mut step) => step,
            StepType::Writer(ref mut step) => step,
            StepType::Transformer(ref mut step) => step,
            StepType::Eraser(ref mut step) => step,
            StepType::Validator(ref mut step) => step,
            StepType::Generator(ref mut step) => step,
        }
    }
}

#[async_trait]
pub trait Step: Send + Sync + StepClone {
    async fn exec(&self) -> io::Result<()>;
    fn number(&self) -> usize {
        1
    }
    fn name(&self) -> String {
        "default".to_string()
    }
    fn set_receiver(&mut self, receiver: Receiver<Context>);
    fn receiver(&self) -> Option<&Receiver<Context>>;
    fn set_sender(&mut self, sender: Sender<Context>);
    fn sender(&self) -> Option<&Sender<Context>>;
    async fn send(&self, context: &Context) {
        if let Some(sender) = self.sender() {
            send(sender, context).await
        }
    }
    async fn receive<'step>(&'step self) -> Pin<Box<dyn Stream<Item = Context> + Send + 'step>> {
        match self.receiver() {
            Some(receiver) => receive(receiver).await,
            None => Box::pin(stream::empty::<Context>()),
        }
    }
}

pub(crate) async fn send(sender: &Sender<Context>, context: &Context) {
    match sender.send(context.clone()).await {
        Ok(_) => {
            trace!(
                context = context.display_only_for_debugging(),
                "Context sended in the channel"
            )
        }
        Err(e) => {
            trace!(
                error = format!("{:?}", e).as_str(),
                "The channel is disconnected. the step can't send any context",
            );
        }
    }
}

pub(crate) async fn receive<'step>(
    receiver: &'step Receiver<Context>,
) -> Pin<Box<dyn Stream<Item = Context> + Send + 'step>> {
    Box::pin(stream! {
        loop {
            match receiver.recv().await {
                Ok(context_received) => {
                    trace!(
                        context = context_received.display_only_for_debugging(),
                        "A new context received from the channel"
                    );

                    yield context_received;
                },
                Err(_) => {
                    trace!("The channel is disconnected. the step can't receive any context");
                    break;
                }
            };
        }
    })
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
