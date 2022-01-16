pub mod eraser;
pub mod reader;
pub mod transformer;
pub mod validator;
pub mod writer;

use crate::{DataResult, StepContext};
use async_std::{task, stream};
use async_stream::stream;
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender, TryRecvError, TrySendError};
use eraser::Eraser;
use futures::Stream;
use reader::Reader;
use serde::Deserialize;
use std::{io, pin::Pin, time::Duration};
use transformer::Transformer;
use validator::Validator;
use writer::Writer;

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
        &self
    ) -> io::Result<()>;
    fn thread_number(&self) -> usize {
        1
    }
    
    fn name(&self) -> String {
        "default".to_string()
    }
    // It the pipe doesn't contain any data to fetch or no receiver is ready, the step sleep before to retry without blocking the thread.
    fn sleep(&self) -> u64 {
        10
    }
    fn set_receiver(&mut self, receiver: Receiver<StepContext>);
    fn receiver(&self) -> Option<&Receiver<StepContext>>;
    fn set_sender(&mut self, sender: Sender<StepContext>);
    fn sender(&self) -> Option<&Sender<StepContext>>;
}

// Send a step_context through a step and a pipe
async fn send<'step>(step: &'step dyn Step, step_context: &'step StepContext) -> io::Result<()> {
    trace!("Send step context to the queue");

    let sender = match step.sender() {
        Some(sender) => sender,
        None => return Ok(()),
    };

    while let Err(e) = sender.try_send(step_context.clone()) {
        match e {
            TrySendError::Full(_) => {
                trace!(step = format!("{:?}", step).as_str(), sleep = step.sleep(), "The step can't send any data, the pipe is full. It tries later");
                task::sleep(Duration::from_millis(step.sleep())).await;
            }
            TrySendError::Disconnected(_) => return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("The step '{}' has been disconnected from the pipe. the step can't send any data", step.name()),
            )),
        }
    }

    Ok(())
}
// Receive a step_context through a step and a pipe
// It return a stream of step_context
async fn receive<'step>(
    step: &'step dyn Step
) -> io::Result<Pin<Box<dyn Stream<Item = StepContext> + Send + 'step>>> {
    let receiver = match step.receiver() {
        Some(receiver) => receiver,
        None => return Ok(Box::pin(stream::empty::<StepContext>())),
    };
    let sleep_time = step.sleep();
    let stream = Box::pin(stream! {
        loop {
            match receiver.try_recv() {
                Ok(step_context_received) => {
                    yield step_context_received.clone();
                },
                Err(TryRecvError::Empty) => {
                    trace!(step = format!("{:?}", step).as_str(), sleep = step.sleep(), "The pipe is empty. The step tries later");
                    task::sleep(Duration::from_millis(sleep_time)).await;
                    continue;
                },
                Err(TryRecvError::Disconnected) => {
                    break;
                },
            };
        }
    });

    Ok(stream)
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
