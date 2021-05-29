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

use self::step::{StepType, DataResult};
use serde::{Deserialize, Serialize};
use std::io;
use multiqueue::MPMCReceiver;
use async_std::task;

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
    pub delimiter: Option<[u8; 1]>,
    pub quote: Option<[u8; 1]>,
    pub escape: Option<[u8; 1]>,
    pub comment: Option<[u8; 1]>,
    pub terminator: Option<[u8; 2]>,
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
