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

pub fn exec_with_pipe(step_types: Vec<StepType>, mut previous_step_pipe_outbound: Option<MPMCReceiver<DataResult>>) -> io::Result<()> {
    let mut handles = vec![];
    let step_types_len = step_types.len();

    for (pos, step_type) in step_types.into_iter().enumerate() {
        let (pipe_inbound, pipe_outbound) = multiqueue::mpmc_queue(1000);
        let step = step_type.step_inner();

        let mut pipe_inbound_option = None;
            
        if pos != step_types_len-1 {
            pipe_inbound_option = Some(pipe_inbound.clone());
        }

        step.par_exec(&mut handles, previous_step_pipe_outbound, pipe_inbound_option);

        previous_step_pipe_outbound = Some(pipe_outbound);   
    }

    handles.into_iter().for_each(|handle| {
        handle.join().unwrap();
    });

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
            compression: None,
        }
    }
}
