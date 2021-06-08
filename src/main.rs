extern crate clap;
extern crate env_applier;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use chewdata::step::StepType;
use clap::{App, Arg};
use env_applier::EnvApply;
use serde::Deserialize;
use slog::{Drain, FnValue};
use std::env;
use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind, Result};

const ARG_JSON: &str = "json";
const ARG_FILE: &str = "file";
const DEFAULT_PROCESSORS: &str = r#"[{"type": "r"},{"type": "w"}]"#;

#[tokio::main]
async fn main() -> Result<()> {
    // Init logger.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::default(drain).fuse();
    let logger = slog::Logger::root(
        drain.fuse(),
        o!("file" => FnValue(move |info| {format!("{}:{}",info.file(),info.line())})),
    );
    let _scope_guard = slog_scope::set_global_logger(logger);

    trace!(slog_scope::logger(), "Chewdata start...");
    let args = application().get_matches();

    if args.value_of("version").is_some() {
        return Ok(());
    }

    trace!(
        slog_scope::logger(),
        "Transform the config in input into steps."
    );
    let steps: Vec<StepType> = match (args.value_of(ARG_JSON), args.value_of(ARG_FILE)) {
        (None, Some(file_path)) => match file_path.split('.').collect::<Vec<&str>>().last() {
            Some(v) => match *v {
                    "json" => {
                        let mut file = File::open(file_path)?;
                        let mut buf = String::default();
                        file.read_to_string(&mut buf)?;
                        serde_json::from_str(env::Vars::apply(buf).as_str())
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))
                    },
                    "yaml"|"yml" => {
                        let mut file = File::open(file_path)?;
                        let mut buf = String::default();
                        file.read_to_string(&mut buf)?;
                        let config = env::Vars::apply(buf);
                        let documents = serde_yaml::Deserializer::from_str(config.as_str());
                        let mut steps = Vec::<StepType>::default();

                        for document in documents {
                            let step: StepType = StepType::deserialize(document)
                                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
                            steps.push(step);
                        }
                        Ok(steps)
                    },
                    format => Err(Error::new(
                        ErrorKind::NotFound,
                        format!("The format of the config file '{}' is not handle. Valid config file formats are [json, yaml].", format),
                    )),
            }
            None => Err(Error::new(
                ErrorKind::NotFound,
                "The format of the config file is not found. Valid config file formats are [json, yaml].",
            ))
        }
        (Some(json), None) => {
            serde_json::from_str(env::Vars::apply(json.to_string()).as_str()).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
        }
        (Some(json), Some(_file)) => {
            serde_json::from_str(env::Vars::apply(json.to_string()).as_str()).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
        }
        _ => serde_json::from_str(DEFAULT_PROCESSORS)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
    }?;

    chewdata::exec(steps, None).await
}

fn application() -> App<'static, 'static> {
    App::new("chewdata")
        .version("1.0")
        .author("Jean-Marc Fiaschi <jm.fiaschi@gmail.com>")
        .about("Simple tool to Extract-Transform-Load")
        .arg(
            Arg::with_name("json")
                .short("j")
                .long("json")
                .value_name("JSON")
                .help("Init steps with a json configuration in input")
                .takes_value(true)
                .required(false)
                .index(1),
        )
        .arg(
            Arg::with_name("file")
                .short("f")
                .long("file")
                .value_name("FILE")
                .help("Init steps with file configuration in input")
                .takes_value(true)
                .required(false),
        )
}
