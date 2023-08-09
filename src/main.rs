extern crate clap;
extern crate env_applier;
#[macro_use]
extern crate version;

use chewdata::step::StepType;
use clap::{Arg, Command};
use env_applier::EnvApply;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::{Error, ErrorKind, Result};
use tracing::*;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const ARG_JSON: &str = "json";
const ARG_FILE: &str = "file";
const DEFAULT_PROCESSORS: &str = r#"[{"type": "r"},{"type": "w"}]"#;

#[async_std::main]
async fn main() -> Result<()> {
    let mut layers = Vec::new();

    // Install a new OpenTelemetry trace pipeline
    #[cfg(feature = "apm")]
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("chewdata")
        .install_simple()
        .unwrap();

    // Create new layer for opentelemetry
    #[cfg(feature = "apm")]
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer).boxed();
    #[cfg(feature = "apm")]
    layers.push(telemetry);

    // Create new layer for stdout logs
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    trace!("Chewdata start...");
    let args = application().get_matches();

    if args.get_one::<String>("version").is_some() {
        return Ok(());
    }

    trace!("Transform the config in input into steps.");
    let steps: Vec<StepType> = match (args.get_one::<String>(ARG_JSON), args.get_one::<String>(ARG_FILE)) {
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

    chewdata::exec(steps, None, None).await?;

    // Shutdown trace pipeline
    #[cfg(feature = "apm")]
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}

fn application() -> Command {
    Command::new("chewdata")
        .version(version!())
        .author("Jean-Marc Fiaschi <jm.fiaschi@gmail.com>")
        .about("Light and chainable ETL")
        .arg(
            Arg::new("json")
                .value_name("JSON")
                .help("Init steps with a json configuration in input")
                .number_of_values(1)
                .required(false)
                .index(1),
        )
        .arg(
            Arg::new("file")
                .short('f')
                .long("file")
                .value_name("FILE")
                .help("Init steps with file configuration in input")
                .number_of_values(1)
                .required(false),
        )
}
