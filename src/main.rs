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
use macro_rules_attribute::apply;
use smol_macros::main;

const ARG_JSON: &str = "json";
const ARG_FILE: &str = "file";
const DEFAULT_PROCESSORS: &str = r#"[{"type": "r"},{"type": "w"}]"#;

#[apply(main!)]
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

    #[cfg(feature = "apm")]
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();

    #[cfg(not(feature = "apm"))]
    let layer = tracing_subscriber::fmt::layer()
        .pretty()
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

    trace!("Transform the config in input into steps");
    let steps: Vec<StepType> = match (args.get_one::<String>(ARG_JSON), args.get_one::<String>(ARG_FILE)) {
        (None, Some(file_path)) => match file_path.split('.').collect::<Vec<&str>>().last() {
            Some(v) => match *v {
                    "json"|"hjson" => {
                        let mut file = File::open(file_path)?;
                        let mut buf = String::default();
                        file.read_to_string(&mut buf)?;
                        deser_hjson::from_str(buf.apply_with_prefix(&str::to_uppercase(chewdata::PROJECT_NAME)).as_str())
                            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))
                    },
                    "yaml"|"yml" => {
                        let mut file = File::open(file_path)?;
                        let mut buf = String::default();
                        file.read_to_string(&mut buf)?;
                        let config = buf.apply_with_prefix(&str::to_uppercase(chewdata::PROJECT_NAME));
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
                        format!("The format of the config file '{}' is not handle. Valid config file formats are [json, hjson, yaml]", format),
                    )),
            }
            None => Err(Error::new(
                ErrorKind::NotFound,
                "The format of the config file is not found. Valid config file formats are [json, hjson, yaml]",
            ))
        }
        (Some(json), _) => {
            deser_hjson::from_str(json.apply_with_prefix(&str::to_uppercase(chewdata::PROJECT_NAME)).as_str()).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
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
    Command::new(chewdata::PROJECT_NAME)
        .version(version!())
        .author("Jean-Marc Fiaschi <jm.fiaschi@gmail.com>")
        .about("Light and chainable ETL")
        .arg(
            Arg::new("json")
                .value_name("JSON")
                .help("Init steps with a json/hjson configuration in input")
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
