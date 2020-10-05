#[macro_use]
extern crate clap;
extern crate env_applier;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use clap::App;
use env_applier::EnvApply;
use serde_json::Value;
use slog::{Drain, FnValue};
use std::env;
use std::io::{Error, ErrorKind, Result};

const ARG_FORMAT: &'static str = "format";
const ARG_CONFIG: &'static str = "config";

fn main() -> Result<()> {
    // Init logger.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::default(drain);
    let logger = slog::Logger::root(
        drain.fuse(),
        o!("file" => FnValue(move |info| {format!("{}:{}",info.file(),info.line())})),
    );
    let _scope_guard = slog_scope::set_global_logger(logger);
    let _log_guard = slog_stdlog::init().unwrap();

    // Init command line argument parser.
    let yaml = load_yaml!("../config/cli.yml");
    let args = App::from_yaml(yaml).get_matches();

    let format = args.value_of(ARG_FORMAT).ok_or(Error::new(
        ErrorKind::InvalidInput,
        "The parameter 'format' is required.",
    ))?;
    let config = args.value_of(ARG_CONFIG).ok_or(Error::new(
        ErrorKind::InvalidInput,
        "The parameter 'config' is required.",
    ))?;

    let config_resolved = env::Vars::apply(config.to_string());

    let config_json_value: Value = match format {
        "json" => serde_json::from_str(config_resolved.as_ref()).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("{}. {}", e, config_resolved),
            )
        }),
        "yaml" | "yml" => serde_yaml::from_str(config_resolved.as_ref()).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("{}. {}", e, config_resolved),
            )
        }),
        _ => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("This format '{}' is not supported", format),
        )),
    }?;

    chewdata::exec(serde_json::from_str(
        config_json_value.to_string().as_ref(),
    )?)?;

    Ok(())
}
