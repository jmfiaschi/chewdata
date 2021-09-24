#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use slog::{Drain, FnValue};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
    let drain = slog_async::Async::default(drain).fuse();
    let logger = slog::Logger::root(
        drain.fuse(),
        o!("file" => FnValue(move |info| {format!("{}:{}",info.file(),info.line())})),
    );
    let _scope_guard = slog_scope::set_global_logger(logger);

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "local",
            "path": "./data/one_line.json"
        }
    },
    {
        "type": "t",
        "alias": "transform",
        "description": "Create a new identifier 'new_id'",
        "data_type": "ok",
        "wait": 100,
        "threads": 3,
        "actions": [
            {
                "field":"/",
                "pattern": "{{ my_input | json_encode() }}"
            },
            {
                "field":"new_id",
                "pattern": "{{ alias_mapping[1].number * my_input.number * my_output.number }}"
            }
        ],
        "referentials":{
            "alias_mapping": {
                "connector": {
                    "type": "local",
                    "path": "./data/multi_lines.json"
                }
            }
        },
        "input": "my_input",
        "output": "my_output"
    },
    {
        "type": "w"
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}