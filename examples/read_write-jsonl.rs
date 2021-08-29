#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use slog::{Drain, FnValue};
use std::io;

#[async_std::main]
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
        "type": "reader",
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.jsonl"
        },
        "document" :{
            "type":"jsonl"
        }
    },
    {
        "type": "writer",
        "document" : {
            "type": "jsonl"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}
