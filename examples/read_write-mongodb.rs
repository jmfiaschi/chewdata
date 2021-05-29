#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

use env_applier::EnvApply;
use slog::{Drain, FnValue};
use std::env;
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
    [
        {
            "type": "e",
            "connector":{
                "type": "mongo",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata"
            }
        },{
            "type": "r",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines_tmp.json"
            }
        },{
            "type": "t",
            "updater": {
                "type": "tera",
                "actions": [
                    {
                        "field":"/",
                        "pattern": "{{ input | json_encode() }}"
                    },
                    {
                        "field":"new_field_in_mongo",
                        "pattern": "{{ now() }}"
                    }
                ]
            },
            "thread_number":1
        },{
            "type": "w",
            "connector":{
                "type": "mongo",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata",
                "update_options": {
                    "upsert": true
                }
            },
            "thread_number":1
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None).await
}
