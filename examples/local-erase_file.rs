use std::io;
use std::env;
use env_applier::EnvApply;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    // init the erase_test file
    let config = r#"
    [
        { 
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test.json"
            }
        },
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        { 
            "type": "writer",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test.json"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
