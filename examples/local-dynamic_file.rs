use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    // init the erase_test file
    let config = r#"
    [
        {"type":"r","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}, "name": "file_ids"},
        { 
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.*"
            }
        },
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.{{ metadata.mime_subtype }}"
            }
        },
        { 
            "type": "writer",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.{{ metadata.mime_subtype }}"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
