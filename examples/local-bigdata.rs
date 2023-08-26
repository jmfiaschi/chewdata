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

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "http://index.commoncrawl.org",
            "path": "/CC-MAIN-2017-04-index?url=https%3A%2F%2Fnews.ycombinator.com%2F*&output=json",
            "method": "get",
            "timeout": null
        },
        "document": {
            "type":"jsonl"
        }
    },{
        "type": "w",
        "connector": {
            "type": "local",
            "path": "./data/out/commoncrawl.{{ metadata.mime_subtype }}"
        },
        "document": {
            "type":"jsonl"
        }
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
