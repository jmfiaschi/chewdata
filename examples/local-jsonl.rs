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

    let config = r#"
    [{
        "type": "reader",
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.{{ metadata.mime_subtype }}"
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

    chewdata::exec(serde_json::from_str(config)?, None, None).await
}
