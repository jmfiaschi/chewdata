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

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "local",
            "path": "./data/out/commoncrawl.{{ metadata.mime_subtype }}"
        },
        "document": {
            "type":"jsonl"
        }
    },{
        "type": "w",
        "connector": {
            "type": "bucket",
            "bucket": "my-bucket",
            "path": "data/out/commoncrawl.{{ metadata.mime_subtype }}",
            "endpoint": "{{ BUCKET_ENDPOINT }}",
            "region": "{{ BUCKET_REGION }}"
        },
        "document": {
            "type":"jsonl"
        }
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await?;

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "bucket_select",
            "bucket": "my-bucket",
            "path": "data/out/commoncrawl.{{ metadata.mime_subtype }}",
            "endpoint": "{{ BUCKET_ENDPOINT }}",
            "region": "{{ BUCKET_REGION }}",
            "query": "select * from s3object where length = '2044'"
        },
        "document" : {
            "type": "jsonl"
        }
    },
    {
        "type": "w",
        "document" : {
            "type": "jsonl"
        }
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
