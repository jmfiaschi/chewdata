use env_applier::EnvApply;
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

    tracing::info!("---BucketSelect with Jsonl---");

    let config = r#"
    [
        {
            "type": "r",
            "connector": {
                "type": "bucket_select",
                "bucket": "my-bucket",
                "path": "data/multi_lines.jsonl",
                "endpoint": "{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "query": "select * from s3object"
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
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;

    tracing::info!("---BucketSelect with Json---");

    let config = r#"
    [
        {
            "type": "r",
            "connector": {
                "type": "bucket_select",
                "bucket": "my-bucket",
                "path": "data/multi_lines.{{ metadata.mime_subtype }}",
                "endpoint": "{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "query": "select * from s3object[*]._1"
            }
        },
        {
            "type": "w",
            "document" : {
                "type": "jsonl"
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;
    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;

    tracing::info!("---BucketSelect with Csv---");

    let config = r#"
    [
        {
            "type": "r",
            "connector": {
                "type": "bucket_select",
                "bucket": "my-bucket",
                "path": "data/multi_lines.{{ metadata.mime_subtype }}",
                "endpoint": "{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "query": "select * from s3object"
            },
            "document" : {
                "type": "csv"
            }
        },
        {
            "type": "w",
            "document" : {
                "type": "jsonl"
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
