use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

use macro_rules_attribute::apply;
use smol_macros::main;

#[apply(main!)]
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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!([10, 20, 30]);

    assert_eq!(
        expected,
        result.clone().search("/*/number")?.unwrap_or_default(),
        "The result not match the expected value"
    );

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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!([10, 20, 30]);

    assert_eq!(
        expected,
        result.clone().search("/*/number")?.unwrap_or_default(),
        "The result not match the expected value"
    );

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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!([10, 20, 30]);

    assert_eq!(
        expected,
        result.clone().search("/*/number")?.unwrap_or_default(),
        "The result not match the expected value"
    );

    Ok(())
}
