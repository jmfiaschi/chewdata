#[cfg(not(feature = "curl"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the curl feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features curl".into());
}

use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;
use std::time::Duration;

#[cfg(feature = "curl")]
#[apply(main!)]
async fn main() -> io::Result<()> {
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::{self, Layer};

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

    run().await
}

#[cfg(feature = "curl")]
async fn publish() -> io::Result<()> {
    // Config the exchange
    {
        let config = r#"
        [{
            "type": "generator",
            "size": 1
        },
        {
            "type": "t",
            "actions": [
                {
                    "field":"type",
                    "pattern": "topic"
                },
                {
                    "field":"durable",
                    "pattern": "true"
                },
                {
                    "field":"auto_delete",
                    "pattern": "false"
                },
                {
                    "field":"internal",
                    "pattern": "false"
                }
            ]
        },
        {
            "type": "writer",
            "connector": {
                "type": "curl",
                "endpoint": "{{ RABBITMQ_ENDPOINT }}",
                "path": "/api/exchanges/%2f/users.event",
                "method": "put",
                "auth": {
                    "type": "basic",
                    "user":"{{ RABBITMQ_USERNAME }}",
                    "pass": "{{ RABBITMQ_PASSWORD }}"
                }
            },
            "document": {
                "type": "jsonl"
            },
            "concurrency_limit": 3,
            "batch": 1
        }]
    "#;

        chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;
    }

    // Config the queue
    {
        let config = r#"
        [{
            "type": "generator",
            "size": 1
        },
        {
            "type": "t",
            "actions": [
                {
                    "field":"auto_delete",
                    "pattern": "false"
                }
            ]
        },
        {
            "type": "writer",
            "connector": {
                "type": "curl",
                "endpoint": "{{ RABBITMQ_ENDPOINT }}",
                "path": "/api/queues/%2f/users.created",
                "method": "put",
                "auth": {
                    "type": "basic",
                    "user":"{{ RABBITMQ_USERNAME }}",
                    "pass": "{{ RABBITMQ_PASSWORD }}"
                }
            },
            "document": {
                "type": "jsonl"
            },
            "concurrency_limit": 3,
            "batch": 1
        }]"#;

        chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;
    }

    // bind exchange & Q
    {
        let config = r#"
        [{
            "type": "generator",
            "size": 1
        },
        {
            "type": "t",
            "actions": [
                {
                    "field":"routing_key",
                    "pattern": "user.create"
                }
            ]
        },
        {
            "type": "writer",
            "connector": {
                "type": "curl",
                "endpoint": "{{ RABBITMQ_ENDPOINT }}",
                "path": "/api/bindings/%2f/e/users.event/q/users.created",
                "method": "post",
                "auth": {
                    "type": "basic",
                    "user":"{{ RABBITMQ_USERNAME }}",
                    "pass": "{{ RABBITMQ_PASSWORD }}"
                }
            },
            "document": {
                "type": "jsonl"
            },
            "concurrency_limit": 3,
            "batch": 1
        }]"#;

        chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;
    }

    let config = r#"
    [{
        "type": "generator",
        "size": 10
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"payload.firstname",
                "pattern": "{{ fake_first_name() }}"
            },
            {
                "field":"payload.lastname",
                "pattern": "{{ fake_last_name() }}"
            },
            {
                "field":"payload.id",
                "pattern": "{{ uuid_v4() }}"
            },
            {
                "field":"payload.event_type",
                "pattern": "create"
            },
            {
                "field":"payload",
                "pattern": "{{ output.payload | json_encode() | base64_encode() }}"
            },
            {
                "field":"payload_encoding",
                "pattern": "base64"
            },
            {
                "field":"properties",
                "pattern": "{\"content_type\":\"application/json\"}"
            },
            {
                "field":"routing_key",
                "pattern": "user.create"
            }
        ]
    },
    {
        "type": "writer",
        "connector": {
            "type": "curl",
            "endpoint": "{{ RABBITMQ_ENDPOINT }}",
            "path": "/api/exchanges/%2f/users.event/publish",
            "method": "post",
            "auth": {
                "type": "basic",
                "user":"{{ RABBITMQ_USERNAME }}",
                "pass": "{{ RABBITMQ_PASSWORD }}"
            }
        },
        "document": {
            "type": "jsonl"
        },
        "concurrency_limit": 3,
        "batch": 1
    }]
    "#;

    // Test example with validation rules
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

    assert!(
        0 == result
            .clone()
            .search("/*/_error")?
            .unwrap_or_default()
            .as_array()
            .unwrap_or(&vec![])
            .len(),
        "There should be 0 '_error' in the result.\n{}",
        result
    );

    Ok(())
}

#[cfg(feature = "curl")]
async fn consume() -> io::Result<()> {
    let config = r#"
    [{
        "type": "generator",
        "size": 1
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"count",
                "pattern": "1"
            },
            {
                "field":"ackmode",
                "pattern": "ack_requeue_false"
            },
            {
                "field":"encoding",
                "pattern": "base64"
            }
        ]
    },
    {
        "type": "reader",
        "connector": {
            "type": "curl",
            "endpoint": "{{ RABBITMQ_ENDPOINT }}",
            "path": "/api/queues/%2f/users.created/get?next={{ paginator.skip }}",
            "method": "post",
            "auth": {
                "type": "basic",
                "user":"{{ RABBITMQ_USERNAME }}",
                "pass": "{{ RABBITMQ_PASSWORD }}"
            },
            "paginator": {
                "type": "offset",
                "skip": 0,
                "limit": 1
            },
            "counter": {
                "type": "body",
                "entry_path": "/messages",
                "path": "/api/queues/%2f/users.created",
                "method": "get"
            }
        },
        "document": {
            "type": "jsonl",
            "metadata": {
                "mime_subtype": "json"
            },
            "entry_path":"/payload$"
        }
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"payload",
                "pattern": "{{ input | base64_decode() }}"
            }
        ]
    },
    {
        "type": "writer",
        "document": {
            "type": "jsonl",
            "is_pretty": true
        }
    }]
    "#;

    // Test example with validation rules
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    assert!(
        0 < receiver_output.recv().await.into_iter().count(),
        "There should find some message."
    );

    Ok(())
}

#[cfg(feature = "curl")]
async fn run() -> io::Result<()> {
    publish().await?;
    smol::Timer::after(Duration::from_secs(5)).await;
    consume().await?;
    Ok(())
}

#[cfg(feature = "curl")]
#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_example() {
        run().await.unwrap();
    }
}
