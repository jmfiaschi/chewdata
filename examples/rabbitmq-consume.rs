use env_applier::EnvApply;
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
                "pattern": "10"
            },
            {
                "field":"ackmode",
                "pattern": "ack_requeue_false"
            },
            {
                "field":"encoding",
                "pattern": "base64"
            },
            {
                "field":"truncate",
                "pattern": "5000"
            }
        ]
    },
    {
        "type": "reader",
        "connector": {
            "type": "curl",
            "endpoint": "{{ RABBITMQ_ENDPOINT }}",
            "path": "/api/queues/%2f/users.events/get?page={{ paginator.skip }}&page_size={{ paginator.limit }}",
            "method": "post",
            "auth": {
                "type": "basic",
                "user":"{{ RABBITMQ_USERNAME }}",
                "pass": "{{ RABBITMQ_PASSWORD }}"
            },
            "paginator": {
                "type": "offset",
                "skip": 1,
                "limit": 10
            }
        },
        "document": {
            "type": "jsonl"
        }
    },
    {
        "type": "writer",
        "document": {
            "type": "jsonl",
            "is_pretty": true
        }
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"payload",
                "pattern": "{{ input.payload | base64_decode() }}"
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

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
