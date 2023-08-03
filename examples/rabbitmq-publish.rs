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
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    let config = r#"
    [{
        "type": "generator",
        "size": 100
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
                "pattern": ""
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
        "threads": 3,
        "batch": 1
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());

    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
