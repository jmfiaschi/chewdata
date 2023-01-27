use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

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

    let config_resolved = env::Vars::apply(config.to_string());

    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await?;

    Ok(())
}
