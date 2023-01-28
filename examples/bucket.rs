use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
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
    [
        {
            "type": "e",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}"
            }
        },
        {
            "type": "r",
            "connector": {
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        {
            "type": "t",
            "actions": [
                {
                    "field":"",
                    "pattern": "{{ input | json_encode() }}"
                },
                {
                    "field":"uuid",
                    "pattern": "{{ uuid_v4() }}"
                }
            ]
        },
        {
            "type": "w",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "tags": {
                    "service:writer:owner": "my_team_name",
                    "service:writer:env": "dev",
                    "service:writer:context": "example"
                }
            },
            "document" : {
                "type": "jsonl",
                "is_pretty": true
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await
}
