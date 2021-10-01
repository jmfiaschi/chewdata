use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_env_filter(EnvFilter::from_default_env())
        // build but do not install the subscriber.
        .finish();

    let config = r#"
    [
        {
            "type": "e",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
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
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
                "region": "{{ BUCKET_REGION }}",
                "tags": {
                    "service:writer:owner": "my_team_name",
                    "service:writer:env": "dev",
                    "service:writer:context": "example"
                }
            },
            "document" : {
                "type": "jsonl",
                "pretty": true
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await
}
