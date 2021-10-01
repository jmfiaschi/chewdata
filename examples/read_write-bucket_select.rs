use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_env_filter(EnvFilter::from_default_env())
        // build but do not install the subscriber.
        .finish();

    let config = r#"
    [
        {
            "type": "r",
            "connector": {
                "type": "bucket_select",
                "bucket": "my-bucket",
                "path": "data/multi_lines.jsonl",
                "endpoint": "{{ BUCKET_ENDPOINT }}",
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
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

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await
}
