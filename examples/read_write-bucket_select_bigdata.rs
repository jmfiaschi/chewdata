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
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "http://index.commoncrawl.org",
            "path": "/CC-MAIN-2017-04-index?url=https%3A%2F%2Fnews.ycombinator.com%2F*&output=json",
            "method": "get"
        },
        "document": {
            "type":"jsonl"
        }
    },{
        "type": "w",
        "connector": {
            "type": "bucket",
            "bucket": "my-bucket",
            "path": "data/commoncrawl.json",
            "endpoint": "{{ BUCKET_ENDPOINT }}",
            "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
            "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
            "region": "{{ BUCKET_REGION }}"
        },
        "document": {
            "type":"jsonl"
        }
    },{
        "type": "r",
        "connector": {
            "type": "bucket_select",
            "bucket": "my-bucket",
            "path": "data/commoncrawl.json",
            "endpoint": "{{ BUCKET_ENDPOINT }}",
            "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
            "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
            "region": "{{ BUCKET_REGION }}",
            "query": "select * from s3object where status = '200'"
        },
        "document" : {
            "type": "json"
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
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await
}
