use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [
        {
            "type": "r",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/*.json*",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "limit": 10,
                "skip": 0
            }
        },
        {
            "type": "w",
            "document":{
                "type": "jsonl"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await
}
