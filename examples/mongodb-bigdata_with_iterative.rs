use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_subscriber;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

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
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/test_write_iterative.jsonl"
            }
        },{
            "type": "r",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "tests",
                "collection": "bigdata",
                "paginator": {
                    "type": "cursor",
                    "limit": 1000
                }
            }
        },{
            "type": "w",
            "thread_number":10,
            "connector":{
                "type": "local",
                "path": "./data/out/test_write_iterative.jsonl"
            },
            "document":{
                "type": "jsonl"
            }
        }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await?;

    tracing::info!("Check the collection: http://localhost:8081/db/tests/bigdata");

    Ok(())
}
