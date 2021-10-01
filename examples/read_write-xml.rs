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
    [{
        "type": "reader",
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.xml"
        },
        "document" :{
            "type":"xml",
            "entry_path": "/root/*/item"
        }
    },
    {
        "type": "writer",
        "document": {
            "type": "xml",
            "entry_path": "/root/*/item"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None)
        .with_subscriber(subscriber)
        .await
}
