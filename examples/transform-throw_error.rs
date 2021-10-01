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
    [{
        "type": "r",
        "connector": {
            "type": "local",
            "path": "./data/one_line.json"
        }
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"test",
                "pattern": "{{ throw(message='I throw an error!') }}"
            }
        ]
    },
    {
        "type": "w",
        "data_type": "err"
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None)
        .with_subscriber(subscriber)
        .await
}
