use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_env_filter(EnvFilter::from_default_env())
        // build but do not install the subscriber.
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [
        {
            "type": "e",
            "connector":{
                "type": "mongo",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "tests",
                "collection": "read_write"
            }
        },{
            "type": "r",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },{
            "type": "t",
            "actions": [
                {
                    "field":"/",
                    "pattern": "{{ input | json_encode() }}"
                },
                {
                    "field":"new_field_in_mongo",
                    "pattern": "{{ now() }}"
                }
            ],
            "thread_number": 3
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "tests",
                "collection": "read_write"
            },
            "thread_number": 1
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await?;

    tracing::info!("Check the collection: http://localhost:8081/db/tests/read_write");

    Ok(())
}
