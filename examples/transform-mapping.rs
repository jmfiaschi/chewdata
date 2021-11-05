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
    [{
        "type": "r",
        "connector": {
            "type": "local",
            "path": "./data/one_line.json"
        }
    },
    {
        "type": "t",
        "alias": "transform",
        "description": "Create a new identifier 'new_id'",
        "data_type": "ok",
        "wait": 100,
        "threads": 3,
        "actions": [
            {
                "field":"/",
                "pattern": "{{ my_input | json_encode() }}"
            },
            {
                "field":"new_id",
                "pattern": "{{ alias_mapping[1].number * my_input.number * my_output.number }}"
            }
        ],
        "referentials":{
            "alias_mapping": {
                "connector": {
                    "type": "local",
                    "path": "./data/multi_lines.json"
                }
            }
        },
        "input": "my_input",
        "output": "my_output"
    },
    {
        "type": "w"
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None, None)
        .with_subscriber(subscriber)
        .await
}
