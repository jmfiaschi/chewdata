use env_applier::EnvApply;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    let config = r#"
    [
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        {
            "type": "validator",
            "rules": {
                "number_rule": {
                    "pattern": "{% if input.number == 10  %} true {% else %} false {% endif %}",
                    "message": "Number must be equal to 10"
                },
                "rename_this_rule": {
                    "pattern": "{% if input.rename_this is matching('.*renamed 2') %} true {% else %} false {% endif %}",
                    "message": "The rename field must match 'rename 2'"
                },
                "code_rule": {
                    "pattern": "{% if mapping_ref | filter(attribute='mapping_code', value=input.code) | length > 0 %} true {% else %} false {% endif %}",
                    "message": "No mapping code found in the mapping reference"
                }
            },
            "refs": {
                "mapping_ref": {
                    "connector": {
                        "type":"local",
                        "path":"./data/mapping.json"
                    }
                }
            }
        },
        { "type": "writer" },
        { "type": "writer", "data_type": "err"}
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
