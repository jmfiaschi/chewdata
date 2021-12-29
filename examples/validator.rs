use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
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

    let config_resolved = env::Vars::apply(config.to_string());

    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await?;

    Ok(())
}
