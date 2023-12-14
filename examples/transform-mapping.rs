use env_applier::EnvApply;
use std::env;
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
    [{
        "type": "r",
        name: reader
        "connector": {
            "type": "local",
            "path": "./data/multi_lines.json"
        }
    },
    {
        # Add the 'new_id' field into the data in input. 
        "type": "t",
        "alias": "transform",
        "data_type": "ok",
        "concurrency_limit": 3,
        "actions": [
            { 
                # Force to set 'output' with the data in 'input'. Same as '{}'.
                "field": "/"
                "pattern": "{{ input | json_encode() }}"
            },
            {},
            {
                field: display_context,
                pattern: "{{ context | json_encode() }}"
            },
            { 
                # Create a new field 'my_new_field' in the output and set the value with the 'pattern' expression.
                "field": my_new_field
                "pattern": "{{ input.number * output.number * local_mapping.2.number * context.steps.reader.number }}"
                "type": merge
            },
            { 
                # Remove the field 'text'.
                "field": "text"
                "type": remove
            },
            { 
                # Replace the 'array' field value.
                "field": "array"
                "pattern": '["a","b"]'
                "type": replace
            },
            {
                field: headers
                pattern: "{{ remote_mapping | json_encode() }}"
            }
        ],
        "referentials":{
            # Create a new data mapping that can be use into the transformer's actions. 
            local_mapping: {
                connector: {
                    type: local
                    path: ./data/multi_lines.json
                }
            },
            remote_mapping: {
                connector: {
                    type: curl
                    endpoint: "{{ CURL_ENDPOINT }}"
                    path: "/get?params={{ input.number }}"
                    method: get
                },
                document:{
                    type: jsonl
                    entry_path: /args
                }
            }
        },
    },
    {
        "type": "w"
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());

    chewdata::exec(
        deser_hjson::from_str(config_resolved.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        None,
    )
    .await
}
