use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

use macro_rules_attribute::apply;
use smol_macros::main;

#[cfg(not(feature = "curl"))]
compile_error!(
    "the curl feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features curl"
);

#[apply(main!)]
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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    assert_eq!(
        3,
        result
            .clone()
            .search("/*/headers/params")?
            .unwrap_or_default()
            .as_array()
            .unwrap_or(&vec![])
            .len(),
        "The result not match the expected value"
    );

    assert_eq!(
        1080000,
        result
            .clone()
            .search("/*/my_new_field")?
            .unwrap_or_default()
            .as_array()
            .unwrap_or(&vec![])
            .into_iter()
            .map(|v| v.as_i64().unwrap())
            .sum::<i64>(),
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::main;

    #[test]
    fn test_example() {
        main().unwrap();
    }
}
