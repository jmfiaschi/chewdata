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

    self::insert().await?;
    tracing::info!("Check the collection: http://localhost:8082/?pgsql=psql&username=admin&db=postgres&ns=examples");

    tracing::info!("Select 2 lines but return one.");
    self::select().await?;

    Ok(())
}

async fn insert() -> io::Result<()> {
    let config = r#"
    [
        {
            "type": "e",
            "connector":{
                "type": "psql",
                "endpoint": "{{ PSQL_ENDPOINT }}",
                "db": "{{ PSQL_DB }}",
                "collection": "examples.simple_insert"
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
                    "field":"date",
                    "pattern": "{{ input.date | date(format=\"%Y-%m-%dT%H:%M:%S\") }}"
                },
                {
                    "field":"array",
                    "pattern": "[1,2,3,4]"
                },
                {
                    "field":"object",
                    "pattern": "{\"object_field\":\"object_value\"}"
                }
            ],
            "concurrency_limit": 1
        },{
            "type": "w",
            "connector":{
                "type": "psql",
                "endpoint": "{{ PSQL_ENDPOINT }}",
                "db": "{{ PSQL_DB }}",
                "collection": "examples.simple_insert"
            },
            "concurrency_limit": 1
        },{
            # Write data in error in the stdout with the error message
            "type": "w",
            "data": "err"
        }
    ]
    "#;

    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        None,
    )
    .await?;

    Ok(())
}

async fn select() -> io::Result<()> {
    let config = r#"
        [
            {
                "type": "r",
                "connector":{
                    "type": "psql",
                    "endpoint": "{{ PSQL_ENDPOINT }}",
                    "db": "{{ PSQL_DB }}",
                    "collection": "examples.simple_insert",
                    "query": "SELECT * FROM {{ collection }} WHERE number IN (10,20)",
                    "paginator": {
                        "type": "offset",
                        "limit": 1,
                        "skip": 1
                    }
                }
            },{
                "type": "w"
            }
        ]
        "#;

    // Test example with validation rules
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

    let expected = serde_json::json!([20]);

    assert_eq!(
        expected,
        result.clone().search("/*/number")?.unwrap_or_default(),
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
