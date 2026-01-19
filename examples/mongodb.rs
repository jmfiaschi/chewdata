#[cfg(not(feature = "mongodb"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the mongodb feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features mongodb".into());
}

use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;

#[cfg(feature = "mongodb")]
#[apply(main!)]
async fn main() -> io::Result<()> {
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::{self, Layer};

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

    run().await
}

#[cfg(feature = "mongodb")]
async fn insert() -> io::Result<()> {
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
            "concurrency_limit": 3
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "tests",
                "collection": "read_write"
            },
            "concurrency_limit": 1
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

    tracing::info!("Check the collection: http://localhost:8082/?mongodb=mongodb&username=admin&db=examples&ns=examples.simple_insert");

    Ok(())
}

#[cfg(feature = "mongodb")]
async fn select() -> io::Result<()> {
    let config = r#"
    [
        {
            "type": "r",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "tests",
                "collection": "read_write"
            }
        },{
            "type": "w"
        }
    ]"#;

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

    let expected = serde_json::json!([10, 20, 30]);

    assert_eq!(
        expected,
        result.clone().search("/*/number")?.unwrap_or_default(),
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(feature = "mongodb")]
async fn run() -> io::Result<()> {
    self::insert().await?;
    self::select().await?;

    Ok(())
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_example() {
        run().await.unwrap();
    }
}
