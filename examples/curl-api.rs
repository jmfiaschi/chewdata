#[cfg(not(feature = "curl"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the curl feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features curl".into());
}

use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;

#[cfg(feature = "curl")]
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

#[cfg(feature = "curl")]
async fn run() -> io::Result<()> {
    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "mem",
            "data": "[{\"my_field\":\"my_value_1\"},{\"my_field\":\"my_value_2\"}]"
        }
    },{
        "type": "w",
        "connector": {
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/post",
            "method": "post"
        }
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

    let expected = serde_json::json!(["my_value_1", "my_value_2"]);

    assert_eq!(
        expected,
        result.clone().search("/*/my_field")?.unwrap_or_default(),
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(feature = "curl")]
#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_example() {
        run().await.unwrap();
    }
}
