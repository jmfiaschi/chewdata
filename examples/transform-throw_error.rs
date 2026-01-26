use json_value_merge::Merge;
use json_value_search::Search;
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

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

    run().await
}

async fn run() -> io::Result<()> {
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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(serde_json::from_str(config)?, None, Some(sender_output)).await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    assert_eq!(
        1,
        result
            .clone()
            .search("/*/_error")?
            .unwrap_or_default()
            .as_array()
            .unwrap_or(&vec![])
            .len(),
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_example() {
        run().await.unwrap();
    }
}
