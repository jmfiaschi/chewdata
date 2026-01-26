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

    run().await
}

async fn run() -> io::Result<()> {
    let config = r#"
    [{
        "type": "generator",
        "size": 10
    },
    {
        "type": "t",
        "actions": [
            {
                "field":"firstname",
                "pattern": "{{ fake_first_name() }}"
            },
            {
                "field":"lastname",
                "pattern": "{{ fake_last_name() }}"
            },
            {
                "field":"city",
                "pattern": "{{ fake_city() }}"
            },
            {
                "field":"password",
                "pattern": "{{ fake_password(min = 5, max = 10) }}"
            },
            {
                "field":"color",
                "pattern": "{{ fake_color_hex() }}"
            }
        ]
    },
    {
        "type": "writer"
    }]
    "#;

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    assert!(
        10 == result
            .clone()
            .search("/*/firstname")?
            .unwrap_or_default()
            .as_array()
            .unwrap_or(&vec![])
            .len(),
        "Should return 10 items in the result.\n{}",
        result
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
