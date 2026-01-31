#[cfg(not(feature = "curl"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the curl feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features curl".into());
}

use env_applier::EnvApply;
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
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/links/{{ paginator.skip }}/10",
            "method": "get",
            "paginator": {
                "type": "offset",
                "skip":0,
                "limit": 1,
                "count": 10
            },
            "version": 1.1
        },
        "document":{
            "type": "text"
        },
        "concurrency_limit":10
    },
    {
        "type": "w"
    }]
    "#;

    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    assert!(
        0 < receiver_output.recv().await.into_iter().count(),
        "There should find some message."
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
