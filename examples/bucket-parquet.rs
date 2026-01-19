#[cfg(not(any(feature = "bucket", feature = "parquet")))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the bucket and parquet features are required for this example. Please enable them in your Cargo.toml file. cargo example EXAMPLE_NAME --features bucket,parquet".into());
}

use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;

#[cfg(any(feature = "bucket", feature = "parquet"))]
#[apply(main!)]
async fn main() -> io::Result<()> {
    use env_applier::EnvApply;
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
            "type": "writer",
            "document": {
                "type": "parquet"
            },
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/parquet_test_bucket.parquet",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}"
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
