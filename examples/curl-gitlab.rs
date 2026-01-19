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

    // Retrieve 20 projects from GitLab API and extract specific fields.
    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "https://gitlab.com",
            "path": "/api/v4/projects",
            "method": "get"
        }
    },
    {        
        "type": "t",
        "actions": [
            {
                "pattern": "{{ input | extract(attributes=[\"id\", \"name$\", \"path\", \"description\",\"count$\"]) | json_encode() }}"
            }
        ]
    },
    {
        "type": "w",
        "doc": {
            "type": "jsonl",
            "is_pretty": true
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
