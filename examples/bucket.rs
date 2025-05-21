use env_applier::EnvApply;
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

    let config = r#"
    [
        {
            "type": "e",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}"
            }
        },
        {
            "type": "r",
            "connector": {
                "type": "local",
                "path": "./data/multi_lines.{{ metadata.mime_subtype }}"
            }
        },
        {
            "type": "t",
            "actions": [
                {
                    "field":"",
                    "pattern": "{{ input | json_encode() }}"
                },
                {
                    "field":"uuid",
                    "pattern": "{{ uuid_v4() }}"
                }
            ]
        },
        {
            "type": "w",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.{{ metadata.mime_subtype }}",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}",
                "tags": {
                    "service:writer:owner": "my_team_name",
                    "service:writer:env": "dev",
                    "service:writer:context": "example"
                }
            },
            "document" : {
                "type": "jsonl",
                "is_pretty": true
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
