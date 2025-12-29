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
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        {
            "type": "writer",
            "document": {
                "type": "parquet",
                "schema":{
                    "fields":[
                        {
                            "name": "number",
                            "nullable": false,
                            "type": {
                                "name": "int",
                                "bitWidth": 8,
                                "isSigned": false
                            }
                        },
                        {
                            "name": "string",
                            "nullable": false,
                            "type": {
                                "name": "utf8"
                            }
                        },
                        {
                            "name": "boolean",
                            "nullable": false,
                            "type": {
                                "name": "bool"
                            }
                        },
                        {
                            "name": "date",
                            "nullable": false,
                            "type": {
                                "name": "date",
                                "unit": "DAY"
                            }
                        }
                    ]
                }
            },
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/parquet_test_bucket-custom_schema.parquet",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "region": "{{ BUCKET_REGION }}"
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
