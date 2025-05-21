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
            "type": "r",
            "conn": {
                "type":"mem",
                "data":"Enter your JWT:\n"
            },
            "doc":{
                "type":"text"
            }
        },
        {
            "type": "w",
            "doc":{
                "type":"text"
            }
        },
        {
            "type": "r",
            "doc":{
                "type":"text"
            }
        },
        {
            "type": "t",
            "actions":[
                {
                    "field": "/header",
                    "pattern": "{{ input | split(pat='.') | first() | base64_decode(config='STANDARD_NO_PAD') }}"
                },
                {
                    "field": "/payload",
                    "pattern": "{{ input | split(pat='.') | nth(n=1) | base64_decode(config='STANDARD_NO_PAD') }}"
                }
            ]
        },
        {
            "type": "w",
            "doc":{
                "type":"json",
                "is_pretty": true
            }
        }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None, None).await
}
