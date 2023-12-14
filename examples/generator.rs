use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
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

    chewdata::exec(serde_json::from_str(config)?, None, None).await
}
