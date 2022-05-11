use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing_subscriber::registry().init();

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

    chewdata::exec(serde_json::from_str(config)?, None, None)
        .with_subscriber(subscriber)
        .await
}
