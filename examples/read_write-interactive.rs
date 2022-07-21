use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [{
        "type": "r",
        "conn":{
            "type": "io"
        }
    },
    {
        "type": "write",
        "dataset_size": 1
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None, None)
        .with_subscriber(subscriber)
        .await
}
