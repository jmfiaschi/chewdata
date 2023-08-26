use chewdata::Context;
use chewdata::DataResult;
use std::io;
use std::thread;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    let (sender_input, receiver_input) = async_channel::unbounded();
    let (sender_output, receiver_output) = async_channel::unbounded();

    let config = r#"
    [{
        "type": "t",
        "alias": "transform",
        "description": "run in a lambda script",
        "actions": [
            {
                "pattern": "{{ input | json_encode() }}"
            },
            {
                "field":"new_field",
                "pattern": "new_value"
            }
        ]
    }]
    "#;

    // Spawn a thread that receives a message and then sends one.
    thread::spawn(move || {
        let data: serde_json::Value =
            serde_json::from_str(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();
        let context = Context::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
        sender_input.try_send(context).unwrap();

        let data = serde_json::from_str(r#"{"field_1":"value_2","field_2":"value_2"}"#).unwrap();
        let context = Context::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
        sender_input.try_send(context).unwrap();
    });

    let config = serde_json::from_str(config.to_string().as_str())?;
    chewdata::exec(config, Some(receiver_input), Some(sender_output)).await?;

    while let Ok(context) = receiver_output.recv().await {
        println!("{}", context.input().to_value().to_string());
    }

    Ok(())
}
