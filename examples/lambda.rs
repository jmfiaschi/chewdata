use chewdata::Context;
use chewdata::DataResult;
use json_value_merge::Merge;
use json_value_search::Search;
use std::io;
use std::thread;
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

    let (sender_input, receiver_input) = async_channel::unbounded();
    let (sender_output, receiver_output) = async_channel::unbounded();

    let config = r#"
    [{
        "type": "t",
        "alias": "transform",
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
        let context = Context::new("step_data_loading".to_string(), DataResult::Ok(data));
        sender_input.try_send(context).unwrap();

        let data = serde_json::from_str(r#"{"field_1":"value_2","field_2":"value_2"}"#).unwrap();
        let context = Context::new("step_data_loading".to_string(), DataResult::Ok(data));
        sender_input.try_send(context).unwrap();
    });

    let config = serde_json::from_str(config.to_string().as_str())?;
    chewdata::exec(config, Some(receiver_input), Some(sender_output)).await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!(["value_1", "value_2"]);

    assert_eq!(
        expected,
        result.clone().search("/*/field_2")?.unwrap_or_default(),
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::main;

    #[test]
    fn test_example() {
        main().unwrap();
    }
}
