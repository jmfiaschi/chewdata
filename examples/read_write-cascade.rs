use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_env_filter(EnvFilter::from_default_env())
        // build but do not install the subscriber.
        .finish();

    let config = r#"
    [
        {"type":"e","connector":{"type":"local","path":"./data/out/cascade_file1.json"}}
        ,{"type":"e","connector":{"type":"local","path":"./data/out/cascade_file2.json"}}
        ,{"type":"r","connector":{"type":"local","path":"./data/multi_lines.json"}}
        ,{"type":"t","actions":[{"field":"/","pattern":"{% if input.number == 10 %}{{ throw(message='data write in the file cascade_file2.json') }}{% else %}{{ input | json_encode() }}{% endif %}"}]}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file1.json"},"data_type":"ok"}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file2.json"},"data_type":"err"}
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await?;

    tracing::info!(
        "Check the files `./data/out/cascade_file1.json` and `./data/out/cascade_file2.json`"
    );

    Ok(())
}
