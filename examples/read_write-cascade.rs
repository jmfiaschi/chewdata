use env_applier::EnvApply;
use std::env;
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
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await?;

    tracing::info!(
        "Check the files `./data/out/cascade_file1.json` and `./data/out/cascade_file2.json`"
    );

    Ok(())
}
