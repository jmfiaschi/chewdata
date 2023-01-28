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

    // init the erase_test file
    let config = r#"
    [
        {"type":"r","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}, "name": "file_ids"},
        { 
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.json"
            }
        },
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        { 
            "type": "writer",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.json"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await?;

    Ok(())
}
