use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.{{ metadata.mime_subtype }}"
            }
        },
        {
            "type": "writer",
            "document": {
                "type": "parquet",
                "schema": {
                    "fields":[
                        {"name": "number", "type": {"name": "int", "bitWidth": 64, "isSigned": false}, "nullable": false},
                        {"name": "group", "type": {"name": "int", "bitWidth": 64, "isSigned": false}, "nullable": false},
                        {"name": "string", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "long-string", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "boolean", "type": {"name": "bool"}, "nullable": false},
                        {"name": "special_char", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "rename_this", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "date", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "filesize", "type": {"name": "int", "bitWidth": 64, "isSigned": false}, "nullable": false},
                        {"name": "round", "type": { "name": "floatingpoint", "precision": "DOUBLE"}, "nullable": false},
                        {"name": "url", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "list_to_sort", "type":{"name": "utf8"}, "nullable": true},
                        {"name": "code", "type": {"name": "utf8"}, "nullable": false},
                        {"name": "remove_field", "type": {"name": "utf8"}, "nullable": false}
                    ]
                }
            },
            "connector": {
                "type":"local",
                "path": "./data/out/parquet_test_local.{{ metadata.mime_subtype }}"
            }
        }
    ]
    "#;
    
    chewdata::exec(serde_json::from_str(config)?, None, None)
        .with_subscriber(subscriber)
        .await
}
