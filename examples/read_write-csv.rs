use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "document":{
            "type":"csv"
        },
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.csv"
        }
    },
    {
        "type": "w",
        "document": {
            "type": "csv"
        }
    },
    {
        "type": "w",
        "document": {
            "type": "csv"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}
