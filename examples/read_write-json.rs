use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {
            "type": "r",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        { "type": "w" }
    ]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}
