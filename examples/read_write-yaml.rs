use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "document" :{
            "type":"yaml"
        },
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.yml"
        }
    },
    {
        "type": "w",
        "document" : {
            "type": "yaml"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}
