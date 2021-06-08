use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "local",
            "path": "./data/one_line.json"
        }
    },
    {
        "type": "t",
        "updater": {
            "type": "tera",
            "actions": [
                {
                    "field":"test",
                    "pattern": "{{ throw(message='I throw an error!') }}"
                }
            ]
        }
    },
    {
        "type": "w",
        "data_type": "err"
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None).await
}
