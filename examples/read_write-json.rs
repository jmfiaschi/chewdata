use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "builder":{
            "type":"json",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        }
    },
    {
        "type": "w",
        "builder": {
            "type": "json"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?)?;

    Ok(())
}
