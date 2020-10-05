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
                "path": "./data/one_line.toml"
            }
        }
    },
    {
        "type": "w",
        "builder": {
            "type": "toml"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?)?;

    Ok(())
}
