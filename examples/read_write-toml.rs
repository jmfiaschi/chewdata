use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "document" :{
            "type":"toml"
        },
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.toml"
        }
    },
    {
        "type": "w",
        "document" : {
            "type": "toml"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None)
}
