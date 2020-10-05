use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "builder":{
            "type":"json",
            "connector": {
                "type": "local",
                "path": "./data/one_line.json"
            }
        }
    },
    {
        "type": "t",
        "updater": {
            "type": "tera",
            "actions": [
                {
                    "field":"",
                    "pattern": "{{ throw(message='I throw an error!') }}"
                }
            ]
        }
    },
    {
        "type": "w",
        "builder":{
            "type":"json",
            "connector": {
                "type": "io"
            }
        },
        "data_type": "err"
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?)?;

    Ok(())
}
