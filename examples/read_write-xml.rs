use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "document" :{
            "type":"xml"
        },
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.xml"
        }
    },
    {
        "type": "w",
        "document" : {
            "type": "xml"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None)
}
