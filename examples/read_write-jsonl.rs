use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
         "document" :{
            "type":"jsonl"
        },
        "connector":{
            "type": "local",
            "path": "./data/multi_lines.jsonl"
        }
    },
    {
        "type": "w",
        "document" : {
            "type": "jsonl"
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?, None)
}
