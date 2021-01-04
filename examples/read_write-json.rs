use std::io;

fn main() -> io::Result<()> {
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

    chewdata::exec_with_pipe(serde_json::from_str(config)?, None)
}
