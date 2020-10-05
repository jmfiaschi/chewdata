use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "builder":{
            "type":"json",
            "connector": {
                "type": "text",
                "data": "[{\"my_field\":\"my_value_1\"},{\"my_field\":\"my_value_2\"}]"
            }
        }
    },{
        "type": "w",
        "builder":{
            "type":"json",
            "connector": {
                "type": "curl",
                "endpoint": "http://localhost:8080",
                "path": "/post",
                "method": "post"
            }
        }
    },
    {
        "type": "w",
        "builder":{
            "type":"json",
            "connector": {
                "type": "io"
            }
        }
    }]
    "#;

    chewdata::exec(serde_json::from_str(config)?)?;

    Ok(())
}
