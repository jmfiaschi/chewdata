use env_applier::EnvApply;
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "mem",
            "data": "[{\"my_field\":\"my_value_1\"},{\"my_field\":\"my_value_2\"}]"
        }
    },{
        "type": "w",
        "connector": {
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/post",
            "method": "post",
            "can_flush_and_read": true
        }
    },
    {
        "type": "w",
        "connector": {
            "type": "io"
        }
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec_with_pipe(serde_json::from_str(config_resolved.as_str())?, None)
}
