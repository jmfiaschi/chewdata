use env_applier::EnvApply;
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {
            "type": "r",
            "document" : {
                "type": "jsonl"
            },
            "connector": {
                "type": "bucket_select",
                "bucket": "my-bucket",
                "path": "data/multi_lines.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
                "region": "{{ BUCKET_REGION }}",
                "query": "select * from s3object"
            }
        },
        {
            "type": "w",
            "document" : {
                "type": "jsonl"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec_with_pipe(serde_json::from_str(config_resolved.as_str())?, None)
}
