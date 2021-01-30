use env_applier::EnvApply;
use std::env;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {
            "type": "e",
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
                "region": "{{ BUCKET_REGION }}"
            }
        },
        {
            "type": "reader",
            "connector": {
                "type": "local",
                "path": "./data/multi_lines.json"
            }
        },
        {
            "type": "transformer",
            "updater": {
                "type": "tera",
                "actions": [
                    {
                        "field":"",
                        "pattern": "{{ input | json_encode() }}"
                    },
                    {
                        "field":"uuid",
                        "pattern": "{{ uuid_v4() }}"
                    }
                ]
            }
        },
        {
            "type": "writer",
            "document" : {
                "type": "jsonl",
                "pretty": true
            },
            "connector": {
                "type": "bucket",
                "bucket": "my-bucket",
                "path": "data/out/db.jsonl",
                "endpoint":"{{ BUCKET_ENDPOINT }}",
                "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
                "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
                "region": "{{ BUCKET_REGION }}"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None).await
}
