use env_applier::EnvApply;
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {
            "type": "e",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata"
            }
        },{
            "type": "r",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines_tmp.json"
            }
        },{
            "type": "t",
            "updater": {
                "type": "tera",
                "actions": [
                    {
                        "field":"/",
                        "pattern": "{{ input | json_encode() }}"
                    },
                    {
                        "field":"new_field_in_mongo",
                        "pattern": "{{ now() }}"
                    }
                ]
            },
            "thread_number":3
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata",
                "update_options": {
                    "upsert": true
                }
            },
            "thread_number":3
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
}
