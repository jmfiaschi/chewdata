use env_applier::EnvApply;
use std::env;
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
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "mongo",
                "can_truncate": true
            }
        },{
            "type": "r",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "mongo"
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
            }
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata",
                "can_truncate": true,
                "update_options": {
                    "upsert": true
                }
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec_with_pipe(serde_json::from_str(config_resolved.as_str())?, None)
}
