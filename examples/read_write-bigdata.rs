use env_applier::EnvApply;
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [{
        "type": "r",
         "document" :{
            "type":"csv",
            "meta":{
                "delimiter":";"
            }
        },
        "connector": {
            "type": "curl",
            "endpoint": "https://data.iledefrance.fr",
            "path": "//explore/dataset/correspondances-code-insee-code-postal/download?format=csv&timezone=Europe/Berlin&use_labels_for_header=true",
            "method": "get"
        }
    },
    {
        "type": "t",
        "updater": {
            "type": "tera",
            "actions": [
                {
                    "field":"/",
                    "pattern": "{{ input | json_encode() }}"
                },
                {
                    "field":"geo_shape",
                    "pattern": "{{ input.geo_shape }}"
                }
            ]
        }
    },{
        "type": "w",
        "document" :{
            "type":"jsonl"
        },
        "connector": {
            "type": "local",
            "path": "./data/out/correspondances-code-insee-code-postal.jsonl",
            "can_truncate": true
        }
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
}
