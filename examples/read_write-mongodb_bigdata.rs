use env_applier::EnvApply;
use std::env;
use std::io;

fn main() -> io::Result<()> {
    let _guard = slog_envlogger::init().unwrap();

    let config = r#"
    [
        {
            "type": "r",
            "document":{
                "type":"csv"
            },
            "connector":{
                "type": "local",
                "path": "./data/out/bigdata.csv"
            },
            "dataset_size": 1000
        },{
            "type": "t",
            "updater": {
                "type": "tera",
                "actions": [
                    {
                        "field":"region",
                        "pattern": "{{ input.Region }}"
                    },
                    {
                        "field":"country",
                        "pattern": "{{ input.Country }}"
                    },
                    {
                        "field":"item_type",
                        "pattern": "{{ input['Item Type'] }}"
                    },
                    {
                        "field":"sales_channel",
                        "pattern": "{{ input['Sales Channel'] }}"
                    },
                    {
                        "field":"order_priority",
                        "pattern": "{{ input['Order Priority'] }}"
                    }
                ]
            }
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata"
            }
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec_with_pipe(serde_json::from_str(config_resolved.as_str())?, None)
}
