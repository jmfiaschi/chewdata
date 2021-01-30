use env_applier::EnvApply;
use std::env;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
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
            }
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
            },
            "thread_number":3
        },{
            "type": "w",
            "connector":{
                "type": "mongodb",
                "endpoint": "{{ MONGODB_ENDPOINT }}",
                "db": "test",
                "collection": "bigdata"
            },
            "thread_number":3
        }
    ]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None).await
}
