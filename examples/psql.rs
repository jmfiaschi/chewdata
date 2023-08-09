use std::io;
use std::env;
use env_applier::EnvApply;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();
    
    {
        let config = r#"
        [
            {
                "type": "e",
                "connector":{
                    "type": "psql",
                    "endpoint": "{{ PSQL_ENDPOINT }}",
                    "db": "{{ PSQL_DB }}",
                    "collection": "examples.simple_insert"
                }
            },{
                "type": "r",
                "connector":{
                    "type": "local",
                    "path": "./data/multi_lines.json"
                }
            },{
                "type": "t",
                "actions": [
                    {
                        "field":"/",
                        "pattern": "{{ input | json_encode() }}"
                    },
                    {
                        "field":"date",
                        "pattern": "{{ input.date | date(format=\"%Y-%m-%dT%H:%M:%S\") }}"
                    },
                    {
                        "field":"array",
                        "pattern": "[1,2,3,4]"
                    },
                    {
                        "field":"object",
                        "pattern": "{\"object_field\":\"object_value\"}"
                    }
                ],
                "thread_number": 1
            },{
                "type": "w",
                "connector":{
                    "type": "psql",
                    "endpoint": "{{ PSQL_ENDPOINT }}",
                    "db": "{{ PSQL_DB }}",
                    "collection": "examples.simple_insert"
                },
                "thread_number": 1
            },{
                "type": "w",
                "desc": "Write data in error in the stdout with the error message",
                "data": "err"
            }
        ]
        "#;

        let config_resolved = env::Vars::apply(config.to_string());
        chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await?;

        tracing::info!("Check the collection: http://localhost:8082/?pgsql=psql&username=admin&db=postgres&ns=examples");
    }

    {
        tracing::info!("Select 2 lines but return one.");

        let config = r#"
        [
            {
                "type": "r",
                "connector":{
                    "type": "psql",
                    "endpoint": "{{ PSQL_ENDPOINT }}",
                    "db": "{{ PSQL_DB }}",
                    "collection": "examples.simple_insert",
                    "query": "SELECT * FROM {{ collection }} WHERE number IN (10,20)",
                    "paginator": {
                        "type": "offset",
                        "limit": 1,
                        "skip": 1
                    }
                }
            },{
                "type": "w"
            }
        ]
        "#;

        let config_resolved = env::Vars::apply(config.to_string());
        chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await?;
    }

    Ok(())
}
