use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_env_filter(EnvFilter::from_default_env())
        // build but do not install the subscriber.
        .finish();

    tracing_subscriber::registry().init();

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
            "method": "post"
        }
    },
    {
        "type": "w"
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());

    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None)
        .with_subscriber(subscriber)
        .await?;

    Ok(())
}
