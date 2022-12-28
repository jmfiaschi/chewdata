use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_futures::WithSubscriber;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing_subscriber::registry().init();

    let config = r#"
    [{
        "type": "r",
        "connector":{
            "type": "mem",
            "data": "{\"username\":\"my_username\",\"password\":\"my_password\"}"
        }
    },
    {
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/bearer",
            "method": "get",
            "auth": {
                "type": "jwt",
                "refresh_connector": {
                    "type": "curl",
                    "endpoint": "http://jwtbuilder.jamiekurtz.com",
                    "path": "/tokens",
                    "method": "post"
                },
                "token_name":"token",
                "key": "my_key",
                "payload": {
                    "alg":"HS256",
                    "claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077},
                    "key":"my_key"
                },
                "refresh_document": {
                    "metadata": {
                        "mime_type": "application",
                        "mime_subtype": "json"
                    }
                }
            }
        }
    },
    { 
        "type": "w" 
    }]
    "#;

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None)
        .with_subscriber(subscriber)
        .await
}
