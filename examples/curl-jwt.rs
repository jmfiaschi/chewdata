use env_applier::EnvApply;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

use macro_rules_attribute::apply;
use smol_macros::main;

#[apply(main!)]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

    let config = r#"
    [{
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/bearer",
            "method": "get",
            "auth": {
                "type": "jwt",
                "refresh": {
                    "type": "curl",
                    "endpoint": "http://jwtbuilder.jamiekurtz.com",
                    "path": "/tokens",
                    "method": "post",
                    "parameters": {
                        "alg":"HS256",
                        "claims":{"GivenName":"Johnny","username":"{{ CURL_BASIC_AUTH_USERNAME }}","password":"{{ CURL_BASIC_AUTH_PASSWORD }}","iat":1599462755,"exp":33156416077},
                        "key":"my_key"
                    },
                },
                "key": "my_key",
                "signing": "secret",
                "document": {
                    "metadata": {
                        "mime_subtype": "json"
                    },
                    "entry_path": "/token"
                }
            }
        }
    },
    { 
        "type": "w" 
    }]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await
}
