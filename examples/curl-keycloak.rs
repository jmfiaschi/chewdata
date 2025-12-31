use env_applier::EnvApply;
use json_value_merge::Merge;
use json_value_search::Search;
use std::env;
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
    [
    {
        "type": "r",
        "connector": {
            "type": "curl",
            "endpoint": "{{ KEYCLOAK_ENDPOINT }}",
            "path": "/realms/test/protocol/openid-connect/certs",
            "method": "get",
        }
    },
    {
        "type":"t",
        "actions":[{
            "field": "/",
            "pattern": "{{ input.keys | filter(attribute='use', value='sig') | first | json_encode() }}"
        }]
    }
    {
        "type": "w"
    }]
    "#;

    // Test example with validation rules
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut jwk = serde_json::Value::default();
    while let Ok(output) = receiver_output.recv().await {
        jwk = output.input().to_value();
        break;
    }

    env::set_var("JWK", jwk.to_string());

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
                "refresh": {
                    "type": "curl",
                    "endpoint": "{{ KEYCLOAK_ENDPOINT }}/realms/test/protocol/openid-connect",
                    "path": "/token",
                    "method": "post",
                    "parameters": "client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password",
                },
                "algorithm":"RS256",
                "jwk": {{ JWK }},
                "signing": "rsa_components",
                "document": {
                    "metadata": {
                        "mime_subtype": "x-www-form-urlencoded"
                    },
                    "entry_path": "/access_token"
                }
            }
        }
    },
    {
        "type": "w"
    }]
    "#;

    // Test example with validation rules
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(
        deser_hjson::from_str(config.apply().as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        Some(sender_output),
    )
    .await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!([true]);

    assert_eq!(
        expected,
        result
            .clone()
            .search("/*/authenticated")?
            .unwrap_or_default(),
        "The result does not match the expected value. Result: {}",
        result.to_string()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[test]
    async fn test_example() {
        main().unwrap();
    }
}
