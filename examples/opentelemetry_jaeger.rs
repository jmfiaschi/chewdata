use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_subscriber;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

#[async_std::main]
async fn main() -> io::Result<()> {
    let mut layers = Vec::new();

    // Install a new OpenTelemetry trace pipeline
    #[cfg(feature = "apm")]
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("chewdata")
        .install_simple()
        .unwrap();

    // Create new layer for opentelemetry
    #[cfg(feature = "apm")]
    let telemetry = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        //.with_filter(EnvFilter::from_default_env())
        .boxed();
    #[cfg(feature = "apm")]
    layers.push(telemetry);

    // Create new layer for stdout logs
    let (non_blocking, _guard) = tracing_appender::non_blocking(io::stdout());
    let layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env())
        .boxed();
    layers.push(layer);

    tracing_subscriber::registry().with(layers).init();

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
                "connector": {
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
                "document": {
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
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await?;

    // Shutdown trace pipeline
    #[cfg(feature = "apm")]
    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}
