use env_applier::EnvApply;
use std::env;
use std::io;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{self, Layer};

#[async_std::main]
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
                    "endpoint": "http://localhost:8083/auth/realms/test/protocol/openid-connect",
                    "path": "/token",
                    "method": "post"
                },
                "algorithm":"RS256",
                "jwk": {
                    "kid": "jPc8FWeTOrgybc2_xBrShjNYUE5kiKTvpwSlNrNGUFA",
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "n": "kVdSs7RwLWFbfMShEoKn5gT_aemVCf6r9aaseowgAwOpKYMlhSpLNXchm6Lgt1qedpcgMD0ih2d3jBr-jGtHSnMB_uOpFHVyI9hIysYveyojet7LREIzjuJr3-qHmsPJ6_vasWrSr7AwxQWoCiHdtrPCzm9qtlnvwgpKdmbJX8SN8FiNgHrkLDwNFCFZB470vxc-4QBgBi0vpqx7hqWr9B5snmiGzrU1Humq351Wk_svGKLEyJM6IkqRzle3F47gynPGeb_lx835xKaJ57kbag-_KHI4G1zzmMnTXpVeRsr9T4scc6777WS2NEp8VHWavCa0VWXwJYBbzogWGSQXww",
                    "e": "AQAB",
                    "x5c": [
                        "MIIClzCCAX8CBgGFVWxmmDANBgkqhkiG9w0BAQsFADAPMQ0wCwYDVQQDDAR0ZXN0MB4XDTIyMTIyNzIxMDkwNVoXDTMyMTIyNzIxMTA0NVowDzENMAsGA1UEAwwEdGVzdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJFXUrO0cC1hW3zEoRKCp+YE/2nplQn+q/WmrHqMIAMDqSmDJYUqSzV3IZui4LdannaXIDA9Iodnd4wa/oxrR0pzAf7jqRR1ciPYSMrGL3sqI3rey0RCM47ia9/qh5rDyev72rFq0q+wMMUFqAoh3bazws5varZZ78IKSnZmyV/EjfBYjYB65Cw8DRQhWQeO9L8XPuEAYAYtL6ase4alq/QebJ5ohs61NR7pqt+dVpP7LxiixMiTOiJKkc5XtxeO4Mpzxnm/5cfN+cSmiee5G2oPvyhyOBtc85jJ016VXkbK/U+LHHOu++1ktjRKfFR1mrwmtFVl8CWAW86IFhkkF8MCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAMDWjsewPWX9YNb9YgmbyAtjmBZij+FJPvy8JZO057STKYcSwyQihYHz1mkItMIqyf+hq4oi+OlINCeki9ZbSoBZP4rUqhruEdz50AKqJt5c6KgxRJTBRwMnm4hPwiqlERFICmNdAyCiL67B5m9CaFsjM5dRc11WVxkXXB6qM0Lpw3M8nmnV0QbFvmUI29JMQ9KmsQG77eZGIuL+PrYLY6+1KqilnbnHth0kkKWq4qijCIqfMhibE/l6PZpgOZsoEjf+ocyoOxd55svfx4DQslncpVc5yRjqLUMMgMbC26cW9CghGBxbR9+PtjURvLO97EvDDsHcU5VmnWUEmlV7cxw=="
                    ],
                    "x5t": "jM3m3RKAFgRaa0iyqkxv4K5xhqE",
                    "x5t#S256": "-WBOVu1q7fKqKz5j7JNaoYCZUal2AlZRqC49GS4lyXQ"
                },
                "signing": "rsa_components",
                "payload": "client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password",
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

    let config_resolved = env::Vars::apply(config.to_string());
    chewdata::exec(serde_json::from_str(config_resolved.as_str())?, None, None).await
}
