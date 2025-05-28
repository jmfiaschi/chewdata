use std::fs;
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
        {"type":"r","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}, "name": "file_ids"},
        { 
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.*"
            }
        },
        {
            "type": "reader",
            "connector":{
                "type": "local",
                "path": "./data/multi_lines.{{ metadata.mime_subtype }}"
            }
        },
        { 
            "type": "writer",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test_{{ steps.file_ids.id }}.{{ metadata.mime_subtype }}"
            }
        }
    ]
    "#;

    // Test example with asserts
    chewdata::exec(
        deser_hjson::from_str(config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        None,
    )
    .await?;

    let result = fs::read_to_string("./data/out/erase_test_1.json");

    assert!(
        result.is_ok(),
        "File doesn't exist in the path data out folder."
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::main;

    #[test]
    fn test_example() {
        main().unwrap();
    }
}
