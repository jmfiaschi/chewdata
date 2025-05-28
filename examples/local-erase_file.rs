use std::fs::File;
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

    // init the erase_test file
    let config = r#"
    [
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
                "path": "./data/out/erase_test.{{ metadata.mime_subtype }}"
            }
        },{ 
            "type": "e",
            "connector":{
                "type": "local",
                "path": "./data/out/erase_test.json"
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

    let file = File::open("./data/out/erase_test.json");

    assert!(file.is_ok(), "Le fichier exist");

    let metadata = file.unwrap().metadata()?;

    let size = metadata.len();

    assert_eq!(0, size, "The file should be empty");

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
