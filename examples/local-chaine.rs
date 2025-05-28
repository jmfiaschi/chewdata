use env_applier::EnvApply;
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

    let config = r#"
    [
        {"type":"e","connector":{"type":"local","path":"./data/out/cascade_file*"}}
        ,{"type":"r","connector":{"type":"local","path":"./data/multi_lines.{{ metadata.mime_subtype }}"}}
        ,{"type":"t","actions":[{"field":"/","pattern":"{% if input.number == 10 %}{{ throw(message='data write in the file cascade_file2.json') }}{% else %}{{ input | json_encode() }}{% endif %}"}]}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file1.{{ metadata.mime_subtype }}"},"data_type":"ok"}
        ,{"type":"w","connector":{"type":"local","path":"./data/out/cascade_file2.{{ metadata.mime_subtype }}"},"data_type":"err"}
    ]
    "#;

    chewdata::exec(serde_json::from_str(config.apply().as_str())?, None, None).await?;

    tracing::info!(
        "Check the files `./data/out/cascade_file1.json` and `./data/out/cascade_file2.json`"
    );

    // Test example with asserts
    chewdata::exec(
        deser_hjson::from_str(config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        None,
        None,
    )
    .await?;

    let file1: Result<File, io::Error> = File::open("./data/out/cascade_file1.json");
    assert!(file1.is_ok(), "Le fichier cascade_file1 exist");

    let file2 = File::open("./data/out/cascade_file2.json");
    assert!(file2.is_ok(), "Le fichier cascade_file2 exist");

    let metadata1 = File::open("./data/out/cascade_file1.json")
        .unwrap()
        .metadata()?;

    let metadata2 = File::open("./data/out/cascade_file1.json")
        .unwrap()
        .metadata()?;

    let size1 = metadata1.len();
    let size2 = metadata2.len();

    assert!(0 < size1, "The file should not be empty");
    assert!(0 < size2, "The file should not be empty");

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
