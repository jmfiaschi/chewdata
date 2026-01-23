#[cfg(not(feature = "csv"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    return Err("the csv feature is required for this example. Please enable it in your Cargo.toml file. cargo example EXAMPLE_NAME --features csv".into());
}

use async_process::{Command, Stdio};
use futures::{AsyncReadExt, AsyncWriteExt};
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;

#[cfg(feature = "csv")]
#[apply(main!)]
async fn main() -> io::Result<()> {
    run().await
}

#[cfg(feature = "csv")]
async fn run() -> io::Result<()> {
    let data_to_transform = b"column1,column2\nvalue1,value2\n";
    let config = r#"[{"type":"r","connector":{"type":"cli"},"document":{"type":"csv"}},{"type":"w","document":{"type":"jsonl"}}]"#;

    println!(
        "Data to transform:\n{}",
        String::from_utf8_lossy(data_to_transform)
    );

    let mut child = Command::new("cargo")
        .arg("run")
        .arg("--features")
        .arg("csv")
        .args(&[config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .env("RUST_LOG", "null")
        .spawn()?;

    let mut child_stdin = child.stdin.take().unwrap();
    child_stdin.write_all(data_to_transform).await.unwrap();
    drop(child_stdin);

    let mut result = String::default();
    let mut child_stdout = child.stdout.take().unwrap();
    child_stdout.read_to_string(&mut result).await.unwrap();

    assert_eq!(
        result, "{\"column1\":\"value1\",\"column2\":\"value2\"}",
        "The result not match the expected value"
    );

    Ok(())
}

#[cfg(feature = "csv")]
#[cfg(test)]
mod tests {
    use super::*;
    use smol_macros::test;

    #[apply(test!)]
    async fn test_example() {
        run().await.unwrap();
    }
}
