use async_process::{Command, Stdio};
use futures::{AsyncReadExt, AsyncWriteExt};
use std::io;

use macro_rules_attribute::apply;
use smol_macros::main;

#[apply(main!)]
async fn main() -> io::Result<()> {
    let data_to_transform = b"column1,column2\nvalue1,value2\n---\n";
    let config = r#"[{"type":"r","connector":{"type":"cli"},"document":{"type":"csv"}},{"type":"w","document":{"type":"jsonl"}}]"#;

    println!(
        "Data to transform:\n{}",
        String::from_utf8_lossy(data_to_transform)
    );

    let mut child = Command::new("./target/debug/chewdata")
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

#[cfg(test)]
mod tests {
    use crate::main;

    #[test]
    fn test_example() {
        main().unwrap();
    }
}
