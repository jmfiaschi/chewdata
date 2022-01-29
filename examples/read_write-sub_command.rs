use std::io::{self, Read, Write};
use std::process::{Command, Stdio};

#[async_std::main]
async fn main() -> io::Result<()> {
    let data_to_transform = b"column1,column2\nvalue1,value2\n";
    let config = r#"[{"type":"r","connector":{"type":"io"},"document":{"type":"csv"}},{"type":"w","document":{"type":"jsonl"}}]"#;

    println!("Data to transform:\n{}", String::from_utf8_lossy(data_to_transform));

    let mut child = Command::new("./target/debug/chewdata")
        .args(&[config])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .env("RUST_LOG", "null")
        .spawn()?;

    let mut child_stdout = child.stdout.take().unwrap();
    let mut child_stdin = child.stdin.take().unwrap();

    child_stdin.write_all(data_to_transform)?;
    drop(child_stdin);
    
    let mut data = String::default();
    child_stdout.read_to_string(&mut data)?;

    println!("Data transformed:\n{}", data);

    Ok(())
}
