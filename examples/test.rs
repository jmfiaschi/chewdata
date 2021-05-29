use async_std::fs::OpenOptions;
use std::io;
use async_std::io::ReadExt;


#[async_std::main]
async fn main() -> io::Result<()> {
    let mut buffer = Vec::default();
    let mut file = OpenOptions::new()
                            .read(true)
                            .write(false)
                            .create(false)
                            .append(false)
                            .truncate(false)
                            .open("data/multi_lines_tmp.json").await?;
     file.read_to_end(&mut buffer).await?;
    println!("file {:?}", &String::from_utf8_lossy(&buffer.as_ref()));
    file.read_to_end(&mut buffer).await?;
    println!("file {:?}", &String::from_utf8_lossy(&buffer.as_ref()));
    Ok(())
}
