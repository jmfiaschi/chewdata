+++
title = "Hello World"
description = "Hello World with chewdata."
date = 2021-09-05T09:19:42+00:00
updated = 2021-09-05T09:19:42+00:00
draft = false
template = "blog/page.html"

[taxonomies]
authors = ["none"]

[extra]
lead = "This is an example how to do <b>Hello World</b> with chewdata."
+++

In your Cargo.toml add :

```toml
[package]
name = "hello_world"
version = "1.0.0"
edition = "2018"

[dependencies]
chewdata = {version="1.2",default-features=false}
async-std = { version = "1.10", features = ["attributes"] }
serde_json = "1.0"
```

In your main rust file in ./src/main.rs :

```rust
use std::io;

#[async_std::main]
async fn main() -> io::Result<()> {
    let config = r#"
    [{
        "type": "r",
        "conn": {
            "type": "mem",
            "data": "Hello World !!!"
        },
        "doc": { "type": "text" }
    },
    {
        "type": "w"
    }]
    "#;
    let config = serde_json::from_str(config.to_string().as_str())?;

    chewdata::exec(config, None).await
}
```

Run your script :

```bash
$ cargo run
["Hello World !!!"]
```
