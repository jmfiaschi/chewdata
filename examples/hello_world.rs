use std::io;

use macro_rules_attribute::apply;
use smol_macros::main;

#[apply(main!)]
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

    chewdata::exec(config, None, None).await
}
