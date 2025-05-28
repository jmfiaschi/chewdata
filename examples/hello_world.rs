use json_value_merge::Merge;
use macro_rules_attribute::apply;
use smol_macros::main;
use std::io;

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

    // Test example with asserts
    let (sender_output, receiver_output) = async_channel::unbounded();
    chewdata::exec(config, None, Some(sender_output)).await?;

    let mut result = serde_json::json!([]);
    while let Ok(output) = receiver_output.recv().await {
        result.merge(&output.input().to_value());
    }

    let expected = serde_json::json!("Hello World !!!");

    assert_eq!(
        expected,
        result.clone(),
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
