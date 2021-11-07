use chewdata::DataResult;
use crossbeam::channel::unbounded;
use std::io;
use std::thread;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (sender_input, receiver_input) = unbounded();
    let (sender_output, receiver_output) = unbounded();

    let config = r#"
    [{
        "type": "t",
        "alias": "transform",
        "description": "run in a lambda script",
        "actions": [
            {
                "field":"/",
                "pattern": "{{ input | json_encode() }}"
            },
            {
                "field":"new_field",
                "pattern": "new_value"
            }
        ]
    }]
    "#;

    // Spawn a thread that receives a message and then sends one.
    thread::spawn(move || {
        let data = serde_json::from_str(r#"{"field_1":"value_1","field_2":"value_1"}"#).unwrap();
        sender_input.send(DataResult::Ok(data)).unwrap();

        let data = serde_json::from_str(r#"{"field_1":"value_2","field_2":"value_2"}"#).unwrap();
        sender_input.send(DataResult::Ok(data)).unwrap();
    });

    let config = serde_json::from_str(config.to_string().as_str())?;
    chewdata::exec(config, Some(receiver_input), Some(sender_output)).await?;

    for data_result in receiver_output {
        println!("data_result {:?}", data_result);
    }

    Ok(())
}
