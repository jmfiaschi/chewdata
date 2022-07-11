use chewdata::DataResult;
use chewdata::StepContext;
use std::io;
use std::thread;

#[async_std::main]
async fn main() -> io::Result<()> {
    let (sender_input, receiver_input) = async_channel::unbounded();
    let (sender_output, receiver_output) = async_channel::unbounded();

    let config = r#"
    [{
        "type": "t",
        "alias": "transform",
        "description": "run in a lambda script",
        "actions": [
            {
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
        let step_context = StepContext::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
        sender_input.try_send(step_context).unwrap();

        let data = serde_json::from_str(r#"{"field_1":"value_2","field_2":"value_2"}"#).unwrap();
        let step_context = StepContext::new("step_data_loading".to_string(), DataResult::Ok(data)).unwrap();
        sender_input.try_send(step_context).unwrap();
    });

    let config = serde_json::from_str(config.to_string().as_str())?;
    chewdata::exec(config, Some(receiver_input), Some(sender_output)).await?;

    for step_context in receiver_output.recv().await {
        println!("{}", step_context.data_result().to_value().to_string());
    }

    Ok(())
}
