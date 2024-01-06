use crate::{
    step::{reader::Reader, receive, Step},
    Context,
};
use async_std::task;
use futures::StreamExt;
use serde_json::Value;
use std::{collections::HashMap, io};

pub mod json_pointer;
pub mod mustache;
pub mod string;
pub mod value;

#[cfg(feature = "xml")]
pub mod xml2json;

/// Replace a HashMap of readers by HashMap of Values. Each Value indexed by the referencial name.
///
/// # Examples
///
/// ```no_run
/// use chewdata::step::reader::Reader;
/// use chewdata::connector::in_memory::InMemory;
/// use chewdata::connector::{Connector, ConnectorType};
/// use std::{io, collections::HashMap};
/// use chewdata::helper::referentials_reader_into_value;
/// use serde_json::Value;
/// use chewdata::DataResult;
/// use chewdata::Context;
///
/// #[async_std::main]
/// async fn main() -> io::Result<()> {
///     let referential_1 = Reader {
///         connector_type: ConnectorType::InMemory(InMemory::new(r#"[{"column1":"value1"}]"#)),
///         ..Default::default()
///     };
///     let referential_2 = Reader {
///         connector_type: ConnectorType::InMemory(InMemory::new(r#"[{"column1":"value2"}]"#)),
///         ..Default::default()
///     };
///     let mut referentials = HashMap::default();
///     referentials.insert("ref_1".to_string(), referential_1);
///     referentials.insert("ref_2".to_string(), referential_2);
///
///     let context = Context::new("step_main".to_string(), DataResult::Ok(Value::Null)).unwrap();
///
///     let values = referentials_reader_into_value(&referentials, &context).await?;
///     let values_expected:HashMap<String, Vec<Value>> = serde_json::from_str(r#"{"ref_1":[{"column1":"value1"}],"ref_2":[{"column1":"value2"}]}"#).unwrap();
///
///     assert_eq!(values_expected, values);
///
///     Ok(())
/// }
/// ```
pub async fn referentials_reader_into_value(
    referentials: &HashMap<String, Reader>,
    context: &Context,
) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_vec = HashMap::new();

    for (name, referential) in referentials {
        let (sender_input, receiver_input) = async_channel::unbounded();
        let (sender_output, receiver_output) = async_channel::unbounded();

        sender_input
            .send(context.clone())
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        sender_input.close();

        let mut task_referential = referential.clone();
        task_referential.name = name.clone();
        task_referential.set_receiver(receiver_input.clone());
        task_referential.set_sender(sender_output.clone());

        task::spawn(async move { task_referential.exec().await }).await?;
        sender_output.close();

        let values = receive(&receiver_output)
            .await?
            .map(|context| context.input().to_value())
            .collect()
            .await;

        referentials_vec.insert(name.clone(), values);
    }

    Ok(referentials_vec)
}

#[cfg(test)]
mod tests {
    use crate::{
        connector::{in_memory::InMemory, ConnectorType},
        DataResult,
    };

    use super::*;

    #[async_std::test]
    async fn referentials_reader_into_value() {
        let referential_1 = Reader {
            connector_type: ConnectorType::InMemory(InMemory::new(
                r#"[{"column1":"value1"},{"column1":"value2"}]"#,
            )),
            ..Default::default()
        };
        let referential_2 = Reader {
            connector_type: ConnectorType::InMemory(InMemory::new(
                r#"[{"column1":"value3"},{"column1":"value4"}]"#,
            )),
            ..Default::default()
        };
        let mut referentials = HashMap::default();
        referentials.insert("ref_1".to_string(), referential_1);
        referentials.insert("ref_2".to_string(), referential_2);

        let context = Context::new("step_main".to_string(), DataResult::Ok(Value::Null)).unwrap();

        let values = super::referentials_reader_into_value(&referentials, &context)
            .await
            .unwrap();

        let values_expected: HashMap<String, Vec<Value>> = serde_json::from_str(
            r#"{"ref_1":[{"column1":"value1"},{"column1":"value2"}],"ref_2":[{"column1":"value3"},{"column1":"value4"}]}"#,
        )
        .unwrap();
        assert_eq!(values_expected, values);
    }
}
