use crate::step::{reader::Reader, Step};
use async_std::task;
use serde_json::Value;
use std::{collections::HashMap, io};

pub mod json_pointer;
pub mod mustache;

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
///     let values = referentials_reader_into_value(&referentials).await?;
///     let values_expected:HashMap<String, Vec<Value>> = serde_json::from_str(r#"{"ref_1":[{"column1":"value1"}],"ref_2":[{"column1":"value2"}]}"#).unwrap();
///
///     assert_eq!(values_expected, values);
///
///     Ok(())
/// }
/// ```
pub async fn referentials_reader_into_value(
    referentials: &HashMap<String, Reader>,
) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_vec = HashMap::new();

    for (name, referential) in referentials.clone() {
        let (sender, receiver) = async_channel::unbounded();
        let mut values: Vec<Value> = Vec::new();

        task::spawn(async move {
            let mut task_referential = referential;
            task_referential.set_sender(sender.clone());
            task_referential.exec().await
        })
        .await?;

        loop {
            match receiver.recv().await {
                Ok(context_received) => {
                    let value = context_received.input().to_value();
                    trace!(
                        value = format!("{:?}", value).as_str(),
                        "A new referential value found in the pipe"
                    );
                    values.push(context_received.input().to_value());
                    continue;
                }
                Err(e) => {
                    info!(
                        error = format!("{:?}", e).as_str(),
                        "The channel is disconnected.",
                    );
                    break;
                }
            };
        }
        referentials_vec.insert(name.clone(), values);
    }

    Ok(referentials_vec)
}

#[cfg(test)]
mod tests {
    use crate::connector::{in_memory::InMemory, ConnectorType};

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
        let values = super::referentials_reader_into_value(&referentials)
            .await
            .unwrap();
        let values_expected: HashMap<String, Vec<Value>> = serde_json::from_str(
            r#"{"ref_1":[{"column1":"value1"},{"column1":"value2"}],"ref_2":[{"column1":"value3"},{"column1":"value4"}]}"#,
        )
        .unwrap();
        assert_eq!(values_expected, values);
    }
}
