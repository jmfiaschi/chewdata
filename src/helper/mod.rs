use crate::step::{reader::Reader, Step};
use serde_json::Value;
use std::{collections::HashMap, io};

pub mod json_pointer;
pub mod mustache;

/// Replace a HashMap of readers by HashMap of Values. Each Value indexed by the referencial name.
///
/// # Example
/// ```rust
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
///     let values = referentials_reader_into_value(referentials).await?;
///     let values_expected:HashMap<String, Vec<Value>> = serde_json::from_str(r#"{"ref_1":[{"column1":"value1"}],"ref_2":[{"column1":"value2"}]}"#).unwrap();
///
///     assert_eq!(values_expected, values);
///
///     Ok(())
/// }
/// ```
pub async fn referentials_reader_into_value(
    referentials: HashMap<String, Reader>,
) -> io::Result<HashMap<String, Vec<Value>>> {
    let mut referentials_vec = HashMap::new();

    for (alias, referential) in referentials {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let mut values: Vec<Value> = Vec::new();

        referential.exec(None, Some(sender)).await?;

        for step_context in receiver {
            values.push(step_context.data_result().to_value());
        }
        referentials_vec.insert(alias, values);
    }

    Ok(referentials_vec)
}
