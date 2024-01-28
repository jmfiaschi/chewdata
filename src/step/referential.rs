use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use async_std::{sync::Mutex, task};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::io;

use crate::Context;

use super::{reader::Reader, receive, Step};

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Referential {
    #[serde(flatten)]
    readers: HashMap<String, Reader>,
}

static CACHES: OnceLock<Arc<Mutex<HashMap<String, Vec<Value>>>>> = OnceLock::new();

impl Referential {
    pub fn new(readers: &HashMap<String, Reader>) -> Self {
        Referential {
            readers: readers.clone(),
        }
    }
    pub async fn cache(&self, referential_name: &String) -> std::io::Result<Option<Vec<Value>>> {
        let caches = CACHES.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        if let Some(results) = caches.lock().await.get(referential_name) {
            trace!(referential_name, "Retrieve entries from the cache");
            return Ok(Some(results.clone()));
        }

        Ok(None)
    }
    pub async fn set_cache(&self, referential_name: &String, values: &Vec<Value>) {
        let caches = CACHES.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let mut map = caches.lock_arc().await;
        if map.contains_key(referential_name) {
            return;
        }
        map.insert(referential_name.clone(), values.clone());
        trace!(referential_name, "create entries in the cache");
    }
    /// Return a HashMap of (string, values).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::step::referential::Referential;
    /// use chewdata::step::reader::Reader;
    /// use chewdata::connector::in_memory::InMemory;
    /// use chewdata::connector::{Connector, ConnectorType};
    /// use std::{io, collections::HashMap};
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
    ///     let mut hashmap = HashMap::default();
    ///     hashmap.insert("ref_1".to_string(), referential_1);
    ///     hashmap.insert("ref_2".to_string(), referential_2);
    ///     let referentials = Referential::new(hashmap);
    ///
    ///     let context = Context::new("step_main".to_string(), DataResult::Ok(Value::Null)).unwrap();
    ///
    ///     let values = referentials.to_value(&context).await?;
    ///     let values_expected:HashMap<String, Vec<Value>> = serde_json::from_str(r#"{"ref_1":[{"column1":"value1"}],"ref_2":[{"column1":"value2"}]}"#).unwrap();
    ///
    ///     assert_eq!(values_expected, values);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn to_value(&self, context: &Context) -> io::Result<HashMap<String, Vec<Value>>> {
        let mut referentials_vec = HashMap::new();

        for (name, reader) in &self.readers {
            if let Some(values) = &self.cache(name).await? {
                referentials_vec.insert(name.clone(), values.clone());
                continue;
            }

            let (sender_input, receiver_input) = async_channel::unbounded();
            let (sender_output, receiver_output) = async_channel::unbounded();

            sender_input
                .send(context.clone())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            sender_input.close();

            let mut task_referential = reader.clone();
            task_referential.name = name.clone();
            task_referential.set_receiver(receiver_input.clone());
            task_referential.set_sender(sender_output.clone());

            task::spawn(async move { task_referential.exec().await }).await?;
            sender_output.close();

            let values = receive(&receiver_output)
                .await
                .map(|context| context.input().to_value())
                .collect()
                .await;

            if !reader.connector_type.inner().is_variable() {
                self.set_cache(name, &values).await;
            }

            referentials_vec.insert(name.clone(), values);
        }

        Ok(referentials_vec)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connector::{in_memory::InMemory, ConnectorType},
        DataResult,
    };

    use super::*;

    #[async_std::test]
    async fn test_to_value() {
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
        let mut map = HashMap::default();
        map.insert("ref_1".to_string(), referential_1);
        map.insert("ref_2".to_string(), referential_2);

        let referential = Referential::new(&map);

        let context = Context::new("step_main".to_string(), DataResult::Ok(Value::Null));

        let values = referential.to_value(&context).await.unwrap();

        let values_expected: HashMap<String, Vec<Value>> = serde_json::from_str(
            r#"{"ref_1":[{"column1":"value1"},{"column1":"value2"}],"ref_2":[{"column1":"value3"},{"column1":"value4"}]}"#,
        )
        .unwrap();
        assert_eq!(values_expected, values);
    }
}
