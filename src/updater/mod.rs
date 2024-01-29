mod tera;
pub mod tera_helpers;

use self::tera::Tera;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io;

pub const INPUT_FIELD_KEY: &str = "input";
pub const OUPUT_FIELD_KEY: &str = "output";
pub const CONTEXT_FIELD_KEY: &str = "context";

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum UpdaterType {
    #[serde(rename = "tera")]
    #[serde(alias = "t")]
    Tera(Tera),
}

impl Default for UpdaterType {
    fn default() -> Self {
        UpdaterType::Tera(Tera::default())
    }
}

impl UpdaterType {
    pub fn updater_inner(self) -> Box<dyn Updater> {
        match self {
            UpdaterType::Tera(updater) => Box::new(updater),
        }
    }
    pub fn updater(&self) -> &dyn Updater {
        match self {
            UpdaterType::Tera(ref updater) => updater,
        }
    }
    pub fn updater_mut(&mut self) -> &mut dyn Updater {
        match *self {
            UpdaterType::Tera(ref mut updater) => updater,
        }
    }
}

/// Trait to format a field of an object with a template engine and a template field.
#[async_trait]
pub trait Updater: Send + Sync {
    /// Update the object with some mapping
    async fn update(
        &self,
        object: &Value,
        context: &Value,
        mapping: &HashMap<String, Vec<Value>>,
        actions: &[Action],
    ) -> io::Result<Value>;
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Action {
    #[serde(default = "default_field_value")]
    pub field: String,
    #[serde(default = "default_pattern_value")]
    pub pattern: Option<String>,
    #[serde(rename = "type")]
    #[serde(default = "ActionType::merge")]
    pub action_type: ActionType,
}

fn default_field_value() -> String {
    "/".to_string()
}

fn default_pattern_value() -> Option<String> {
    Some("{{ input | json_encode() }}".to_string())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ActionType {
    #[serde(alias = "merge")]
    Merge,
    #[serde(alias = "replace")]
    Replace,
    #[serde(alias = "remove")]
    Remove,
}

impl ActionType {
    fn merge() -> Self {
        ActionType::Merge
    }
}
