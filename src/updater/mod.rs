mod tera;
pub mod tera_helpers;

use self::tera::Tera;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::{fmt, io};

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
pub trait Updater: Send + Sync {
    /// Update the object with some mapping
    fn update(
        &self,
        object: Value,
        context: Value,
        mapping: Option<HashMap<String, Vec<Value>>>,
        actions: Vec<Action>,
        input_name: String,
        output_name: String,
    ) -> io::Result<Value>;
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Action {
    #[serde(default = "default_field_value")]
    field: String,
    pattern: Option<String>,
    #[serde(rename = "type")]
    #[serde(default = "ActionType::merge")]
    action_type: ActionType,
}

/// Default field value link to the root object
fn default_field_value() -> String {
    "/".to_string()
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

impl fmt::Display for ActionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ActionType {
    fn merge() -> Self {
        ActionType::Merge
    }
}
