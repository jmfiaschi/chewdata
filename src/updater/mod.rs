mod tera;
mod tera_helpers;

use self::tera::Tera;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::{fmt, io};

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Updater {
    #[serde(rename = "tera")]
    #[serde(alias = "t")]
    Tera(Tera),
}

impl Default for Updater {
    fn default() -> Self {
        Updater::Tera(Tera::default())
    }
}

impl Updater {
    pub fn get(self) -> Box<dyn Update> {
        match self {
            Updater::Tera(tera) => Box::new(tera),
            // Updater::Handlebars(handlebars) => Box::new(handlebars),
        }
    }
}

/// Trait to format a field of an object with a template engine and a template field.
pub trait Update: Send + Sync {
    /// Update the object with some mapping
    fn update(
        &self,
        object: Value,
        mapping: Option<HashMap<String, Vec<Value>>>,
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
    Merge,
    Replace,
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
