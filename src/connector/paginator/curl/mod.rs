pub mod cursor;
pub mod offset;

use cursor::Cursor;
use offset::Offset;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum PaginatorType {
    #[serde(alias = "offset")]
    Offset(Offset),
    #[serde(rename = "cursor")]
    Cursor(Cursor),
}

impl Default for PaginatorType {
    fn default() -> Self {
        PaginatorType::Offset(Offset::default())
    }
}