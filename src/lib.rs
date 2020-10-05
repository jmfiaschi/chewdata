#[macro_use]
extern crate slog;
extern crate glob;
extern crate json_value_merge;
extern crate json_value_resolve;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate serde;
extern crate serde_json;

pub mod connector;
pub mod document_builder;
pub mod processor;
pub mod updater;

use self::processor::Data;
use self::processor::Processor;
use std::io;

pub fn exec(processors: Vec<Processor>) -> io::Result<()> {
    let mut data: Option<Data> = None;
    for processor in processors {
        let process = processor.to_owned().get();
        // skip the processor if it's not enable.
        if !process.is_enable() {
            continue;
        }

        data = process.exec(data)?.data
    }

    if let Some(generator) = data {
        // exec each generator.
        for _object in generator {}
    }

    Ok(())
}

/// Structure to transform field_path to a json_pointer.
pub struct FieldPath {
    path: String,
}

impl FieldPath {
    pub fn new(path: String) -> Self {
        FieldPath { path: path }
    }
    /// Transform a path_field to a json_pointer (json_path)
    ///
    /// # Example
    /// ```
    /// use chewdata::FieldPath;
    ///
    /// let field_path = FieldPath::new("value.sub_value.0.array_value".to_string());
    /// let pointer = field_path.to_json_pointer();
    /// assert_eq!("/value/sub_value/0/array_value", pointer);
    /// ```
    pub fn to_json_pointer(self) -> String {
        format!("/{}", self.path)
            .replace("][", "/")
            .replace("]", "")
            .replace("[", "/")
            .replace(".", "/")
            .replace("///", "/")
            .replace("//", "/")
    }
}
