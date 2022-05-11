pub trait JsonPointer {
    /// Retourn the json pointer
    fn to_json_pointer(&self) -> String;
}

impl JsonPointer for String {
    /// Transform a path_field to a json_pointer (json_path)
    ///
    /// # Example
    /// ```
    /// use chewdata::helper::json_pointer::JsonPointer;
    ///
    /// let field_path = "value.sub_value.0.array_value".to_string();
    /// let expected_result = "/value/sub_value/0/array_value";
    /// assert_eq!(expected_result, field_path.to_json_pointer());
    ///
    /// let field_path = "value.sub_value[0].array_value".to_string();
    /// assert_eq!(expected_result, field_path.to_json_pointer());
    /// ```
    fn to_json_pointer(&self) -> String {
        format!("/{}", self)
            .replace("][", "/")
            .replace(']', "")
            .replace('[', "/")
            .replace('.', "/")
            .replace("///", "/")
            .replace("//", "/")
    }
}
