use regex::Regex;

pub trait JsonPointer {
    /// Retourn the json pointer
    fn to_json_pointer(&self) -> String;
}

impl JsonPointer for String {
    /// Transform a path_field to a json_pointer (json_path).
    /// Escape `.` with `\\` if the `.` is not separate two attribute names.
    ///
    /// # Examples
    ///
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
        let new_path = format!("/{}", self)
            .replace("][", "/")
            .replace(']', "")
            .replace(['['], "/")
            .replace("///", "/")
            .replace("//", "/");

        let re = Regex::new(r"([^\\])[.]").unwrap();
        let new_path = re
            .replace_all(new_path.as_str(), "$1/")
            .to_string()
            .replace("\\.", ".");

        new_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_json_pointer() {
        let field_path = "value.sub_value.0.array_value".to_string();
        let expected_result = "/value/sub_value/0/array_value";
        assert_eq!(expected_result, field_path.to_json_pointer());
        let field_path = "value.sub_value[0].array_value".to_string();
        assert_eq!(expected_result, field_path.to_json_pointer());
        let field_path = "value\\.a.value\\.b".to_string();
        let expected_result = "/value.a/value.b";
        assert_eq!(expected_result, field_path.to_json_pointer());
    }
}
