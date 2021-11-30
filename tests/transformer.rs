#[cfg(test)]
mod transformer {
    use chrono::Utc;
    use json_value_resolve::Resolve;
    use serde_json::Value;
    use std::env;
    use std::path::PathBuf;
    use std::process::Command;

    const APP_NAME: &str = "chewdata";
    #[test]
    fn it_should_apply_simple_transformation() {
        let now = Utc::now().format("%Y-%m-%d").to_string();
        let patterns = [
            ("number", "{{ input.number * 10 }}", "100"),
            ("string", "{{ input.string }} done", "value to test done"),
            (
                "long-string",
                "{{ input['long-string'] }} done",
                "Long val\nto test done",
            ),
            (
                "boolean",
                "{% if input.boolean %}{{ false | safe }}{% endif %}",
                "false",
            ),
            ("special_char", "{{ input.special_char }} good", "é good"),
            ("new_field", "new field added", "new field added"),
            (
                "field_renamed",
                "{{ input.rename_this }}",
                "field must be renamed",
            ),
            (
                "date",
                "{{ input.date | date(format='%d-%m-%Y') }}",
                "31-12-2019",
            ),
            (
                "filesize",
                "{{ input.filesize | filesizeformat }}",
                "976.56 KB",
            ),
            (
                "round",
                "{{ input.round | round(method='floor', precision=2) }}",
                "10.15",
            ),
            (
                "url",
                "{{ input.url | urlencode }}",
                "%3Fsearch%3Dtest%20me",
            ),
            (
                "list_to_sort",
                "{{ input.list_to_sort | split(pat=',') | reverse | join(sep=',') }}",
                "C,B,A",
            ),
            (
                "now",
                "{{ now(timestamp=false, utc=true) | date(format='%Y-%m-%d') }}",
                now.as_str(),
            ),
            (
                "object",
                r#"{\"field1\":\"value1\"}"#,
                r#"{"field1":"value1"}"#,
            ),
            ("remove_field", r#"null"#, r#"null"#),
        ];
        let configs = [(
            "tera",
            r#"[{"type":"r", "connector":{"type":"local","path":"./data/one_line.json"}},{"type": "t","updater": {"type":"{{ TEMPLATE_ENGINE }}"},"actions":[{"field":"{{ FIELD_NAME }}","pattern":"{{ FIELD_PATTERN_TERA }}"}]},{"type": "w"}]"#,
        )];
        patterns
            .iter()
            .for_each(|(field_name, pattern_tera, expected_value)| {
                configs.iter().for_each(|(template_engine, config)| {
                    println!(
                        "Test the field '{}' with the template '{}'.",
                        field_name, template_engine
                    );
                    let output = Command::new(debug_dir().join(APP_NAME))
                        .args(&[config])
                        .env("TEMPLATE_ENGINE", template_engine)
                        .env("FIELD_NAME", field_name)
                        .env("FIELD_PATTERN_TERA", pattern_tera)
                        .env("RUST_LOG", "null")
                        .current_dir(repo_dir())
                        .output()
                        .expect("failed to execute process.");

                    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                    assert!(
                        error_result.is_empty(),
                        "stderr is not empty with this value {}.",
                        error_result
                    );
                    assert!(
                        !json_result.is_empty(),
                        "stdout should not be empty."
                    );
                    let object_result: Value =
                        serde_json::from_str(&json_result).expect("Parse json result failed.");
                    let value = object_result
                        .get(0)
                        .expect("The result should begin with an array.")
                        .get(field_name)
                        .unwrap_or_else(|| panic!("Should have a '{}'.", field_name));

                    assert_eq!(
                        &Value::resolve(expected_value.to_string()),
                        value,
                        "Tested with the template engine '{}'.",
                        template_engine
                    );
                });
            });
    }
    #[test]
    fn it_should_replace_output_with_current_input() {
        let patterns = [(
            "/",
            "./data/one_line.json",
            "{{ input | json_encode() }}",
            r#"[{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date": "2019-12-31","filesize":1000000,"round": 10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove"}]"#,
        )];
        let configs = [(
            "tera",
            r#"[{"type":"r","connector":{"type":"local","path":"{{ INPUT_FILE_PATH }}"}},{"type":"t","updater": {"type":"{{ TEMPLATE_ENGINE }}"},"actions":[{"field":"{{ FIELD_NAME }}","pattern":"{{ FIELD_PATTERN_TERA }}"}]},{"type":"w"}]"#,
        )];
        patterns
            .iter()
            .for_each(|(field_name, input_file, pattern_tera, expected_value)| {
                configs.iter().for_each(|(template_engine, config)| {
                    println!(
                        "Test the field '{}' with the template '{}'.",
                        field_name, template_engine
                    );
                    let output = Command::new(debug_dir().join(APP_NAME))
                        .args(&[config])
                        .env("TEMPLATE_ENGINE", template_engine)
                        .env("FIELD_NAME", field_name)
                        .env("FIELD_PATTERN_TERA", pattern_tera)
                        .env("INPUT_FILE_PATH", input_file)
                        .env("RUST_LOG", "null")
                        .current_dir(repo_dir())
                        .output()
                        .expect("failed to execute process.");

                    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                    assert!(
                        error_result.is_empty(),
                        "stderr is not empty with this value {}.",
                        error_result
                    );
                    assert!(
                        !json_result.is_empty(),
                        "stdout should not be empty."
                    );

                    let object_result: Value =
                        serde_json::from_str(&json_result).expect("Parse json result failed.");
                    println!(
                        "expected {:?} , result {:?}",
                        &Value::resolve(expected_value.to_string()),
                        json_result.clone()
                    );
                    assert_eq!(
                        Value::resolve(expected_value.to_string()),
                        object_result,
                        "Tested with the template engine '{}'.",
                        template_engine
                    );
                });
            });
    }
    #[test]
    fn it_should_apply_complex_transformation() {
        let patterns = [
            (
                "object_merged",
                "{{ output.object1 | merge(with=output.object2) | json_encode() }}",
                r#"{"field1":"value1","field2":"value2"}"#,
            ),
            (
                "object_merged_in",
                "{{ output.object1 | merge(with=output.object2, in='/other') | json_encode() }}",
                r#"{"field1":"value1","other":{"field2":"value2"}}"#,
            ),
        ];
        let configs = [(
            "tera",
            r#"[{"type":"r","connector":{"type":"local","path":"./data/one_line.json"}},{"type":"t","updater": {"type":"{{ TEMPLATE_ENGINE }}"},"actions":[{"field":"object1","pattern":"{\"field1\":\"value1\"}"},{"field":"object2","pattern":"{\"field2\":\"value2\"}"},{"field":"{{ FIELD_NAME }}","pattern":"{{ FIELD_PATTERN_TERA }}"}]},{"type":"w"}]"#,
        )];
        patterns
            .iter()
            .for_each(|(field_name, pattern_tera, expected_value)| {
                configs.iter().for_each(|(template_engine, config)| {
                    println!(
                        "Test the field '{}' with the template '{}'.",
                        field_name, template_engine
                    );
                    let output = Command::new(debug_dir().join(APP_NAME))
                        .args(&[config])
                        .env("TEMPLATE_ENGINE", template_engine)
                        .env("FIELD_NAME", field_name)
                        .env("FIELD_PATTERN_TERA", pattern_tera)
                        .env("RUST_LOG", "null")
                        .current_dir(repo_dir())
                        .output()
                        .expect("failed to execute process.");

                    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                    assert!(
                        error_result.is_empty(),
                        "stderr is not empty with this value {}.",
                        error_result
                    );
                    assert!(
                        !json_result.is_empty(),
                        "stdout should not be empty."
                    );
                    let object_result: Value =
                        serde_json::from_str(&json_result).expect("Parse json result failed.");
                    let value = object_result
                        .get(0)
                        .expect("The result should begin with an array.")
                        .get(field_name)
                        .unwrap_or_else(|| panic!("Should have a '{}'.", field_name));

                    assert_eq!(
                        &Value::resolve(expected_value.to_string()),
                        value,
                        "Tested with the template engine '{}'.",
                        template_engine
                    );
                });
            });
    }
    #[test]
    fn it_should_apply_transformation_with_mapping() {
        let patterns = [(
            "mapping",
            r#"{{ alias_mapping | filter(attribute=\"mapping_code\", value=input.code) | first | get(key=\"mapping_value\") }}"#,
            r#"value mapped"#,
        )];
        let configs = [(
            "tera",
            r#"[{"type":"r","connector":{"type":"local","path":"./data/one_line.json"}},{"type":"t","updater":{"type":"{{ TEMPLATE_ENGINE }}"},"actions":[{"field":"{{ FIELD_NAME }}","pattern":"{{ FIELD_PATTERN_TERA }}"}],"refs":{"alias_mapping":{"connector":{"type":"local","path":"./data/mapping.json"}}}},{"type": "w"}]"#,
        )];
        patterns
            .iter()
            .for_each(|(field_name, pattern_tera, expected_value)| {
                configs.iter().for_each(|(template_engine, config)| {
                    println!(
                        "Test the field '{}' with the template '{}'.",
                        field_name, template_engine
                    );
                    let output = Command::new(debug_dir().join(APP_NAME))
                        .args(&[config])
                        .env("TEMPLATE_ENGINE", template_engine)
                        .env("FIELD_NAME", field_name)
                        .env("FIELD_PATTERN_TERA", pattern_tera)
                        .env("RUST_LOG", "null")
                        .current_dir(repo_dir())
                        .output()
                        .expect("failed to execute process.");

                    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                    assert!(
                        error_result.is_empty(),
                        "stderr is not empty with this value {}.",
                        error_result
                    );
                    assert!(
                        !json_result.is_empty(),
                        "stdout should not be empty."
                    );
                    let object_result: Value =
                        serde_json::from_str(&json_result).expect("Parse json result failed.");
                    let value = object_result
                        .get(0)
                        .expect("The result should begin with an array.")
                        .get(field_name)
                        .unwrap_or_else(|| panic!("Should have a '{}'.", field_name));

                    assert_eq!(
                        &Value::resolve(expected_value.to_string()),
                        value,
                        "Tested with the template engine '{}'.",
                        template_engine
                    );
                });
            });
    }
    #[test]
    fn it_should_throw_an_error() {
        let patterns = [(
            "_error",
            "{{ throw(message='I want to throw an error') }}",
            r#"Failed to render the field '/my_field'. I want to throw an error."#,
        )];
        let configs = [(
            "tera",
            r#"[{"type":"r","connector":{"type":"local","path":"./data/one_line.json"}},{"type":"t","updater":{"type":"{{ TEMPLATE_ENGINE }}"},"actions":[{"field":"/my_field","pattern":"{{ FIELD_PATTERN_TERA }}"}]},{"type":"w","data_type": "err"}]"#,
        )];
        patterns
            .iter()
            .for_each(|(field_name, pattern_tera, expected_value)| {
                configs.iter().for_each(|(template_engine, config)| {
                    println!(
                        "Test the field '{}' with the template '{}'.",
                        field_name, template_engine
                    );
                    let output = Command::new(debug_dir().join(APP_NAME))
                        .args(&[config])
                        .env("TEMPLATE_ENGINE", template_engine)
                        .env("FIELD_NAME", field_name)
                        .env("FIELD_PATTERN_TERA", pattern_tera)
                        .env("RUST_LOG", "null")
                        .current_dir(repo_dir())
                        .output()
                        .expect("failed to execute process.");

                    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                    assert!(
                        error_result.is_empty(),
                        "stderr should be empty {}.",
                        error_result
                    );
                    assert!(
                        !json_result.is_empty(),
                        "stdout should not be empty."
                    );

                    let object_result: Value =
                        serde_json::from_str(&json_result).expect("Parse json result failed.");
                    let value = object_result
                        .get(0)
                        .expect("The result should begin with an array.")
                        .get(field_name)
                        .unwrap_or_else(|| panic!("Should have a '{}'.", field_name));

                    assert_eq!(
                        &Value::resolve(expected_value.to_string()),
                        value,
                        "Tested with the template engine '{}'.",
                        template_engine
                    );
                });
            });
    }
    /// Return the repo root directory path.
    fn repo_dir() -> PathBuf {
        debug_dir()
            .parent()
            .expect("target directory path.")
            .parent()
            .expect("repo directory path.")
            .to_path_buf()
    }
    /// Return the target/debug directory path.
    fn debug_dir() -> PathBuf {
        env::current_exe()
            .expect("target/debug/deps/binary path.")
            .parent()
            .expect("target/debug/deps directory path.")
            .parent()
            .expect("target/debug directory path.")
            .to_path_buf()
    }
}
