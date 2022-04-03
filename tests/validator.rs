use json_value_resolve::Resolve;
use serde_json::Value;
use std::env;
use std::path::PathBuf;
use std::process::Command;

const APP_NAME: &str = "chewdata";

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

#[test]
fn it_should_apply_simple_validation() {
    let patterns = [(
        "number_rule",
        "{% if input.number == 10  %} true {% else %} false {% endif %}",
        "{{ rule.name }}: The value '{{ input.number }}' is not equal to 10",
        "number_rule: The value '20' is not equal to 10"
    ),
    (
        "matching_rule",
        "{% if input.rename_this is matching('.*renamed 2') %} true {% else %} false {% endif %}",
        "{{ rule.name }}: The value '{{ input.rename_this }}' not match with '.*renamed 2'",
        "matching_rule: The value 'field must be renamed' not match with '.*renamed 2'"
    ),
    (
        "mapping_rule",
        "{% if mapping_ref | filter(attribute='mapping_code', value=input.code) | length > 0 %} true {% else %} false {% endif %}",
        "{{ rule.name }}: The value '{{ input.code }}' not map with the referencial",
        "mapping_rule: The value 'value_to_map_2' not map with the referencial"
    ),];
    let configs = [(
        "tera",
        r#"[{"type":"r","conn":{"type":"local","path":"./data/multi_lines.json"}},{"type":"v","updater":{"type":"{{ TEMPLATE_ENGINE }}"},"rules":{"{{ RULE_NAME }}":{"pattern":"{{ RULE_PATTERN_TERA }}","message": "{{ RULE_PATTERN_MESSAGE }}"}},"refs":{"mapping_ref":{"connector":{"type":"local","path":"./data/mapping.json"}}}},{"type":"w","data_type":"err"}]"#,
    )];
    patterns
        .iter()
        .for_each(|(rule_name, rule_pattern, rule_message, expected_error)| {
            configs.iter().for_each(|(template_engine, config)| {
                println!(
                    "Test the rule '{}' with the template '{}'.",
                    rule_name, template_engine
                );
                let output = Command::new(debug_dir().join(APP_NAME))
                    .args(&[config])
                    .env("TEMPLATE_ENGINE", template_engine)
                    .env("RULE_NAME", rule_name)
                    .env("RULE_PATTERN_TERA", rule_pattern)
                    .env("RULE_PATTERN_MESSAGE", rule_message)
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
                assert!(!json_result.is_empty(), "stdout should not be empty.");

                let object_result: Value =
                    serde_json::from_str(&json_result).expect("Parse json result failed.");

                let value = object_result
                    .get(0)
                    .expect("The result should begin with a json array.")
                    .get("_error")
                    .unwrap_or_else(|| panic!("Should have a field '_error'."));

                assert_eq!(
                    &Value::resolve(expected_error.to_string()),
                    value,
                    "Tested with the template engine '{}'.",
                    template_engine
                );
            });
        });
}
