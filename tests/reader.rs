#[cfg(test)]
mod reader {
    use json_value_merge::Merge;
    use serde_json::Value;
    use std::env;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use std::process::Command;

    const APP_NAME: &str = "chewdata";
    const APP_ARG_FORMAT_JSON: &str = "json";
    #[test]
    fn it_should_read_file_in_local_with_one_line() {
        let config = r#"[{"type":"r","builder":{"type":"{{ APP_FORMAT_INPUT }}","connector":{"type":"local","path":"{{ APP_FILE_PATH_INPUT }}"}}},{"type":"w"}]"#;
        let formats = ["csv", "json", "jsonl", "yml", "xml"];
        for format in &formats {
            let data_file_path = format!("{}/{}.{}", "data", "one_line", format);
            println!("Try to test this file '{}'.", data_file_path);
            let output = Command::new(debug_dir().join(APP_NAME))
                .args(&[config, APP_ARG_FORMAT_JSON])
                .env("APP_FILE_PATH_INPUT", &data_file_path)
                .env("APP_FORMAT_INPUT", format)
                .env("RUST_LOG", "")
                .current_dir(repo_dir())
                .output()
                .expect("failed to execute process.");

            let json_result = String::from_utf8_lossy(output.stdout.as_slice());
            let error_result = String::from_utf8_lossy(output.stderr.as_slice());
            assert!(
                error_result.is_empty(),
                format!("stderr is not empty with this value {}.", error_result)
            );
            assert!(
                !json_result.is_empty(),
                format!("stdout should not be empty.")
            );
            let object_result: Value =
                serde_json::from_str(&json_result).expect("Parse json result failed.");

            let json_value_expected =
                data(format!("{}/{}.{}", "data", "one_line", "json").as_str());
            assert_eq!(json_value_expected.to_string(), object_result.to_string());
        }
    }
    #[test]
    fn it_should_read_file_in_local_with_multi_lines() {
        let config = r#"[{"type":"r","builder":{"type":"{{ APP_FORMAT_INPUT }}","connector":{"type":"local","path":"{{ APP_FILE_PATH_INPUT }}"}}},{"type":"w"}]"#;
        let formats = [
            "csv", "json", "jsonl", /*"yml", NOT SUPPORTED YET*/ "xml",
        ];
        for format in &formats {
            let data_file_path = format!("{}/{}.{}", "data", "multi_lines", format);
            println!("Try to test this file '{}'.", data_file_path);
            let output = Command::new(debug_dir().join(APP_NAME))
                .args(&[config, APP_ARG_FORMAT_JSON])
                .env("APP_FILE_PATH_INPUT", &data_file_path)
                .env("APP_FORMAT_INPUT", format)
                .env("RUST_LOG", "")
                .current_dir(repo_dir())
                .output()
                .expect("failed to execute process.");

            let json_result = String::from_utf8_lossy(output.stdout.as_slice());
            let error_result = String::from_utf8_lossy(output.stderr.as_slice());
            assert!(
                error_result.is_empty(),
                format!("stderr is not empty with this value {}.", error_result)
            );
            assert!(
                !json_result.is_empty(),
                format!("stdout should not be empty.")
            );
            let object_result: Value =
                serde_json::from_str(&json_result).expect("Parse json result failed.");

            let json_value_expected =
                data(format!("{}/{}.{}", "data", "multi_lines", "json").as_str());
            assert_eq!(json_value_expected.to_string(), object_result.to_string());
        }
    }
    #[test]
    fn it_should_read_multi_files_in_local() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/*.json"}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        let json_value_multi_lines_expected =
            data(format!("{}/{}.{}", "data", "multi_lines", "json").as_str());
        let json_value_one_line_expected =
            data(format!("{}/{}.{}", "data", "one_line", "json").as_str());

        let mut json_value_expected = Value::default();
        json_value_expected.merge(json_value_multi_lines_expected);
        json_value_expected.merge(json_value_one_line_expected);

        assert_eq!(json_value_expected.to_string(), object_result.to_string());
    }
    #[test]
    fn it_should_read_file_in_bucket_with_one_line() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"bucket","bucket":"my-bucket","path":"data/one_line.json","endpoint": "{{ BUCKET_ENDPOINT }}","access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}","secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}"}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("BUCKET_ENDPOINT", env::var("BUCKET_ENDPOINT").unwrap())
            .env(
                "BUCKET_ACCESS_KEY_ID",
                env::var("BUCKET_ACCESS_KEY_ID").unwrap(),
            )
            .env(
                "BUCKET_SECRET_ACCESS_KEY",
                env::var("BUCKET_SECRET_ACCESS_KEY").unwrap(),
            )
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        let json_value_expected = data(format!("{}/{}.{}", "data", "one_line", "json").as_str());
        assert_eq!(json_value_expected.to_string(), object_result.to_string());
    }
    #[test]
    fn it_should_read_file_in_bucket_with_multi_lines() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"bucket","bucket":"my-bucket","path":"data/multi_lines.json","endpoint": "{{ BUCKET_ENDPOINT }}","access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}","secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}"}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("BUCKET_ENDPOINT", env::var("BUCKET_ENDPOINT").unwrap())
            .env(
                "BUCKET_ACCESS_KEY_ID",
                env::var("BUCKET_ACCESS_KEY_ID").unwrap(),
            )
            .env(
                "BUCKET_SECRET_ACCESS_KEY",
                env::var("BUCKET_SECRET_ACCESS_KEY").unwrap(),
            )
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        let json_value_expected = data(format!("{}/{}.{}", "data", "multi_lines", "json").as_str());
        assert_eq!(json_value_expected.to_string(), object_result.to_string());
    }
    #[test]
    fn it_should_read_data_get_api() {
        println!("{:?}", env::var("CURL_ENDPOINT"));
        let config = r#"[{"type":"r","builder":{"type":"json","connector": {"type":"curl","method":"GET","endpoint":"{{ CURL_ENDPOINT }}","path":"/get"}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("CURL_ENDPOINT", env::var("CURL_ENDPOINT").unwrap())
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        assert!(object_result.pointer("/0/headers").is_some());
    }
    #[test]
    fn it_should_read_data_get_api_with_basic() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"curl","method":"GET","endpoint":"{{ CURL_ENDPOINT }}","path":"/basic-auth/my-username/my-password","authenticator":{"type": "basic","username":"{{ CURL_BASIC_AUTH_USERNAME }}","password":"{{ CURL_BASIC_AUTH_PASSWORD }}"}}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("CURL_ENDPOINT", env::var("CURL_ENDPOINT").unwrap())
            .env(
                "CURL_BASIC_AUTH_USERNAME",
                env::var("CURL_BASIC_AUTH_USERNAME").unwrap(),
            )
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        assert_eq!(
            r#"[{"authenticated":true,"user":"my-username"}]"#,
            object_result.to_string()
        );
    }
    #[test]
    fn it_should_read_data_get_api_with_bearer() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"curl","method":"GET","endpoint":"{{ CURL_ENDPOINT }}","path":"/bearer","authenticator":{"type": "bearer","token":"{{ CURL_BEARER_TOKEN }}"}}}},{"type":"w"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("CURL_ENDPOINT", env::var("CURL_ENDPOINT").unwrap())
            .env("CURL_BEARER_TOKEN", env::var("CURL_BEARER_TOKEN").unwrap())
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            error_result.is_empty(),
            format!("stderr is not empty with this value {}.", error_result)
        );
        assert!(
            !json_result.is_empty(),
            format!("stdout should not be empty.")
        );
        let object_result: Value =
            serde_json::from_str(&json_result).expect("Parse json result failed.");

        assert_eq!(
            r#"[{"authenticated":true,"token":"abcd1234"}]"#,
            object_result.to_string()
        );
    }
    fn data(data_path: &str) -> Value {
        let mut json_expected_file =
            File::open(data_path).expect(format!("File '{}' not found.", data_path).as_str());
        let mut json_expected_string = String::default();
        json_expected_file
            .read_to_string(&mut json_expected_string)
            .expect(format!("Can't read the file '{}'.", data_path).as_str());
        serde_json::from_str(json_expected_string.as_ref())
            .expect(format!("Can't deserialize the data. {}", json_expected_string).as_str())
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
