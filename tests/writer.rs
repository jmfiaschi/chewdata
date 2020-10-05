#[cfg(test)]
mod writer {
    use chrono::Utc;
    use std::env;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use std::process::Command;

    const APP_NAME: &str = "chewdata";
    const APP_ARG_FORMAT_JSON: &str = "json";
    #[test]
    fn it_should_write_file_in_local_with_one_line() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"w","builder":{"type":"{{ APP_FORMAT_OUTPUT }}","pretty":true,"connector":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}","truncate":true}}}]"#;
        let formats = ["csv", "json", "jsonl", "yml", "xml"];
        for format in &formats {
            let output_file_path = format!("{}/{}.{}", "data/out", "one_line", format);
            println!("Try to test this file '{}'.", output_file_path);
            let output = Command::new(debug_dir().join(APP_NAME))
                .args(&[config, APP_ARG_FORMAT_JSON])
                .env("APP_FILE_PATH_OUTPUT", &output_file_path)
                .env("APP_FORMAT_OUTPUT", format)
                .env("RUST_LOG", "")
                .current_dir(repo_dir())
                .output()
                .expect("failed to execute process.");

            let json_result = String::from_utf8_lossy(output.stdout.as_slice());
            let error_result = String::from_utf8_lossy(output.stderr.as_slice());
            assert!(
                error_result.is_empty(),
                format!("stderr should be empty. {}.", error_result)
            );
            assert!(
                json_result.is_empty(),
                format!("stdout should be empty. {}", json_result)
            );

            let value_result = data(&output_file_path);
            let value_expected = data(format!("{}/{}.{}", "data", "one_line", format).as_str());
            assert_eq!(value_expected.to_string(), value_result.to_string());
        }
    }
    #[test]
    fn it_should_write_file_in_local_with_multi_lines() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/multi_lines.json"}}},{"type":"w","builder":{"type":"{{ APP_FORMAT_OUTPUT }}","pretty":true,"connector":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}","truncate":true}}}]"#;
        let formats = ["csv", "json", "jsonl", "yml", "xml"];
        for format in &formats {
            let output_file_path = format!("{}/{}.{}", "data/out", "multi_lines", format);
            println!("Try to test this file '{}'.", output_file_path);
            let output = Command::new(debug_dir().join(APP_NAME))
                .args(&[config, APP_ARG_FORMAT_JSON])
                .env("APP_FILE_PATH_OUTPUT", &output_file_path)
                .env("APP_FORMAT_OUTPUT", format)
                .env("RUST_LOG", "")
                .current_dir(repo_dir())
                .output()
                .expect("failed to execute process.");

            let json_result = String::from_utf8_lossy(output.stdout.as_slice());
            let error_result = String::from_utf8_lossy(output.stderr.as_slice());
            assert!(
                error_result.is_empty(),
                format!("stderr should be empty. {}.", error_result)
            );
            assert!(
                json_result.is_empty(),
                format!("stdout should be empty. {}", json_result)
            );

            let value_result = data(&output_file_path);
            let value_expected = data(format!("{}/{}.{}", "data", "multi_lines", format).as_str());
            assert_eq!(value_expected.to_string(), value_result.to_string());
        }
    }
    #[test]
    fn it_should_read_data_call_api_200() {
        [("POST","/post"),("PUT","/put"),("PATCH","/patch"),("DELETE","/delete")].iter().for_each(|(method, path)| {
            println!("Try to call '{} {}'.", method, path);
            let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"w","builder":{"type":"json","connector": {"type":"curl","method":"{{ METHOD }}","endpoint":"{{ CURL_ENDPOINT }}","path":"{{ PATH }}"}}}]"#;
            let output = Command::new(debug_dir().join(APP_NAME))
                .args(&[config,APP_ARG_FORMAT_JSON])
                .env("CURL_ENDPOINT", env::var("CURL_ENDPOINT").unwrap())
                .env("METHOD", method)
                .env("PATH", path)
                .env("RUST_LOG","")
                .current_dir(repo_dir())
                .output()
                .expect("failed to execute process.");

            let json_result = String::from_utf8_lossy(output.stdout.as_slice());
            let error_result = String::from_utf8_lossy(output.stderr.as_slice());
            assert!(error_result.is_empty(), format!("stderr should be empty. {}.", error_result));
            assert!(json_result.is_empty(), format!("stdout should be empty. {}", json_result));
        });
    }
    #[test]
    fn it_should_read_data_call_api_4xx() {
        ["POST","PUT","PATCH","DELETE"].iter().for_each(|method| {
            ["400","401","404","500"].iter().for_each(|status| {
                println!("Try to call '{} /status/{}'.", method, status);
                let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","paths":"./data/one_line.json"}}},{"type":"w","builder":{"type":"json","connector": {"type":"curl","method":"{{ METHOD }}","endpoint":"{{ CURL_ENDPOINT }}","path":"/status/{{ STATUS }}"}}}]"#;
                let output = Command::new(debug_dir().join(APP_NAME))
                    .args(&[config,APP_ARG_FORMAT_JSON])
                    .env("CURL_ENDPOINT", env::var("CURL_ENDPOINT").unwrap())
                    .env("METHOD", method)
                    .env("STATUS", status)
                    .env("RUST_LOG","")
                    .current_dir(repo_dir())
                    .output()
                    .expect("failed to execute process.");

                let json_result = String::from_utf8_lossy(output.stdout.as_slice());
                let error_result = String::from_utf8_lossy(output.stderr.as_slice());
                assert!(!error_result.is_empty(), format!("stderr should not be empty. {}.", error_result));
                assert!(json_result.is_empty(), format!("stdout should be empty. {}", json_result));
            });
        });
    }
    #[test]
    fn it_should_write_file_with_dynamic_name() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"now","pattern":"{{ now(timestamp=false, utc=true) | date(format='%Y%m%d') }}"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/{{ now }}.json","truncate":true}}}]"#;
        let output_file_path = format!("{}/{}.{}", "data/out", Utc::now().format("%Y%m%d"), "json");
        println!("Try to test this file '{}'.", output_file_path);
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
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let value_result = data(&output_file_path);
        assert_eq!(
            format!(r#"[{{"now":{}}}]"#, Utc::now().format("%Y%m%d")),
            value_result.to_string()
        );
    }
    #[test]
    fn it_should_truncate_the_file() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"field1","pattern":"value1"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/truncate_file.json","truncate":true}}}]"#;
        let output_file_path = format!("{}/{}.{}", "data/out", "truncate_file", "json");
        println!("Try to test this file '{}'.", output_file_path);
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
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"field2","pattern":"value2"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/truncate_file.json","truncate":true}}}]"#;
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
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let value_result = data(&output_file_path);
        assert_eq!(r#"[{"field2":"value2"}]"#, value_result.to_string());
    }
    #[test]
    fn it_should_not_truncate_the_file() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"field1","pattern":"value1"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/no_truncate_file.json","truncate":true}}}]"#;
        let output_file_path = format!("{}/{}.{}", "data/out", "no_truncate_file", "json");
        println!("Try to test this file '{}'.", output_file_path);
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
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/one_line.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"field2","pattern":"value2"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/no_truncate_file.json","truncate":false}}}]"#;
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
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let value_result = data(&output_file_path);
        assert_eq!(
            r#"[{"field1":"value1"},{"field2":"value2"}]"#,
            value_result.to_string()
        );
    }
    #[test]
    fn it_should_chain_writers() {
        let config = r#"[{"type":"r","builder":{"type":"json","connector":{"type":"local","path":"./data/multi_lines.json"}}},{"type":"t","updater":{"type":"tera","actions":[{"field":"/","pattern":"{% if input.number == 10 %}{{ throw(message='data go to writer.cascade_file2.json') }}{% else %}{{ input | json_encode() }}{% endif %}"}]}},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/cascade_file1.json","truncate":true}},"data_type":"ok"},{"type":"w","builder":{"type":"json","connector":{"type":"local","path":"./data/out/cascade_file2.json","truncate":true}},"data_type":"err"}]"#;
        let output = Command::new(debug_dir().join(APP_NAME))
            .args(&[config, APP_ARG_FORMAT_JSON])
            .env("RUST_LOG", "")
            .current_dir(repo_dir())
            .output()
            .expect("failed to execute process.");

        let json_result = String::from_utf8_lossy(output.stdout.as_slice());
        let error_result = String::from_utf8_lossy(output.stderr.as_slice());
        assert!(
            !error_result.is_empty(),
            format!("stderr should not be empty {}.", error_result)
        );
        assert!(
            json_result.is_empty(),
            format!("stdout should be empty. {}", json_result)
        );

        let output_file1_path = format!("{}/{}.{}", "data/out", "cascade_file1", "json");
        println!("Try to test this file '{}'.", output_file1_path);

        let value_result1 = data(&output_file1_path);
        assert_eq!(
            r#"[{"number":20,"group":1456,"string":"value to test 2","long-string":"Long val\nto test 2","boolean":true,"special_char":"à","rename_this":"field must be renamed 2","date":"2020-12-31","filesize":2000000,"round":10.12,"url":"?search=test me 2","list_to_sort":"D,E,F","code":"value_to_map_2","remove_field":"field to remove 2"},{"number":30,"group":1456,"string":"value to test 3","long-string":"Long val\nto test 3","boolean":true,"special_char":"€","rename_this":"field must be renamed 3","date":"2018-12-31","filesize":5000000,"round":100.1,"url":"?search=test me 3","list_to_sort":"G,H,I","code":"value_to_map_3","remove_field":"field to remove 3"}]"#,
            value_result1.to_string()
        );

        let output_file2_path = format!("{}/{}.{}", "data/out", "cascade_file2", "json");
        println!("Try to test this file '{}'.", output_file2_path);
        let value_result2 = data(&output_file2_path);
        assert_eq!(
            r#"[{"number":10,"group":1456,"string":"value to test","long-string":"Long val\nto test","boolean":true,"special_char":"é","rename_this":"field must be renamed","date":"2019-12-31","filesize":1000000,"round":10.156,"url":"?search=test me","list_to_sort":"A,B,C","code":"value_to_map","remove_field":"field to remove","_error":"Failed to render the field '/'. data go to writer.cascade_file2.json"}]"#,
            value_result2.to_string()
        );
    }
    fn data(data_path: &str) -> String {
        let mut json_expected_file =
            File::open(data_path).expect(format!("File '{}' not found.", data_path).as_str());
        let mut json_expected_string = String::default();
        json_expected_file
            .read_to_string(&mut json_expected_string)
            .expect(format!("Can't read the file '{}'.", data_path).as_str());
        json_expected_string
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
