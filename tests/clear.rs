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
fn it_should_clear_one_file() {
    let config = r#"[{"type":"r","conn":{"type":"local","path":"./data/multi_lines.json"}},{"type":"w","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}}]"#;
    let output_file_path = "./data/out/clear_one_file.json";
    Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("CHEWDATA_APP_FILE_PATH_OUTPUT", &output_file_path)
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let config = r#"[{"type":"e","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}},{"type":"r","conn":{"type":"local","path":"./data/multi_lines.json"}},{"type":"w","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}},{"type":"w"}]"#;
    println!(
        "Clear the file and write again. Test this file is not empty '{}'.",
        output_file_path
    );
    let output = Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("CHEWDATA_APP_FILE_PATH_OUTPUT", &output_file_path)
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
    assert!(
        error_result.is_empty(),
        "stderr should be empty. {}.",
        error_result
    );
    assert!(
        !json_result.is_empty(),
        "stdout shouldn't be empty. {}",
        json_result
    );

    let config = r#"[{"type":"r","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}},{"type":"e","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}},{"type":"w","conn":{"type":"local","path":"{{ APP_FILE_PATH_OUTPUT }}"}},{"type":"w"}]"#;
    println!(
        "Read the file, clear the file and rewrite again. Test this file is empty '{}'.",
        output_file_path
    );
    let output = Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("CHEWDATA_APP_FILE_PATH_OUTPUT", &output_file_path)
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
    assert!(
        error_result.is_empty(),
        "stderr should be empty. {}.",
        error_result
    );
    assert!(
        !json_result.is_empty(),
        "stdout shouldn't be empty. {}",
        json_result
    );
}

#[test]
fn it_should_clear_dynamique_files() {
    let config = r#"[{"type":"r","alias":"mem","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}},{"type":"r","conn":{"type":"local","path":"./data/multi_lines.json"}},{"type":"w","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}}]"#;
    Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let config = r#"[{"type":"r","alias":"mem","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}},{"type":"e","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}},{"type":"r","conn":{"type":"local","path":"./data/multi_lines.json"}},{"type":"w","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}},{"type":"w"}]"#;
    println!("Clear the files and write again.");
    let output = Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
    assert!(
        error_result.is_empty(),
        "stderr should be empty. {}.",
        error_result
    );
    assert!(
        !json_result.is_empty(),
        "stdout shouldn't be empty. {}",
        json_result
    );

    let config = r#"[{"type":"r","alias":"mem","conn":{"type":"mem","data":"[{\"id\":1},{\"id\":2},{\"id\":3}]"}},{"type":"r","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}},{"type":"e","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}},{"type":"w","conn":{"type":"local","path":"./data/out/clear_multi_file_{{ steps.mem.id }}.json"}},{"type":"w"}]"#;
    println!("Read the files, clear the files and rewrite again. Test these files are empty.");
    let output = Command::new(debug_dir().join(APP_NAME))
        .args(&[config])
        .env("RUST_LOG", "null")
        .current_dir(repo_dir())
        .output()
        .expect("failed to execute process.");

    let json_result = String::from_utf8_lossy(output.stdout.as_slice());
    let error_result = String::from_utf8_lossy(output.stderr.as_slice());
    assert!(
        error_result.is_empty(),
        "stderr should be empty. {}.",
        error_result
    );
    assert!(
        !json_result.is_empty(),
        "stdout shouldn't be empty. {}",
        json_result
    );
}
