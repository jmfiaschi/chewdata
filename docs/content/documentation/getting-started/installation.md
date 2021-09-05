+++
title = "Installation"
weight = 10
+++

You can install Chewdata in two ways

## From the source code

Go the the Chewdata project in github and follow the [installation steps](https://github.com/jmfiaschi/chewdata#setup-from-source-code).

## From cargo

Requirement:
* [Rustup](https://www.rust-lang.org/tools/install)

First of all, to install the project with cargo you have diferent possibilities

Default installation
This command will install the project with de default feature.
```bash
cargo install chewdata
```

Installation with minimal features
If you need just read/write json file, transform them and store them into the local environment, this configuration is enough.
```bash
cargo install chewdata --no-default-features
```

Installation with custom features
If you want to specify some features to add to your installation
```bash
cargo install chewdata --no-default-features --features "use_xml_document use_bucket_connector"
```
Please, referer to the features page and choose your required feature.

### Custom the log level

If you need to change the log level of the command, you need to define it during the installation
Display `info` logs and highest severities
```bash
cargo install chewdata --no-default-features --features "slog/release_max_level_info"
echo '{"field1":"value1"}' | RUST_LOG=trace chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"io"}},{"type":"writer","document":{"type":"json"},"connector":{"type":"io"}}]'
```

Display `trace` logs and highest severities
```bash
cargo install chewdata --no-default-features --features "slog/release_max_level_trace"
echo '{"field1":"value1"}' | RUST_LOG=trace chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"io"}},{"type":"writer","document":{"type":"json"},"connector":{"type":"io"}}]'
```

List of possible features
* slog/release_max_level_off
* slog/release_max_level_error
* slog/release_max_level_warn
* slog/release_max_level_info
* slog/release_max_level_debug
* slog/release_max_level_trace
