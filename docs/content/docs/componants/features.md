+++
title = "Features"
description = "List of features that you can use"
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 500
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "List of features that you can use"
toc = true
top = false
+++

List of available features that you can enable during the installation

|   Name                    |   Description                                                             |   Componant                     |
|:--------------------------|:--------------------------------------------------------------------------|:--------------------------------|
|   `use_xml_document`      |   Add the possibility to read and write xml documents                     |   document/xml                  |
|   `use_toml_document`     |   Add the possibility to read and write toml documents                    |   document/toml                 |
|   `use_csv_document`      |   Add the possibility to read and write csv documents                     |   document/csv                  |
|   `use_bucket_connector`  |   Add the possibility to read and write documents with bucket minio/s3    |   connector/bucket et connector/bucket_select   |
|   `use_curl_connector`    |   Add the possibility to read and write documents with APIs               |   connector/curl                 |
|   `use_mongodb_connector` |   Add the possibility to read and write documents with mongodb            |   connector/mongodb                 |
|   `slog/release_max_level_[LOG_LEVEL]` |  Enable log level, LOG_LEVEL: [ off / error / warn / info / debug / trace ] |   -   |

Example of command in order to add features
```bash
cargo install chewdata --no-default-features --features "use_xml_document use_bucket_connector"
```

## Custom the log level

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

