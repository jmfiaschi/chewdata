+++
title = "Features"
weight = 2
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

Example of command in order to add features
```bash
cargo install chewdata --no-default-features --features "use_xml_document use_bucket_connector"
```
