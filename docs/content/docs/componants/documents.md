+++
title = "Documents"
description = "Document that you can use"
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 400
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "Documents that you can use"
toc = true
top = false
+++

### Metadata

By default, the metadata is manage with the document metadata but you can override it if necessary

| key          | Description                                   | Default Value | Possible Values                                                                                               | Example                                                                                      |
| ------------ | --------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| has_headers  | Define if the document contain a header entry | `none`        | `true` \ `false`                                                                                              | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| delimiter    | Special character to seperate fields          | `none`        | String                                                                                                        | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| quote        | Special character used for quote              | `none`        | String                                                                                                        | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| escape       | Special character used for escape             | `none`        | String                                                                                                        | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| comment      | Special character used for comment            | `none`        | String                                                                                                        | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| terminator   | Special character used to define new line     | `none`        | String                                                                                                        | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| mime_type    | Mime type of the document                     | `none`        | `application` / [etc...](https://developer.mozilla.org/fr/docs/Web/HTTP/Basics_of_HTTP/MIME_types)            | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| mime_subtype |                                               | `none`        | `plain` / `octet-stream` / [etc...](https://developer.mozilla.org/fr/docs/Web/HTTP/Basics_of_HTTP/MIME_types) | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| charset      |                                               | `none`        | `utf-8` / `ISO-8859-1` / etc...                                                                               | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| compression  |                                               | `none`        | String                                                                                                        | -                                                                                            |
| language     |                                               | `none`        | String                                                                                                        | -                                                                                            |
