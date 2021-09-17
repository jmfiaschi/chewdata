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
lead = "Read/Write data with the good document format"
toc = true
top = false
+++

## CSV

| key         | alias | Description                                                                                                     | Default Value | Possible Values                                  |
| ----------- | ----- | --------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------ |
| type        | -     | Required in order to use this document                                                                          | `csv`         | `csv`                                            |
| metadata    | meta  | Metadata describe the resource                                                                                  | `none`        | [Metadata](#metadata)                            |
| is_flexible | -     | If flexible is true, the application try to match the number of header's fields and the number of line's fields | `true`        | `true` / `false`                                 |
| quote_style | -     | The quoting style to use when writing CSV                                                                       | `NOT_NUMERIC` | `NOT_NUMERIC` / `ALWAYS` / `NEVER` / `NECESSARY` |
| trim        | -     | Define where you trim the data. The application can trimmed fields, headers or both                             | `ALL`         | `ALL` / `FIELDS` / `HEADERS`                     |

examples:

```json
[
    {
        "type": "write",
        "document": {
            "type": "csv",
            "is_flexible": true,
            "quote_style": "NOT_NUMERIC",
            "trim": "ALL",
            "metadata": {
                "has_headers": true,
                "delimiter": ",",
                "quote": "\"",
                "escape": "\\",
                "comment": "#",
                "terminator": "\n"
            }
        }
    }
]
```

input/output:

```csv
"column1","column2",...
"value1","value2",...
...
```

## Json

| key        | alias | Description                                                           | Default Value | Possible Values                                                                |
| ---------- | ----- | --------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------ |
| type       | -     | Required in order to use this document                                | `json`        | `json`                                                                         |
| metadata   | meta  | Metadata describe the resource                                        | `none`        | [Metadata](#metadata)                                                          |
| is_pretty  | -     | Display json data readable for human                                  | `false`       | `false` / `true`                                                               |
| entry_path | -     | Use this field if you want target a specific field in the json object | `none`        | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "json",
            "is_pretty": true,
            "entry_path": "/my_root/array_field/0/my_field"
        }
    }
]
```

input/output:

```json
[{
    "field":"value",
    ...
},
...]
```

## Jsonl

| key        | alias | Description                                                      | Default Value | Possible Values                                                                |
| ---------- | ----- | ---------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------ |
| type       | -     | Required in order to use this document                           | `jsonl`       | `jsonl`                                                                        |
| metadata   | meta  | Metadata describe the resource                                   | `none`        | [Metadata](#metadata)                                                          |
| is_pretty  | -     | Display data in readable format for human                        | `false`       | `false` / `true`                                                               |
| entry_path | -     | Use this field if you want target a specific field in the object | `none`        | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "json",
            "is_pretty": true,
            "entry_path": "/my_root/array_field/0/my_field"
        }
    }
]
```

input/output:

```jsonl
{ "field":"value", ... }
{ "field":"value", ... }
...
```

## Text

| key      | alias | Description                            | Default Value | Possible Values       |
| -------- | ----- | -------------------------------------- | ------------- | --------------------- |
| type     | -     | Required in order to use this document | `text`        | `text`                |
| metadata | meta  | Metadata describe the resource         | `none`        | [Metadata](#metadata) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "text"
        },
        "connector": {
            "type": "mem",
            "data": "Hello world !!!"
        }
    }
]
```

input/output:

```text
Hello world !!!
```

## Toml

| key      | alias | Description                            | Default Value | Possible Values       |
| -------- | ----- | -------------------------------------- | ------------- | --------------------- |
| type     | -     | Required in order to use this document | `toml`        | `toml`                |
| metadata | meta  | Metadata describe the resource         | `none`        | [Metadata](#metadata) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "toml"
        }
    }
]
```

input/output:

```toml
[[line]]
field= value
...
```

## Xml

| key         | alias | Description                                                      | Default Value | Possible Values                                                                |
| ----------- | ----- | ---------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------ |
| type        | -     | Required in order to use this document                           | `xml`         | `xml`                                                                          |
| metadata    | meta  | Metadata describe the resource                                   | `none`        | [Metadata](#metadata)                                                          |
| is_pretty   | -     | Display data in readable format for human                        | `false`       | `false` / `true`                                                               |
| indent_char | -     | Character to use for indentation in pretty mode                  | `space`       | Simple character                                                               |
| indent_size | -     | Number of indentation to use for each line in pretty mode        | `4`           | unsigned number                                                                |
| entry_path  | -     | Use this field if you want target a specific field in the object | `none`        | String in [json pointer format](https://datatracker.ietf.org/doc/html/rfc6901) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "xml",
            "is_pretty": true,
            "indet_char": " ",
            "indent_size": 4,
            "entry_path": "/root/0/item"
        }
    }
]
```

input/output:

```xml
<root>
    <item field1="value1"/>
    ...
</root>
```

## Yaml

| key      | alias | Description                            | Default Value | Possible Values       |
| -------- | ----- | -------------------------------------- | ------------- | --------------------- |
| type     | -     | Required in order to use this document | `yaml`        | `yaml`                |
| metadata | meta  | Metadata describe the resource         | `none`        | [Metadata](#metadata) |

examples:

```json
[
    {
        "type": "read",
        "document": {
            "type": "xml",
            "is_pretty": true,
            "indet_char": " ",
            "indent_size": 4,
            "entry_path": "/root/0/item"
        }
    }
]
```

input/output:

```yaml
---
field: value
...

```

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
| mime_subtype | Mime sub type of the document                 | `none`        | `plain` / `octet-stream` / [etc...](https://developer.mozilla.org/fr/docs/Web/HTTP/Basics_of_HTTP/MIME_types) | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| charset      | Charset of the document                       | `none`        | `utf-8` / `ISO-8859-1` / etc...                                                                               | [Read/Write CSV](https://github.com/jmfiaschi/chewdata/blob/main/examples/read_write-csv.rs) |
| compression  | Compression used to compress the document     | `none`        | String                                                                                                        | `gzip`                                                                                       |
| language     | The language link to the document             | `none`        | String                                                                                                        | See [language](https://developer.mozilla.org/fr/docs/Web/HTTP/Headers/Content-Language)      |
