+++
title = "Steps"
description = "Steps that you can use"
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 200
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "Steps that you can use"
toc = true
top = false
+++

## Reader

This step read dataset through a queue. For each data in input, it fetch the resource, read resource's data and put each data into the queue for the next step.

| key                 | alias | Description                                                             | Default Value | Possible Values                              |
| ------------------- | ----- | ----------------------------------------------------------------------- | ------------- | -------------------------------------------- |
| type                | -     | Required in order to use reader step                                    | `reader`      | `reader` / `read` / `r`                      |
| connector           | conn  | Connector type to use in order to read a resource                       | `io`          | See [connectors](/docs/componants/connector) |
| document            | doc   | Document type to use in order to manipulate the resource                | `json`        | See [documents](/docs/componants/document)   |
| alias               | -     | Alias the step use during the debug mode                                | `none`        | Auto generate alphanumeric value             |
| description         | desc  | Describ your step and give more visibility                              | `none`        | String                                       |
| data_type           | data  | Type of data the reader push in the queue : [ ok / err ]                | `ok`          | `ok` / `err`                                 |
| wait_in_millisecond | wait  | Time to wait in millisecond until to retry to put the data in the queue | `10`          | unsigned number                              |

examples:

```json
[
    {
        "type": "reader",
        "alias": "read_a",
        "description": "My description of the step",
        "connector": {
            "type": "io"
        },
        "document": {
            "type": "json"
        },
        "data_type": "ok",
        "wait_in_millisecond": 10
    }
]
```

## Writer

This step read dataset through a queue and write data in a resource though a connector and document type.

| key                 | alias   | Description                                                                 | Default Value | Possible Values                              |
| ------------------- | ------- | --------------------------------------------------------------------------- | ------------- | -------------------------------------------- |
| type                | -       | Required in order to use writer step                                        | `writer`      | `writer` / `write` / `w`                     |
| connector           | conn    | Connector type to use in order to read a resource                           | `io`          | See [connectors](/docs/componants/connector) |
| document            | doc     | Document type to use in order to manipulate the resource                    | `json`        | See [documents](/docs/componants/document)   |
| alias               | -       | Alias the step use during the debug mode                                    | `none`        | Auto generate alphanumeric value             |
| description         | desc    | Describ your step and give more visibility                                  | `none`        | String                                       |
| data_type           | data    | Type of data read for writing. skip other data type                         | `ok`          | `ok` / `err`                                 |
| wait_in_millisecond | -       | Time to wait in millisecond until to retry to put the data in the queue     | `10`          | unsigned number                              |
| thread_number       | threads | Parallelize the step in multiple threads                                    | `1`           | unsigned number                              |
| dataset_size        | batch   | Stack size limit before to push data into the resource though the connector | `1000`        | unsigned number                              |

examples:

```json
[
    {
        "type": "writer",
        "alias": "write_a",
        "description": "My description of the step",
        "connector": {
            "type": "io"
        },
        "document": {
            "type": "json"
        },
        "data_type": "ok",
        "wait_in_millisecond": 10,
        "thread_number": 1,
        "dataset_size": 1000
    }
]
```

## Transformer

This step read dataset through a queue, transform the data with actions and write the output object into the queue.

| key                 | alias   | Description                                                                                                                 | Default Value | Possible Values                                 |
| ------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------- |
| type                | -       | Required in order to use transformer step                                                                                   | `transformer` | `transformer` / `transform` / `t`               |
| updater             | u       | Updater type used as a template engine for treansformation                                                                  | `tera`        | `tera`                                          |
| referentials        | refs    | List of `reader` that can be use to map object during the transformation. Use the referential alias in the action's pattern | `none`        | `{"alias_a": READER,"alias_b": READER, etc...}` |
| alias               | -       | Alias the step use during the debug mode                                                                                    | `none`        | Auto generate alphanumeric value                |
| description         | desc    | Describ your step and give more visibility                                                                                  | `none`        | String                                          |
| data_type           | data    | Type of data used for the transformation. skip other data type                                                              | `ok`          | `ok` / `err`                                    |
| wait_in_millisecond | -       | Time to wait in millisecond until to retry to put the data in the queue                                                     | `10`          | unsigned number                                 |
| thread_number       | threads | Parallelize the step in multiple threads                                                                                    | `1`           | unsigned number                                 |
| actions             | -       | List of actions composed                                                                                                    | `none`        | See [Action](#action)                           |
| input_name          | input   | Input name variable can be used in the pattern action                                                                       | `input`       | String                                          |
| output_name         | output  | Output name variable can be used in the pattern action                                                                      | `output`      | String                                          |

### Action

| key     | Description                                                                                                                                                           | Default Value | Possible Values                                                               |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------------------------------------- |
| field   | Json pointer that define the field path created into the output object                                                                                                | `/`           | alphanumeric or [json pointer](https://datatracker.ietf.org/doc/html/rfc6901) |
| pattern | Pattern in [django template language](https://docs.djangoproject.com/en/3.1/topics/templates/) format used to build the output field. This project use Tera's methods | `none`        |
| type    | Type of action                                                                                                                                                        | `merge`       | `merge` / `replace` / `remove`                                                |

examples:

```json
[
    {
        "type": "transformer",
        "updater": {
            "type": "tera"
        },
        "referentials": {
            "ref_a": {
                "connector": {
                    "type": "io"
                }
            }
        },
        "alias": "transform_a",
        "description": "My description of the step",
        "connector": {
            "type": "io"
        },
        "document": {
            "type": "json"
        },
        "data_type": "ok",
        "wait_in_millisecond": 10,
        "thread_number": 1,
        "actions": [{
            "field": "field_A",
            "pattern": "{{ my_input.number * my_output.number * ref_a.number }}",
            "type": "merge"
        }],
        "input_name": "my_input",
        "output_name": "my_output"
    }
]
```

## Eraser

| key                 | alias   | Description                                                             | Default Value | Possible Values                              |
| ------------------- | ------- | ----------------------------------------------------------------------- | ------------- | -------------------------------------------- |
| type                | -       | Required in order to use eraser step                                    | `eraser`      | `eraser` / `eraser` / `truncate` / `e`       |
| connector           | conn    | Connector type to use in order to read a resource                       | `io`          | See [connectors](/docs/componants/connector) |
| alias               | -       | Alias the step use during the debug mode                                | `none`        | Auto generate alphanumeric value             |
| description         | desc    | Describe your step and give more visibility                             | `none`        | String                                       |
| wait_in_millisecond | wait    | Time to wait in millisecond until to retry to put the data in the queue | `10`          | unsigned number                              |
| exclude_paths       | exclude | resource to exclude for the erase step                                  | `none`        | List of string                               |

```json
[
    {
        "type": "erase",
        "alias": "erase_a",
        "description": "My description of the step",
        "connector": {
            "type": "local",
            "path": "./*.json"
        },
        "wait_in_millisecond": 10,
        "exclude_paths": [
            "file1.json"
        ]
    }
]
```
