+++
title = "Connectors"
description = "Connectors that you can use"
date = 2021-05-01T18:20:00+00:00
updated = 2021-05-01T18:20:00+00:00
draft = false
weight = 300
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "Connectors that you can use"
toc = true
top = false
+++

## IO

Read and write data through standard input and output.

| key      | alias | Description                             | Default Value | Possible Values       |
| -------- | ----- | --------------------------------------- | ------------- | --------------------- |
| type     | -     | Required in order to use this connector | `io`          | `io`                  |
| metadata | meta  | Override metadata information           | `none`        | [Metadata](#metadata) |

examples:

```json
[
    {
        "type": "reader",
        "connector":{
            "type": "io"
        }
    }
]
```

## In Memory

Read and write data through memory. You can use this connector if you want inject constant in your flow.

| key      | alias              | Description                             | Default Value | Possible Values       |
| -------- | ------------------ | --------------------------------------- | ------------- | --------------------- |
| type     | -                  | Required in order to use this connector | `in_memory`   | `in_memory` / `mem`   |
| metadata | meta               | Override metadata information           | `none`        | [Metadata](#metadata) |
| memory   | value / doc / data | Memory value                            | `none`        | String                |

examples:

```json
[
    {
        "type": "reader",
        "connector":{
            "type": "in_memory",
            "memory": {
                "username": "{{ MY_USERNAME }}",
                "password": "{{ MY_PASSWORD }}"
            }
        }
    }
]
```

## Local

Read and write data in local file. It is possible to read multifiles with wildcard. If you want to write dynamicaly in different files, use the [mustache](http://mustache.github.io/) variable that will be replace with the data in input.

| key        | alias | Description                                                                                      | Default Value | Possible Values       |
| ---------- | ----- | ------------------------------------------------------------------------------------------------ | ------------- | --------------------- |
| type       | -     | Required in order to use this connector                                                          | `local`       | `local`               |
| metadata   | meta  | Override metadata information                                                                    | `none`        | [Metadata](#metadata) |
| path       | -     | Path of a file or list of files. Allow wildcard charater `*` and mustache variables              | `none`        | String                |
| parameters | -     | Variable that can be use in the path. Parameters of the connector is merge with the current data | `none`        | List of key and value |

examples:

```json
[
    {
        "type": "reader",
        "connector":{
            "type": "local",
            "path": "./{{ folder }}/*.json",
            "metadata": {
                "content-type": "application/json; charset=utf-8"
            },
            "parameters": {
                "folder": "my_folder"
            }
        }
    }
]
```

## Curl

Read and write data through http(s) connector.

| key           | alias | Description                                              | Default Value | Possible Values                                                        |
| ------------- | ----- | -------------------------------------------------------- | ------------- | ---------------------------------------------------------------------- |
| type          | -     | Required in order to use this connector                  | `curl`        | `curl`                                                                 |
| metadata      | meta  | Override metadata information                            | `none`        | [Metadata](#metadata)                                                  |
| authenticator | auth  | Define the authentification that secure the http(s) call | `none`        | [Authenticator](#authenticator)                                        |
| endpoint      | -     | The http endpoint of the url                             | `none`        | String                                                                 |
| path          | uri   | The path of the resource                                 | `none`        | String                                                                 |
| method        | -     | The http method to use                                   | `get`         | [HTTP methods](https://developer.mozilla.org/fr/docs/Web/HTTP/Methods) |
| headers       | -     | The http headers to override                             | `none`        | List of key/value                                                      |
| parameters    | -     | Parameters used in the path that can be override         | `none`        | Object or Array of objects                                             |
| limit         | -     | Limit value used by the pagination                       | `100`         | unsigned number                                                        |
| skip          | -     | Skip value used by the pagination                        | `0`           | unsigned number                                                        |
| paginator     | -     | Paginator parameters                                     | `none`        | [Paginator](#paginator)                                                |

examples:

```json
[
    {
        "type": "read",
        "connector":{
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/post",
            "method": "post",
            "authenticator": {
                "type": "basic",
                "username": "{{ BASIC_USERNAME }}",
                "password": "{{ BASIC_PASSWORD }}",
            },
            "headers": {
                "Accept": "application/json"
            },
            "parameters": [
                { "field": "value" }
            ],
            "limit": 100,
            "skip": 0,
            "paginator": {
                "limit": "limit_name",
                "skip": "skip_name"
            }
        },
    }
]
```

### Paginator

Use to override the default configuration

| key        | alias | Description                                     | Default Value | Possible Values |
| ---------- | ----- | ----------------------------------------------- | ------------- | --------------- |
| limit_name | limit | Name of the field limit used in query parameter | `limit`       | String          |
| skip_name  | skip  | Name of the field skip used un query parameter  | `skip`        | String          |

examples:

```json
[
    {
        "type": "write",
        "connector":{
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/resource",
            "method": "get",
            "paginator": {
                "limit": "my_limit_field",
                "skip": "my_skip_field"
            }
        },
    }
]
```

### Authenticator

#### Basic

| key      | alias      | Description                                  | Default Value | Possible Values |
| -------- | ---------- | -------------------------------------------- | ------------- | --------------- |
| type     | -          | Required in order to use this authentication | `basic`       | `basic`         |
| username | user / usr | Username to use for the authentification     | `none`        | String          |
| password | pass / pwd | Password to use for the authentification     | `none`        | String          |

examples:

```json
[
    {
        "type": "read",
        "connector":{
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/get",
            "method": "get",
            "authenticator": {
                "type": "basic",
                "username": "{{ BASIC_USERNAME }}",
                "password": "{{ BASIC_PASSWORD }}",
            }
        },
    }
]
```

#### Bearer

| key        | alias | Description                                                             | Default Value | Possible Values          |
| ---------- | ----- | ----------------------------------------------------------------------- | ------------- | ------------------------ |
| type       | -     | Required in order to use this authentication                            | `bearer`      | `bearer`                 |
| token      | -     | The bearer tocken                                                       | `none`        | String                   |
| is_base64  | -     | Specify if the bearer token is encoded in base64                        | `false`       | `false` / `true`         |
| parameters | -     | Use to replace the token with dynamic value in input from the connector | `none`        | List of Key/Value string |

examples:

```json
[
    {
        "type": "write",
        "connector":{
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/post",
            "method": "post",
            "authenticator": {
                "type": "bearer",
                "token": "{{ token }}",
                "is_base64": false,
                "parameters": {
                    "token": "my_token"
                }
            }
        },
    }
]
```

#### (JWT) Java Web Token

| key               | alias | Description                                                          | Default Value | Possible Values                                                                            |
| ----------------- | ----- | -------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------ |
| type              | -     | Required in order to use this authentication                         | `jwt`         | `jwt`                                                                                      |
| algorithm         | algo  | The algorithm used to build the signing                              | `HS256`       | String                                                                                     |
| refresh_connector | -     | The connector used to refresh the token                              | `none`        | See [Connectors](#connectors)                                                              |
| refresh_token     | -     | The token name used to identify the token into the refresh connector | `token`       | String                                                                                     |
| jwk               | -     | The Json web key used to sign                                        | `none`        | [Object](https://datatracker.ietf.org/doc/html/rfc7517#page-5)                             |
| format            | -     | Define the type of the key used for the signing                      | `secret`      | `secret` / `base64secret` / `rsa_pem` / `rsa_components` / `ec_pem` / `rsa_der` / `ec_der` |
| key               | -     | Key used for the signing                                             | `none`        | String                                                                                     |
| payload           | -     | The jwt payload                                                      | `none`        | Object or Array of objects                                                                 |
| parameters        | -     | The parameters used to remplace variables in the payload             | `none`        | Object or Array of objects                                                                 |
| token             | -     | The token that can be override if necessary                          | `none`        | String                                                                                     |

examples:

```json
[
    {
        "type": "write",
        "connector":{
            "type": "curl",
            "endpoint": "{{ CURL_ENDPOINT }}",
            "path": "/post",
            "method": "post",
            "authenticator": {
                "type": "jwt",
                "refresh_connector": {
                    "type": "curl",
                    "endpoint": "http://my_api.com",
                    "path": "/tokens",
                    "method": "post"
                },
                "refresh_token":"token",
                "key": "my_key",
                "payload": {
                    "alg":"HS256",
                    "claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077}
                },
                "parameters": {
                    "username": "my_username",
                    "password": "my_username"
                }
            }
        },
    }
]
```

## Mongodb

Read and write data into mongodb database.

| key            | alias      | Description                                                                                                                                                                   | Default Value | Possible Values                                                                      |
| -------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------ |
| type           | -          | Required in order to use this connector                                                                                                                                       | `mongodb`     | `mongodb` / `mongo`                                                                  |
| endpoint       | -          | Endpoint of the connector                                                                                                                                                     | `none`        | String                                                                               |
| database       | db         | The database name                                                                                                                                                             | `none`        | String                                                                               |
| collection     | col        | The collection name                                                                                                                                                           | `none`        | String                                                                               |
| query          | -          | Query to find an element into the collection                                                                                                                                  | `none`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.find/)       |
| find_options   | projection | Specifies the fields to return in the documents that match the query filter. To return all fields in the matching documents, omit this parameter. For details, see Projection | `none`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.find/)       |
| update_options | -          | Options apply during the update)                                                                                                                                              | `none`        | [Object](https://docs.mongodb.com/manual/reference/method/db.collection.updateMany/) |

examples:

```json
[
    {
        "type": "w",
        "connector":{
            "type": "mongodb",
            "endpoint": "mongodb://admin:admin@localhost:27017",
            "db": "tests",
            "collection": "test",
            "update_options": {
                "upsert": true
            }
        },
        "thread_number":3
    }
]
```

## Bucket

Read and write data into S3/Minio bucket.

| key               | alias  | Description                                                            | Default Value                    | Possible Values            |
| ----------------- | ------ | ---------------------------------------------------------------------- | -------------------------------- | -------------------------- |
| type              | -      | Required in order to use this connector                                | `bucket`                         | `bucket`                   |
| metadata          | meta   | Override metadata information                                          | `none`                           | [Metadata](#metadata)      |
| endpoint          | -      | Endpoint of the connector                                              | `none`                           | String                     |
| access_key_id     | -      | The access key used for the authentification                           | `none`                           | String                     |
| secret_access_key | -      | The secret access key used for the authentification                    | `none`                           | String                     |
| region            | -      | The bucket's region                                                    | `us-east-1`                      | String                     |
| bucket            | -      | The bucket name                                                        | `none`                           | String                     |
| path              | key    | The path of the resource. Can use `*` in order to read multiple files  | `none`                           | String                     |
| parameters        | params | The parameters used to remplace variables in the path                  | `none`                           | Object or Array of objects |
| limit             | -      | Limit the number of files to read.                                     | `none`                           | Unsigned number            |
| skip              | -      | Skip N files before to start to read the next files                    | `none`                           | Unsigned number            |
| version           | -      | Read a specific version of a file                                      | `none`                           | String                     |
| tags              | -      | List of tags to apply on the file. Used to give more context to a file | `(service:writer:name,chewdata)` | List of Key/Value          |
| cache_control     | -      | Override the file cache controle                                       | `none`                           | String                     |
| expires           | -      | Override the file expire date                                          | `none`                           | String                     |

examples:

```json
[
    {
        "type": "r",
        "connector": {
            "type": "bucket",
            "bucket": "my-bucket",
            "path": "data/*.json*",
            "endpoint":"{{ BUCKET_ENDPOINT }}",
            "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
            "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
            "region": "{{ BUCKET_REGION }}",
            "limit": 10,
            "skip": 0,
            "tags": {
                "service:writer": "my_service",
                "service:writer:owner": "my_team_name",
                "service:writer:env": "dev",
                "service:writer:context": "example"
            }
        }
    },
]
```

## Bucket Select

Filter data file with S3 select queries and read data into AWS/Minio bucket. Use Bucket connector in order to write into the bucket.

| key               | alias  | Description                                           | Default Value            | Possible Values                                                                                                        |
| ----------------- | ------ | ----------------------------------------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| type              | -      | Required in order to use this connector               | `bucket`                 | `bucket`                                                                                                               |
| metadata          | meta   | Override metadata information                         | `none`                   | [Metadata](#metadata)                                                                                                  |
| endpoint          | -      | Endpoint of the connector                             | `none`                   | String                                                                                                                 |
| access_key_id     | -      | The access key used for the authentification          | `none`                   | String                                                                                                                 |
| secret_access_key | -      | The secret access key used for the authentification   | `none`                   | String                                                                                                                 |
| region            | -      | The bucket's region                                   | `us-east-1`              | String                                                                                                                 |
| bucket            | -      | The bucket name                                       | `none`                   | String                                                                                                                 |
| path              | key    | The path of the resource                              | `none`                   | String                                                                                                                 |
| parameters        | params | The parameters used to remplace variables in the path | `none`                   | Object or Array of objects                                                                                             |
| query             | -      | S3 select query                                       | `select * from s3object` | See [AWS S3 select](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-select.html) |

examples:

```json
[
    {
        "type": "r",
        "connector": {
            "type": "bucket_select",
            "bucket": "my-bucket",
            "path": "data/my_file.jsonl",
            "endpoint": "{{ BUCKET_ENDPOINT }}",
            "access_key_id": "{{ BUCKET_ACCESS_KEY_ID }}",
            "secret_access_key": "{{ BUCKET_SECRET_ACCESS_KEY }}",
            "region": "{{ BUCKET_REGION }}",
            "query": "select * from s3object[*].results[*] r where r.number = 20"
        },
        "document" : {
            "type": "jsonl"
        }
    }
]
```

## Metadata

By default, the metadata is manage with the document [Metadata](/docs/componants/documents) but you can override it if necessary.
