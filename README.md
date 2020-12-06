# data-pipeline
[![Linter](https://github.com/jmfiaschi/chewdata/workflows/Lint/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![Tests](https://github.com/jmfiaschi/chewdata/workflows/CI/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![codecov](https://codecov.io/gh/jmfiaschi/chewdata/branch/main/graph/badge.svg?token=EI62L7XQAH)](https://codecov.io/gh/jmfiaschi/chewdata)


ETL (Extract-Transform-Load) in rust. 

How it works in general ?
```Mermaid
sequenceDiagram
    participant ConnectorSource
    participant DocumentSource
    participant Reader
    participant Transformer
    participant Writer
    loop Read
        Reader->>DocumentSource: Fetch data through a document type
        DocumentSource->>ConnectorSource: Fetch data through a connector type
        ConnectorSource->>DocumentSource: Return a buffer of formatted data
        DocumentSource->>Reader: Return a denormalized data result 
    end
    Reader-->>Transformer: Send a dataset of denormalized data result
    loop Transform
        Transformer->>Transformer: Read all the dataset and transform each data
    end
    Transformer-->Writer: Send the transformed dataset
    loop Write
        Writer->>DocumentTarget: Read each data result in the dataset and send the data
        DocumentTarget->>ConnectorTarget: Write normalize data into the connector
    end
    Writer->>DocumentTarget: flush & send data
    DocumentTarget->>ConnectorTarget: flush & send data
```
(if you don't see this schema with your browser, try with the chrome [pluging](https://chrome.google.com/webstore/detail/mermaid-diagrams/phfcghedmopjadpojhmmaffjmfiakfil))

## Getting started
### Requirement
* [Rust](https://www.rust-lang.org/tools/install)

### Installation
```Bash
$ git clone https://github.com/jmfiaschi/chewdata.git chewdata
$ cd chewdata
```
### Configuration

Create the .env file used by the application and customize your configuration
```Bash
$ cp .env.dev .env && vim .env
```

Build the project
```Bash
$ make build
```

Init stack in local
```Bash
// if you want to test your etl with buckets
$ make minio
$ make minio-install
// if you want to test your etl with APIs
$ make httpbin
```

### Run with ETL configuration

Without ETL configuration, it will use the default configuration that display the data in input.
```Bash
$ cat ./data/multi_lines.json | make run 
=> [{...}]
```
With json etl configuration in argument
```Bash
$ cat ./data/multi_lines.csv | make run json='[{"type":"r","document":{"type":"csv","meta":{"delimiter":","}}},{"type":"w"}]'
=> [{...}]
```
With etl file configuration in argument
```Bash
$ echo '[{"type":"r","document":{"type":"csv","meta":{"delimiter":","}}},{"type":"w"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | make run-file file='my_etl.conf.json'
=> [{...}]
```

### Run tests
After code modifications, please run all tests.
```Bash
$ make test
```

### Run examples
```Bash
// list all examples
$ make example
$ make example name=read_write-json
=> [{...}]
```

## Documentations

in progress...

## How to contribute
In progress...

## Usefull links
* Documentation: [Chewdata](http://www.chewdata.org)
* Doc API : [crates.io](https://crates.io/crates/chewdata)
