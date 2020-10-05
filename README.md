# data-pipeline
[![Actions Status](https://github.com/jmfiaschi/data-pipeline/workflows/CI/badge.svg)](https://github.com/jmfiaschi/data-pipeline/actions)

ETL (Extract-Transform-Load) in rust. 

How it works ?
```Mermaid
sequenceDiagram
    participant SourceDocument
    participant Reader
    participant Transformer
    participant Writer
    loop Healthcheck
        Reader->>SourceDocument: Read document by document
    end
    Note left of Reader: It can read these formats:<br/>json,jsonl,yaml,csv,xml
    Reader-->>Transformer: Send the document to transform
    loop Healthcheck
        Transformer->>Transformer: Read each fields into the INPUT data <br/>to transform and return a new OUTPUT data
    end
    Note left of Transformer: It use Tera or Handlebars engines<br/> in order to transform the INPUT values.<br/>It can use a referential in order to map some values
    Transformer-->Writer: Send the new document to the writer
    Writer->>TargetDocument: Push the new document
    Note right of Writer: It can write into these formats:<br/>json,jsonl,yaml,csv,xml
```

## Getting started
### Configure your DataPipeline

### Run in local
```Bash
$ make install
// Edit the .env
$ vim .env
// Run with Docker
$ make run config=./examples/local.config.json format=json
// Or rust natively
$ cargo run "$(cat ./examples/local.config.json)" json
```
