# Chewdata

[![CI](https://github.com/jmfiaschi/chewdata/workflows/ci/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![Coverage](https://codecov.io/gh/jmfiaschi/chewdata/branch/main/graph/badge.svg?token=EI62L7XQAH)](https://codecov.io/gh/jmfiaschi/chewdata)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

This application is a lightweight ETL written in Rust, designed to act as a high-performance connector between heterogeneous systems.
It focuses on parallelism, low overhead, and modular feature selection, making it suitable for both local workloads and cloud-native deployments.

| Feature                                  | Values                                                                                                  | Description                                                    |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Generate data                            | -                                                                                                       | Generate synthetic data for testing and development            |
| Supported data formats                   | `json` [E] , `jsonl` [E] , `csv` [D] , `toml` [D] , `xml` [D] , `yaml` [E] , `text` [E] , `parquet` [D] | Read and write multiple structured and semi-structured formats |
| Multiple Connectors                      | `mongodb` [D] , `bucket` [D], `curl` [D] , `psql` [D], `local` [E], `cli` [E], `inmemory` [E]           | Read, write, and clean data across different backends          |
| Multiple Http auths                      | `basic` [D] , `bearer` [D], `jwt` [D]                                                                   | Authentication strategies for the `curl` connector             |
| Data transformation                      | [tera](https://keats.github.io/tera/docs/)    [E]                                                       | Transform data on the fly using templates                      |
| Configuration formats allowed            | `json` [E], `yaml` [E], [hjson](https://hjson.github.io/) [E]                                           | Job definitions provided via versionable config files          |
| Parallel / sequential reading            | `cursor`[E] , `offset`     [E]                                                                          | Flexible pagination strategies for data ingestion              |
| Application Performance Monitoring (APM) | `apm`[D]                                                                                                | Export traces and metrics                                      |

> [E] - `E`nabled by default - disable with `--no-default-features`
> [D] - `D`isabledby default - enable explicitly via `--features`

More useful information:

* Requires only `rustup` — no external runtime dependencies
* No garbage collector
* Parallel works
* Fully cross-platform
* Use async/await for concurrent execution with zero-cost
* Highly parallel execution model
* Read multiple files concurrently from:
*  - Local filesystem
*  - S3 compatible bucket
* Query CSV / JSON / Parquet files using S3 Select
* Deployable as an AWS Lambda
* Configuration is easy to version and review
* Generate test data on the fly
* Built-in data validation
*  - Separate streams for valid and invalid records
* Compile only what you need: 
*  - cargo build --no-default-features --features "toml,psql"

## Getting started

### Setup from source code

Requirement:

* [Rust](https://www.rust-lang.org/tools/install)
* [Docker](https://docs.docker.com/get-docker/) and [Docker-compose](https://docs.docker.com/compose/install/) for testing the code in local

Commands to execute:

```Bash
git clone https://github.com/jmfiaschi/chewdata.git chewdata
cd chewdata
cp .env.dev .env
vim .env // Edit the .env file
just build
just test
```

If all the test pass, the project is ready. read the Makefile in order to see, what kind of shortcut you can use.

If you want some examples to discover this project, go in this section [./examples](./examples/)

### Setup from cargo package

#### Default installation

This command will install the project with all features.

```bash
cargo install chewdata
```

#### With minimal features

If you need just read/write json file, transform and store them into the local environment.

```bash
cargo install chewdata --no-default-features
```

#### With custom features

If you want to specify some features to add to your installation

```bash
cargo install chewdata --no-default-features --features "xml,bucket"
```

Please, referer to the [features documentation](/docs/componants/features)</a>.

#### How to change the log level

If you need to change the log level of the command, you need to define it during the installation.

```bash
cargo install chewdata --no-default-features --features "log-release-trace"
echo '{"field1":"value1"}' | RUST_LOG=trace chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"cli"}},{"type":"writer","document":{"type":"json"},"connector":{"type":"cli"}}]'
```

If you want to filter logs, you can use the directive syntax from [tracing_subscriber](https://tracing.rs/tracing_subscriber/filter/struct.envfilter).

```bash
echo '{"field1":"value1"}' | RUST_LOG=chewdata=trace chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"cli"}},{"type":"writer","document":{"type":"json"},"connector":{"type":"cli"}}]'
```

### Run

First of all, you can check how the command works with the option `--help`

```bash
chewdata --help
...
USAGE:
    chewdata [OPTIONS] [JSON]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --file <FILE>    Init steps with file configuration in input

ARGS:
    <JSON>    Init steps with a json/hjson configuration in input
```

#### Without configuration

It is possible to run the command without configuration, the application will wait until you write `json` data. By default, the program write json data in the output and the program stop when you enter empty value.

```Bash
$ cargo run
$ [{"key":"value"},{"name":"test"}]
$ --enter--
[{"key":"value"},{"name":"test"}]
```

Another examples without configuration and with file in input

```Bash
$ cat ./data/multi_lines.json | cargo run
[{...}]
```

or

```Bash
$ cat ./data/multi_lines.json | just run
[{...}]
```

or

```Bash
$ cat ./data/multi_lines.json | chewdata
[{...}]
```

#### With configuration

The configuration is usefull to customize a list of steps. It support [`hjson`](https://hjson.github.io/) format in order to enrich it.

```Bash
$ cat ./data/multi_lines.csv | cargo run --features "csv" '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

or

```Bash
$ cat ./data/multi_lines.csv | just run-with-json '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

or

```Bash
$ cat ./data/multi_lines.csv | chewdata '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

Another example, With file configuration in argument

```Bash
$ echo '[{"type":"reader","connector":{"type":"cli"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | cargo run --features "csv" -- --file my_etl.conf.json
[{...}]
```

or

```Bash
$ echo '[{"type":"reader","connector":{"type":"cli"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | just run-with-file my_etl.conf.json
[{...}]
```

or

```Bash
$ echo '[{"type":"reader","connector":{"type":"cli"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | chewdata --file my_etl.conf.json
[{...}]
```

PS: It's possible to replace Json configuration file by Yaml format.

### Chain commands

It is possible to chain chewdata program :

```bash
task_A=$(echo '{"variable": "a"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"10"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
task_B=$(echo '{"variable": "b"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"20"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
echo $task_A | CHEWDATA_VAR_B=$task_B chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"var_b","pattern":"{{ get_env(name=\"VAR_B\") }}"},{"field":"result","pattern":"{{ output.var_b.value * input.value }}"},{"field":"var_b","type":"remove"}]},{"type":"w"}]'
[{"result":200}]
```

### Apply custom environmnet variables

If you want to inject an environment variable, please prefix it with `CHEWDATA`. 
It's mandatory in order to avoid collision and for security.

```bash
CHEWDATA_VAR_B=$task_B CHEWDATA_CURL_ENDPOINT=my_endpoint RUST_LOG=info chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"var_b","pattern":"{{ get_env(name=\"VAR_B\") }}"},{"field":"result","pattern":"{{ output.var_b.value * input.value }}"},{"field":"var_b","type":"remove"}]},{"type":"w", "connector": {"type": "curl","endpoint": "{{ CURL_ENDPOINT }}","path": "/post","method": "post"}}]'
```

## How it works ?

This program execute `steps` from a configuration file that you need to inject in `Json` or `Yaml` format :

Example:

```json
[
 {
    "type": "erase",
    "connector": {
        "type": "local",
        "path": "./my_file.out.csv"
    }
 },
 {
    "type": "reader",
    "connector": {
        "type": "local",
        "path": "./my_file.csv"
    }
 },
 {
    "type": "writer",
    "connector": {
        "type": "local",
        "path": "./my_file.out.csv"
    }
 },
 ...
]
```

These `steps` are executed in the `FIFO` order.

All `steps` are linked together by an `input` and `output` context queue. 
When a step finishes handling data, a new context is created and send into the output queue. The next step will handle this new context.
Step1(Context) -> Q1[Contexts] -> StepN(Context) -> QN[Contexts] ->  StepN+1(Context)
Each step runs asynchronously. Each queue contains a limit that can be customized in the step's configuration.

Check the module [`step`] to see the list of steps you can use and their configuration. Check the folder [/examples](./examples) to have some examples how to use and build a configuration file.  

### List of steps with the configurations

* [reader](https://docs.rs/chewdata/latest/chewdata/step/reader/index.html)
* [writer](https://docs.rs/chewdata/latest/chewdata/step/writer/index.html)
* [eraser](https://docs.rs/chewdata/latest/chewdata/step/eraser/index.html)
* [generator](https://docs.rs/chewdata/latest/chewdata/step/generator/index.html)
* [transformer](https://docs.rs/chewdata/latest/chewdata/step/transformer/index.html)
* [validator](https://docs.rs/chewdata/latest/chewdata/step/validator/index.html)

## How to contribute ?

Follow the [GitHub flow](https://guides.github.com/introduction/flow/).

Folow the [Semantic release Specification](https://semantic-release.gitbook.io/semantic-release/)

After code modifications, please run all tests.

```Bash
just test
```

## Useful links

* [Benchmark report](https://jmfiaschi.github.io/chewdata/benches/main/)
* [Documentation](https://docs.rs/chewdata/latest/chewdata/)
* [Package](https://crates.io/crates/chewdata)
