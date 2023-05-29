# Chewdata

[![CI](https://github.com/jmfiaschi/chewdata/workflows/CI/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![Coverage](https://codecov.io/gh/jmfiaschi/chewdata/branch/main/graph/badge.svg?token=EI62L7XQAH)](https://codecov.io/gh/jmfiaschi/chewdata)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

This application is a light ETL in rust that can be used as a connector between systems

| Feature                                  | Values                                                                                                  | Description                                                        |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Generate data                            | -                                                                                                       | Generate data for testing                                          |
| Supported formats                        | `json` [E] , `jsonl` [E] , `csv` [D] , `toml` [D] , `xml` [D] , `yaml` [E] , `text` [E] , `parquet` [D] | Read and Write in these formats                                    |
| Multi Connectors                         | `mongodb` [D] , `bucket` [D], `curl` [D] , `psql` [D], `local` [E], `io` [E], `inmemory` [E]            | Read / Write / Clean data                                          |
| Multi Http auths                         | `basic` [D] , `bearer` [D], `jwt` [D]                                                                   | Give different possibilities to authenticate the `curl`            |
| Transform data                           | [tera](https://tera.netlify.app/docs)    [E]                                                            | Transform the data in the fly                                      |
| Configuration formats allowed            | `json` [E], `yaml`     [E]                                                                              | The project need a jobs configuration in input                     |
| Read data in parallel or sequential mode | `cursor`[E] , `offset`     [E]                                                                          | With this type of paginator, the data can be read in different way |
| Application Performance Monitoring (APM) | `apm`[D]                                                                                                | Send APM logs into Jaeger                                          |

> [E] - Feature `E`nabled by default. Use `--no-default-features` argument to remove all enabled features by default.
>
> [D] - Feature `D`isabled and must be enabled with the `--features` argument.

More useful information:

* It need only rustup
* No garbage collector
* Parallel works
* Cross-platform
* Use async/await for concurrent threads with zero-cost
* Read multi files in parallel into the local or in a bucket
* Search data into multi csv/json/parquet files with S3 select
* Can be deployed into AWS Lambda
* The configuration easly versionable
* Can generate data in the fly for testing purpose
* Control and validate the data. Handle bad and valid data in a dedicated stream
* Enable only required feature: --no-default-features --features "toml psql"

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
make build
make unit-tests
make integration-tests
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
cargo install chewdata --no-default-features --features "xml bucket"
```

Please, referer to the [features documentation](/docs/componants/features)</a>.

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
    <JSON>    Init steps with a json configuration in input
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
$ cat ./data/multi_lines.json | make run
[{...}]
```

or

```Bash
$ cat ./data/multi_lines.json | chewdata
[{...}]
```

#### With configuration

The configuration is usefull to customize a list of steps.

```Bash
$ cat ./data/multi_lines.csv | cargo run '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

or

```Bash
$ cat ./data/multi_lines.csv | make run json='[{\"type\":\"reader\",\"document\":{\"type\":\"csv\"}},{\"type\":\"writer\"}]'
[{...}] // Will transform the csv data into json format
```

or

```Bash
$ cat ./data/multi_lines.csv | chewdata '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

Another example, With file configuration in argument

```Bash
$ echo '[{"type":"reader","connector":{"type":"io"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | cargo run -- --file my_etl.conf.json
[{...}]
```

or

```Bash
$ echo '[{"type":"reader","connector":{"type":"io"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | make run file=my_etl.conf.json
[{...}]
```

or

```Bash
$ echo '[{"type":"reader","connector":{"type":"io"},"document":{"type":"csv"}},{"type":"writer"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | chewdata --file my_etl.conf.json
[{...}]
```

PS: It's possible to replace Json configuration file by Yaml format.

### Chain commands

It is possible to chain chewdata program :

```bash
task_A=$(echo '{"variable": "a"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"10"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
task_B=$(echo '{"variable": "b"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"20"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
echo $task_A | VAR_B=$task_B chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"var_b","pattern":"{{ get_env(name=\"VAR_B\") }}"},{"field":"result","pattern":"{{ output.var_b.value * input.value }}"},{"field":"var_b","type":"remove"}]},{"type":"w"}]'
[{"result":200}]
```

## How to contribute

Follow the [GitHub flow](https://guides.github.com/introduction/flow/).

Folow the [Semantic release Specification](https://semantic-release.gitbook.io/semantic-release/)

After code modifications, please run all tests.

```Bash
make test
```

## Useful links

* [Benchmark report](https://jmfiaschi.github.io/chewdata/benches/main/)
* [Documentation](https://docs.rs/chewdata/latest/chewdata/)
* [Package](https://crates.io/crates/chewdata)
