# Chewdata

[![Linter](https://github.com/jmfiaschi/chewdata/workflows/Lint/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![CI](https://github.com/jmfiaschi/chewdata/workflows/CI/badge.svg)](https://github.com/jmfiaschi/chewdata/actions)
[![Coverage](https://codecov.io/gh/jmfiaschi/chewdata/branch/main/graph/badge.svg?token=EI62L7XQAH)](https://codecov.io/gh/jmfiaschi/chewdata)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

This application is an simple ETL in rust that can be used as a connector between systems

* It handle multiple formats : Json, Jsonl, CSV, Toml, XML, Yaml, Text
* It can read/write data from :
  * Mongodb database
  * S3/Minio with versionning & select
  * Http(s) APIs with some authenicators: Basic, Bearer, Jwt
  * Local
  * Relational DB like PSQL (`Not Yet`)
  * Message broker (`Not Yet`)
* It need only rustup
* No garbage collector
* Parallel work
* Multi platforms
* Use [tera template](https://tera.netlify.app/docs) in order to configure the actions for the data transformation

the target of this project is to simplify the work of developers and simplify the connection between system.
The work is not finished but I hope it will be useful for you.

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

If you want some examples to discover this project, go in this section [./examples](./examples/README.md)

### Run the ETL

If you run the program without parameters, the application will wait until you write json data. By default, the program write json data in the output and the program stop when you press multiple times the 'enter' key.

```Bash
$ cargo run 
$ [{"key":"value"},{"name":"test"}]
$ exit
[{"key":"value"},{"name":"test"}]
```

Another example without etl configuration and with file in input

```Bash
$ cat ./data/multi_lines.json | cargo run 
[{...}]
```

or

```Bash
$ cat ./data/multi_lines.json | make run 
[{...}]
```

Another example, With a json etl configuration in argument

```Bash
$ cat ./data/multi_lines.csv | cargo run '[{"type":"reader","document":{"type":"csv"}},{"type":"writer"}]'
[{...}] // Will transform the csv data into json format
```

or

```Bash
$ cat ./data/multi_lines.csv | make run json='[{\"type\":\"reader\",\"document\":{\"type\":\"csv\"}},{\"type\":\"writer\"}]'
[{...}] // Will transform the csv data into json format
```

Another example, With etl file configuration in argument

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

It is possible to use alias and default value to decrease the configuration length

```Bash
$ echo '[{"type":"r","doc":{"type":"csv"}},{"type":"w"}]' > my_etl.conf.json
$ cat ./data/multi_lines.csv | make run file=my_etl.conf.json
[{...}]
```

## How to contribute

In progress...

After code modifications, please run all tests.

```Bash
make test
```

## Useful links

* [Benchmark report](https://jmfiaschi.github.io/chewdata/benches/main/)
* [Documentation](https://jmfiaschi.github.io/chewdata-docs/)
* [Package](https://crates.io/crates/chewdata)
