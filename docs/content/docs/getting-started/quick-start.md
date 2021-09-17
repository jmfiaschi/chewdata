+++
title = "Quick Start"
description = "One page summary of how to start a new Chewdata project."
date = 2021-09-05T08:20:00+00:00
updated = 2021-09-05T08:20:00+00:00
draft = false
weight = 20
sort_by = "weight"
template = "docs/page.html"

[extra]
lead = "One page summary of how to start a new Chewdata project."
toc = true
top = false
+++

## 1 - Installation

### Requirements

Before using the theme, you need to install the [rustup](https://www.rust-lang.org/tools/install).

After that, you can install Chewdata in two ways

### From the source code

Go the the Chewdata project in github and follow the [installation steps](https://github.com/jmfiaschi/chewdata#setup-from-source-code).

### From cargo package

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
cargo install chewdata --no-default-features --features "use_xml_document use_bucket_connector"
```
Please, referer to the [features documentation](/docs/componants/feature)</a>.

## 2 - Run

First of all, you can check how the command works with the option `--help`
```bash
$ chewdata --help
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

### Without configuration

It is possible to run the command without configuration
```bash
$ echo '{"field1":"value1"}' | chewdata
[{"field1":"value1"}]
```
 Without configuration in input, the command will use these steps :
* Reader: with `io` connector + `json` document 
* Writer: with `io` connector + `json` document

The program will deserialize the data in input and serialize the data into the output.


### With configuration in input

With configuration, you can customize the behaviour of your command. The same command as before will be
```bash
$ echo '{"field1":"value1"}' | chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"io"}},{"type":"writer","document":{"type":"json"},"connector":{"type":"io"}}]'
[{"field1":"value1"}]
```
 With this configuration in input, the command will use these steps :
* Reader: with `io` connector + `json` document 
* Writer: with `io` connector + `json` document

If you want the same output into an object, use the document type `jsonl`
```bash
$ echo '{"field1":"value1"}' | chewdata '[{"type":"reader","document":{"type":"json"},"connector":{"type":"io"}},{"type":"writer","document":{"type":"jsonl"},"connector":{"type":"io"}}]'
{"field1":"value1"}
```
 With this configuration in input, the command will use these steps :
* Reader: with `io` connector + `json` document 
* Writer: with `io` connector + `jsonl` document

### Chain commands

It's possible to chaine CLI calls in order to do transactional actions

Execute the second call if the first is finished
```bash
A=$(echo '{"variable": "a"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"10"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
B=$(echo '{"variable": "b"}' | chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"/","pattern":"{{ input | json_encode() }}"},{"field":"value","pattern":"20"}]},{"type":"w", "doc":{"type":"jsonl"}}]') &&\
echo $A | VAR_B=$B chewdata '[{"type":"r"},{"type":"transformer","actions":[{"field":"var_b","pattern":"{{ get_env(name=\"VAR_B\") }}"},{"field":"result","pattern":"{{ output.var_b.value * input.value }}"},{"field":"var_b","type":"remove"}]},{"type":"w"}]'
[{"result":200}]
```
*The step `reader` read the variable `A` and the transformer put the result into the `input` object. 
The first action fetch the variable `B` into the environment variable and put the result into the `output` object. 
A string json is automaticaly transform in an object at the end of an action.
The next action multiply the value of the `var_b` and the `input` and store the result into the field `result` of the `output` object.
When all the actions are executed, the `output` is passed to the `writer` and `write` into the connector.*

### Write data through multi connectors

It is possible to write data into different connectors
```bash
echo '[{"field":"value1"},{"field":"value2"}]' | chewdata '[{"type":"r"},{"type":"t","actions":[{"field":"field","pattern":"{% if input.field == \"value2\" %}{{ throw(message=\"I throw an error!\") }}{% else %}{{ input | json_encode() }}{% endif %}"}]},{"type":"w","data_type":"ok"},{"type":"w","data_type":"err"}]'
[{"field":{"field":"value1"}}][{"field":"value2","_error":"Failed to render the field 'field'. I throw an error!"}]
```
*Here we can write data without issue into a connector and failed data into another connector with the message of the error.*

## With configuration into a file

For better readable and versionable configuration, store your configuration into :

* Json file
```bash
$ echo '[{"type":"reader","document":{"type":"json"},"connector":{"type":"io"}},{"type":"writer","document":{"type":"jsonl"},"connector":{"type":"io"}}]' > my_new_config.json
$ echo '{"field1":"value1"}' | chewdata --file my_new_config.json
{"field1":"value1"}
```

* Yaml file
```bash
$ echo -e '---\n'\
'type: reader\n'\
'document:\n'\
'   type: json\n'\
'connector:\n'\
'   type: io\n'\
'---\n'\
'type: writer\n'\
'document:\n'\
'   type: jsonl\n'\
'connector:\n'\
'   type: io' > my_new_config.yaml
$ echo '{"field1":"value1"}' | chewdata --file my_new_config.yaml
{"field1":"value1"}
```

If you want to understand how works the configuration file, please go to the [configuration documentation](/docs/componants/configuration).
