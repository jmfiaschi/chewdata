# Chewdata Examples

## Run an example

### With Make

Edit the .env to custom the log level and services information

```Bash
$ make example //List all examples
$ make example name=read_write-xml
```

With an available service
```Bash
$ make example name=read_write-mongodb
```

### With cargo

Define environment variables at the begin of the command.

```Bash
$ cargo run --example //List all examples
$ RUST_LOG=INFO cargo run --example read_write-xml
```

With an available service
```Bash
$ RUST_LOG=INFO MONGODB_ENDPOINT=mongodb://admin:admin@localhost:27017 MONGODB_USERNAME=admin MONGODB_PASSWORD=admin cargo run --example read_write-mongodb
```
