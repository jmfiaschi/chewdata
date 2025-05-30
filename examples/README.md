# Chewdata Examples

## Run an example

### With Just

Edit the .env to custom the log level and services information

```Bash
just example EXAMPLE_NAME
```

With an available service

```Bash
just example mongodb
```

### With cargo

Define environment variables at the begin of the command.

```Bash
cargo run --example //List all examples
RUST_LOG=INFO cargo run --example local-xml --features "xml"
```

With an available service

```Bash
RUST_LOG=INFO MONGODB_ENDPOINT=mongodb://admin:admin@localhost:27017 MONGODB_USERNAME=admin MONGODB_PASSWORD=admin cargo run --example mongodb --features "mongodb"
```
