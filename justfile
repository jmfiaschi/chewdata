set dotenv-load
set dotenv-required

# Tasks
debug:
    rustup -V
    cargo -V

# Install cargo extensions
setup:
    cargo install cargo-edit
    cargo install cargo-criterion
    cargo install cargo-tarpaulin

# Build the project
build:
    cargo build --lib --bins --tests --benches --features "ordered,xml,csv,parquet,toml,bucket,curl,mongodb,psql"

build-feature-csv:
    cargo build --lib --bins --tests --benches --features "csv"

build-feature-xml:
    cargo build --lib --bins --tests --benches --features "xml"

build-feature-parquet:
    cargo build --lib --bins --tests --benches --features "parquet"

build-feature-toml:
    cargo build --lib --bins --tests --benches --features "toml"

build-feature-bucket:
    cargo build --lib --bins --tests --benches --features "bucket"

build-feature-curl:
    cargo build --lib --bins --tests --benches --features "curl"

build-feature-psql:
    cargo build --lib --bins --tests --benches --features "psql"

build-feature-mongodb:
    cargo build --lib --bins --tests --benches --features "mongodb"

build-feature-apm:
    cargo build --lib --bins --tests --benches --features "apm"

# Run the project with 'json_config' data in argument
run-with-json json_config: debug
    cargo run --all-features '{{json_config}}'

# Run the project with json 'file_path' in argument
run-with-file file_path: debug
    cargo run --all-features -- --file '{{file_path}}'

# Run the project without arguments
run: debug
    cargo run

# Run an example with all features enabled
example name:
    cargo run --example '{{name}}' --all-features

# Build for release with minimum features
release:
    cargo build --release --lib --bins

test: start test-basic test-xml test-csv test-toml test-parquet test-bucket test-psql test-curl test-mongodb

test-basic:
    cargo test --tests --features "ordered"
    cargo test --examples --features "ordered"
    cargo test --doc --features "ordered"

test-xml:
    cargo test --tests --features "ordered,xml"
    cargo test --examples --features "ordered,xml"
    cargo test --doc --features "ordered,xml"

test-csv:
    cargo test --tests --features "ordered,csv"
    cargo test --examples --features "ordered,csv"
    cargo test --doc --features "ordered,csv"

test-toml:
    cargo test --tests --features "ordered,toml"
    cargo test --examples --features "ordered,toml"
    cargo test --doc --features "ordered,toml"

test-parquet:
    cargo test --tests --features "ordered,parquet"
    cargo test --examples --features "ordered,parquet"
    cargo test --doc --features "ordered,parquet"

test-bucket: minio-install
    cargo test --tests --features "ordered,bucket,csv,parquet"
    cargo test --examples --features "ordered,bucket,csv,parquet"
    cargo test --doc --features "ordered,bucket,csv,parquet"

test-psql: psql
    cargo test --tests --features "ordered,psql"
    cargo test --examples --features "ordered,psql"
    cargo test --doc --features "ordered,psql"

test-curl: http-mock https-mock keycloak rabbitmq
    cargo test --tests --features "ordered,curl"
    cargo test --examples --features "ordered,curl"
    cargo test --doc --features "ordered,curl"

test-mongodb: mongodb
    cargo test --tests --features "ordered,mongodb"
    cargo test --examples --features "ordered,mongodb"
    cargo test --doc --features "ordered,mongodb"

# Lint with all features.
lint:
    cargo clippy --all-features

coverage: start
    cargo tarpaulin --out Xml --skip-clean --jobs 1 --features "ordered,xml,csv,parquet,toml,bucket,curl,mongodb,psql"

# Benchmark the project.
bench cpus="1": http-mock
    cargo criterion --benches \
    --output-format bencher \
    --jobs {{cpus}} \
    --plotting-backend disabled \
    --features "xml,csv,parquet,toml,bucket,curl,mongodb,psql" 2>&1

# Start minio in local.
minio:
    @echo "Run Minio server."
    @echo "Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY}"
    podman-compose up -d minio

minio-install:
    @echo "Configure Minio server."
    podman-compose run --rm mc alias set s3 http://minio:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
    podman-compose run --rm mc mb -p s3/my-bucket
    podman-compose run --rm mc mirror /root/data s3/my-bucket/data

# Start mockhttp APIs in local.
http-mock:
    @echo "Run http mock server."
    @echo "Host: http://localhost:8080"
    podman-compose up -d http-mock

https-mock:
    @echo "Run http mock server."
    @echo "Host: https://localhost:8084"
    podman-compose up -d https-mock

# Start mongodb server in local.
mongodb:
    @echo "Run mongodb server."
    podman-compose up -d mongodb

mongodb-admin:
    @echo "Run mongodb admin server."
    podman-compose up -d mongodb-admin

# Start psql server in local.
psql:
    @echo "Run psql server."
    podman-compose up -d psql

# Start db admin in local.
adminer:
    @echo "Run admin db"
    @echo "Host: http://localhost:8081"
    podman-compose up -d adminer

# Start keycloak server in local.
keycloak:
    @echo "Run keycloak"
    @echo "Host: http://localhost:8083"
    podman-compose up -d keycloak-ready

# Start APM server in local.
apm:
    @echo "Run monitoring"
    @echo "Host: http://localhost:16686"
    podman-compose up -d monitoring

# Start rabbitmq server in local.
rabbitmq:
    @echo "Run rabbitmq"
    @echo "Host: http://localhost:15672"
    podman-compose up -d rabbitmq-ready

semantic-release:
    npx semantic-release

# Start all servers
start: stop debug minio-install http-mock https-mock mongodb keycloak rabbitmq

# Stop all servers
stop:
    podman-compose down

# Clean the project and stop servers
clean: stop
    sudo rm -Rf .cache
    sudo rm -Rf data/out/*
    cargo clean

version:
    sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1

podman-build:
    podman build -t chewdata .
