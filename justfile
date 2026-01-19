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
    cargo build --lib --bins --tests --benches --features "xml csv parquet toml bucket curl mongodb psql"

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
    cargo run '{{json_config}}'

# Run the project with json 'file_path' in argument
run-with-file file_path: debug
    cargo run -- --file '{{file_path}}'

# Run the project without arguments
run: debug
    cargo run

# Run an example with all features enabled
example name:
    cargo run --example '{{name}}' --all-features

# Build for release with minimum features
release:
    cargo build --release --lib --bins

test: start test-basic test-csv test-bucket test-postgres test-curl

test-basic:
    cargo test --tests --features "ordered"
    cargo test --examples --features "ordered"

test-csv:
    cargo test --features "csv"
    cargo test --examples --features "csv"

test-bucket: 
    cargo test --features "bucket"
    cargo test --examples --features "bucket"

test-postgres: psql
    cargo test --features "psql"
    cargo test --examples --features "psql"

test-curl:
    cargo test --features "curl"
    cargo test --examples --features "curl"

# Lint with all features.
lint:
    cargo clippy --all-features

coverage: start
    cargo tarpaulin --out Xml --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

coverage_ut:
    rustup toolchain install nightly
    cargo install cargo-tarpaulin
    cargo +nightly tarpaulin --out Xml --lib --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

coverage_it:
    cargo tarpaulin --out Xml --doc --tests --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

# Benchmark the project.
bench cpus="1": http-mock
    cargo criterion --benches \
    --output-format bencher \
    --jobs {{cpus}} \
    --plotting-backend disabled \
    --features "xml csv parquet toml bucket curl mongodb psql" 2>&1

# Start minio in local.
minio:
    @echo "Run Minio server."
    @echo "Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY}"
    podman-compose up -d minio

minio_install:
    @echo "Configure Minio server."
    podman-compose run --rm mc alias set s3 http://minio:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
    podman-compose run --rm mc mb -p s3/my-bucket
    podman-compose run --rm mc cp -r /root/data s3/my-bucket

# Start mockhttp APIs in local.
http-mock:
    @echo "Run http mock server."
    @echo "Host: http://localhost:8080"
    podman-compose up -d http-mock

https-mock:
    @echo "Run http mock server."
    @echo "Host: https://localhost:8084"
    podman-compose up -d https-mock

# Start mongo server in local.
mongo:
    @echo "Run mongo server."
    podman-compose up -d mongo

mongo-admin:
    @echo "Run mongo admin server."
    podman-compose up -d mongo-admin

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
start: stop debug minio_install http-mock https-mock mongo keycloak rabbitmq

# Stop all servers
stop:
    podman-compose down

# Clean the project and stop servers
clean: stop
    sudo rm -Rf .cache
    cargo clean

version:
    grep -Po '\b^version\s*=\s*"\K.*?(?=")' Cargo.toml | head -1

podman_build:
    podman build -t chewdata .
