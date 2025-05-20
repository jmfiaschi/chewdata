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

test: start unit-tests integration-tests

test_docs:
    cargo test --doc --features "xml csv parquet toml bucket curl mongodb psql"

test_docs_by_feature:
    cargo test --doc
    cargo test --doc --features "xml"
    cargo test --doc --features "csv"
    cargo test --doc --features "parquet"
    cargo test --doc --features "toml"
    cargo test --doc --features "bucket csv"
    cargo test --doc --features "curl xml"
    cargo test --doc --features "mongodb"
    cargo test --doc --features "psql"

test_libs:
    cargo test --lib --features "xml csv parquet toml bucket curl mongodb psql"

test_libs_by_feature:
    cargo test --lib
    cargo test --lib --features "xml"
    cargo test --lib --features "csv"
    cargo test --lib --features "parquet"
    cargo test --lib --features "toml"
    cargo test --lib --features "bucket csv"
    cargo test --lib --features "curl xml"
    cargo test --lib --features "mongodb"
    cargo test --lib --features "psql"

test_integration:
    cargo test --tests --features "xml csv parquet toml bucket curl mongodb psql"

unit-tests: start test_libs

integration-tests: start test_docs test_integration

# Lint with all features.
lint:
    cargo clippy --all-features

coverage: start
    cargo tarpaulin --out Xml --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

coverage_ut: start
    rustup toolchain install nightly
    cargo install cargo-tarpaulin
    cargo +nightly tarpaulin --out Xml --lib --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

coverage_it: start
    cargo tarpaulin --out Xml --doc --tests --skip-clean --jobs 1 --features "xml csv parquet toml bucket curl mongodb psql"

# Benchmark the project.
bench: httpbin
    cargo criterion --benches --output-format bencher --plotting-backend disabled --features "xml csv parquet toml bucket curl mongodb psql"

# Start minio in local.
minio:
    @echo "Run Minio server."
    @echo "Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY}"
    podman-compose up -d nginx

minio_install:
    @echo "Configure Minio server."
    podman-compose run --rm mc alias set s3 http://nginx:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
    podman-compose run --rm mc mb -p s3/my-bucket
    podman-compose run --rm mc cp -r /root/data s3/my-bucket

# Start httpbin APIs in local.
httpbin:
    @echo "Run httpbin server."
    @echo "Host: http://localhost:8080 "
    podman-compose up -d httpbin

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
    @echo "Host: http://localhost:8081 "
    podman-compose up -d adminer

# Start keycloak server in local.
keycloak:
    @echo "Run keycloak"
    @echo "Host: http://localhost:8083 "
    podman-compose up -d keycloak-ready

# Start APM server in local.
apm:
    @echo "Run monitoring"
    @echo "Host: http://localhost:16686 "
    podman-compose up -d monitoring

# Start rabbitmq server in local.
rabbitmq:
    @echo "Run rabbitmq"
    @echo "Host: http://localhost:15672 "
    podman-compose up -d rabbitmq
    @echo "Init rabbitmq"
    curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X PUT ${RABBITMQ_ENDPOINT}/api/exchanges/%2f/users.event -d "{\"type\":\"direct\",\"auto_delete\":false,\"durable\":true,\"internal\":false,\"arguments\":{}}"
    curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X PUT ${RABBITMQ_ENDPOINT}/api/queues/%2f/users.events -d "{\"auto_delete\":false,\"durable\":true,\"arguments\":{}}"
    curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X POST ${RABBITMQ_ENDPOINT}/api/bindings/%2f/e/users.event/q/users.events -d "{\"routing_key\":\"\",\"arguments\":{}}"

semantic-release:
    npx semantic-release

# Start all servers
start: debug minio minio_install httpbin mongo keycloak

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
