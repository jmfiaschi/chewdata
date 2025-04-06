include .env
export $(shell sed "s/=.*//" .env)

.SILENT:
.PHONY: setup build exec test bench help minio httpbin clean docs debug keycloak run example release coverage lint

debug:
	@rustup -V
	@cargo -V

help: ##		Display all commands.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
	@cargo run -- --help

setup: ##		Install all cargo extension. 
setup: ##			USAGE: make setup
	@cargo install cargo-edit
	@cargo install cargo-criterion
	@cargo install cargo-tarpaulin

build: ##		Build the script in local without examples
build: ##			USAGE: make build
	@cargo build --lib --bins --tests --benches --features "xml csv parquet toml bucket curl mongodb psql"

run: debug
run: ##		Launch the script in local.
run: ##			USAGE: make run
run: ##			USAGE: make run json='[{"type":"r"},{"type":"w"}]'
run: ##			USAGE: make run file=./data/one_line.json
	@if [ "$(json)" ]; then\
		cargo run '$(json)';\
	fi
	@if [ "$(file)" ]; then\
		cargo run -- --file $(file);\
	fi
	@if [ -z "$(file)" ] && [ -z "$(json)" ] && [ -z "$(PIPE)" ]; then\
		cargo run;\
	fi

example: start
example: ##	Run an example with all the features enabled.
example: ##		USAGE: make example name=local-json
	@if [ -z $(name) ]; then\
		echo "$(RED)USAGE: example name=[EXAMPLE_NAME]${NC}";\
		cargo run --example;exit 1;\
	fi
	@cargo run --example $(name)  --all-features

release: ##	Released with minimum features.
release: ##		USAGE: make release
	@cargo build --release --lib --bins

test: start unit-tests integration-tests

test\:docs:
	@cargo test --doc  --features "xml csv parquet toml bucket curl mongodb psql"

test\:docs\:by_feature:
	@cargo test --doc
	@cargo test --doc --features "xml"
	@cargo test --doc --features "csv"
	@cargo test --doc --features "parquet"
	@cargo test --doc --features "toml"
	@cargo test --doc --features "bucket csv"
	@cargo test --doc --features "curl xml"
	@cargo test --doc --features "mongodb"
	@cargo test --doc --features "psql"

test\:libs:
	@cargo test --lib  --features "xml csv parquet toml bucket curl mongodb psql"

test\:libs\:by_feature:
	@cargo test --lib
	@cargo test --lib --features "xml"
	@cargo test --lib --features "csv"
	@cargo test --lib --features "parquet"
	@cargo test --lib --features "toml"
	@cargo test --lib --features "bucket csv"
	@cargo test --lib --features "curl xml"
	@cargo test --lib --features "mongodb"
	@cargo test --lib --features "psql"

test\:integration:
	@cargo test --tests  --features "xml csv parquet toml bucket curl mongodb psql"

unit-tests: start test\:libs

integration-tests: start test\:docs test\:integration

lint: ##		Lint with all features.
lint: ##			USAGE: make lint
	@cargo clippy --all-features

coverage: ##	Run code coverage with all features.
coverage: ##		USAGE: make coverage
coverage: start
coverage:
	@cargo tarpaulin --out Xml --skip-clean --jobs 1  --features "xml csv parquet toml bucket curl mongodb psql"

coverage\:ut: start
coverage\:ut:
	@rustup toolchain install nightly
	@cargo install cargo-tarpaulin
	@cargo +nightly tarpaulin --out Xml --lib --skip-clean --jobs 1  --features "xml csv parquet toml bucket curl mongodb psql"

coverage\:it: start
coverage\:it:
	@cargo tarpaulin --out Xml --doc --tests --skip-clean --jobs 1  --features "xml csv parquet toml bucket curl mongodb psql"

bench: ##		Benchmark the project.
bench: ##			USAGE: make bench
bench:
	@cargo criterion --benches --output-format bencher --plotting-backend disabled  --features "xml csv parquet toml bucket curl mongodb psql" 2>&1

minio: ##		Start minio in local.
minio: ##			USAGE: make minio
minio:
	echo "${BLUE}Run Minio server.${NC}"
	echo "${YELLOW}Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY} ${NC}"
	@docker-compose up -d minio nginx

minio\:install:
	echo "${BLUE}Configure Minio server.${NC}"
	@docker-compose run --rm mc alias set s3 http://nginx:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
	@docker-compose run --rm mc mb -p s3/my-bucket
	@docker-compose run --rm mc cp -r /root/data s3/my-bucket

httpbin: ##	Start httpbin APIs in local.
httpbin: ##		USAGE: make httpbin
httpbin:
	echo "${BLUE}Run httpbin server.${NC}"
	echo "${YELLOW}Host: http://localhost:8080 ${NC}"
	@docker-compose up -d httpbin

mongo: ##		Start mongo server in local.
mongo: ##			USAGE: make mongo
mongo:
	echo "${BLUE}Run mongo server.${NC}"
	@docker-compose up -d mongo-admin mongo

psql: ##		Start psql server in local.
psql: ##			USAGE: make psql
psql:
	echo "${BLUE}Run psql server.${NC}"
	@docker-compose up -d psql

adminer: ##	Start db admin in local.
adminer: ##		USAGE: make adminer
adminer:
	echo "${BLUE}Run admin db${NC}"
	echo "${YELLOW}Host: http://localhost:8081 ${NC}"
	@docker-compose up -d adminer

keycloak: ##	Start keycloak server in local.
keycloak: ##		USAGE: make keycloak
keycloak:
	echo "${BLUE}Run keycloak${NC}"
	echo "${YELLOW}Host: http://localhost:8083 ${NC}"
	@docker-compose up -d keycloak

apm: ##		Start APM server in local.
apm: ##			USAGE: make apm
apm:
	echo "${BLUE}Run monitoring${NC}"
	echo "${YELLOW}Host: http://localhost:16686 ${NC}"
	@docker-compose up -d monitoring

rabbitmq: ##	Start rabbitmq server in local.
rabbitmq: ##		USAGE: make rabbitmq
rabbitmq:
	echo "${BLUE}Run rabbitmq${NC}"
	echo "${YELLOW}Host: http://localhost:15672 ${NC}"
	@docker-compose up -d rabbitmq
	echo "${BLUE}Init rabbitmq${NC}"
	curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X PUT ${RABBITMQ_ENDPOINT}/api/exchanges/%2f/users.event -d"{\"type\":\"direct\",\"auto_delete\":false,\"durable\":true,\"internal\":false,\"arguments\":{}}"
	curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X PUT ${RABBITMQ_ENDPOINT}/api/queues/%2f/users.events -d"{\"auto_delete\":false,\"durable\":true,\"arguments\":{}}"
	curl -i -u ${RABBITMQ_USERNAME}:${RABBITMQ_PASSWORD} -H "content-type:application/json" -X POST ${RABBITMQ_ENDPOINT}/api/bindings/%2f/e/users.event/q/users.events -d"{\"routing_key\":\"\",\"arguments\":{}}"

semantic-release:
	@npx semantic-release

start: ##		Start all servers in local.
start: ##			USAGE: make start
start: debug minio minio\:install httpbin mongo adminer keycloak

stop: ##		Stop all servers in local.
stop: ##			USAGE: make stop
stop:
	@docker-compose down

clean: ##		Clean the project in local.
clean: ##			USAGE: make clean
clean: stop
clean:
	@sudo rm -Rf .cache
	@cargo clean

version: ##	Get the current project version.
version: ##		USAGE: make version
version:
	@grep -Po '\b^version\s*=\s*"\K.*?(?=")' Cargo.toml | head -1

docker\:build:
	@docker build -t chewdata .

# Shell colors.
RED=\033[0;31m
LIGHT_RED=\033[1;31m
GREEN=\033[0;32m
LIGHT_GREEN=\033[1;32m
ORANGE=\033[0;33m
YELLOW=\033[1;33m
BLUE=\033[0;34m
LIGHT_BLUE=\033[1;34m
PURPLE=\033[0;35m
LIGHT_PURPLE=\033[1;35m
CYAN=\033[0;36m
LIGHT_CYAN=\033[1;36m
NC=\033[0m
