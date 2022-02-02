include .env
export $(shell sed 's/=.*//' .env)

.SILENT:
.PHONY: build exec test bench help minio minio-install httpbin clean docs

help: ## Display all commands.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

build: ## Build the script in local
	@cargo build --all-targets --all-features

run: ## Launch the script in local
	@if [ "$(json)" ]; then\
		cargo run ${json};\
	fi
	@if [ "$(file)" ]; then\
		cargo run -- --file $(file);\
	fi
	@if [ -z "$(file)" ] && [ -z "$(json)" ]; then\
		echo "$(RED)USAGE:${NC}";\
		echo "$(RED)make run json=[JSON]${NC}";\
		echo "$(RED)make run file=[FILE_PATH]${NC}";exit 1;\
	fi

example: start
example:
	@if [ -z $(name) ]; then\
		echo "$(RED)USAGE: example name=[EXAMPLE_NAME]${NC}";\
		cargo run --example;exit 1;\
	fi
	@cargo run --example $(name)

release: ## Released the script in local
	@cargo build --release

test: start unit-tests integration-tests

test\:docs:
	@cargo test --doc -- ${name}

test\:libs:
	@cargo test --lib -- ${name}

test\:integration:
	@cargo test --tests -- ${name}

unit-tests: start test\:libs

integration-tests: start test\:docs test\:integration

lint:
	@cargo clippy

coverage: start
coverage:
	@cargo install cargo-tarpaulin
	@cargo tarpaulin --out Xml --verbose --skip-clean --timeout 1200

coverage\:ut: start
coverage\:ut:
	@rustup toolchain install nightly
	@cargo install cargo-tarpaulin
	@cargo +nightly tarpaulin --out Xml --verbose --lib --skip-clean --timeout 1200 --jobs 1

coverage\:it: start
coverage\:it:
	@cargo install cargo-tarpaulin
	@cargo tarpaulin --out Xml --verbose --doc --tests --skip-clean --timeout 1200

bench:
	@cargo install cargo-criterion
	@cargo criterion --output-format bencher --plotting-backend disabled 2>&1

minio:
	echo "${BLUE}Run Minio server.${NC}"
	echo "${YELLOW}Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY} ${NC}"
	@docker-compose up -d minio1 minio2 minio3 minio4 nginx

minio\:install:
	echo "${BLUE}Configure Minio server.${NC}"
	@docker-compose run --rm mc alias set s3 http://nginx:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
	@docker-compose run --rm mc mb -p s3/my-bucket
	@docker-compose run --rm mc cp -r /root/data s3/my-bucket

httpbin:
	echo "${BLUE}Run httpbin server.${NC}"
	echo "${YELLOW}Host: http://localhost:8080${NC}"
	@docker-compose up -d httpbin

mongo:
	echo "${BLUE}Run mongodb server.${NC}"
	@docker-compose up -d mongo
	echo "${BLUE}Run mongo express.${NC}"
	echo "${YELLOW}Host: http://localhost:8081${NC}"
	@docker-compose up -d mongo-express

semantic-release:
	@npx semantic-release

start: minio minio\:install httpbin mongo

stop:
	@docker-compose down

clean:
	@sudo rm -Rf .cache
	@cargo clean

docs:
	@cd docs && zola build

version:
	@grep -Po '\b^version\s*=\s*"\K.*?(?=")' Cargo.toml

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
