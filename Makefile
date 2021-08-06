include .env
export $(shell sed 's/=.*//' .env)

.SILENT:
.PHONY: build exec test bench help minio minio-install httpbin clean

help: ## Display all commands.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

build: ## Build the script in local
	@cargo build

run-file: ## Launch the script in local
	@if [ -z $(file) ]; then\
		echo "$(RED)USAGE: make run file=[FILE_PATH]${NC}";exit 1;\
	fi
	@cargo run -- --file $(file)

run: ## Launch the script in local
	@if [ -z "$(json)" ]; then\
		echo "$(RED)USAGE: make run json=[JSON]${NC}";\
	fi
	@cargo run '$(json)'

example:
	@if [ -z $(name) ]; then\
		echo "$(RED)USAGE: example name=[EXAMPLE_NAME]${NC}";\
		cargo run --example;exit 1;\
	fi
	@cargo run --example $(name)

release: ## Released the script in local
	@cargo build --release --test-threads=1

test: start
test: ## Launch all tests in local
	@cargo test --doc connector::bucket_select::BucketSelect::fetch -- --test-threads=1 ${name} 
	@cargo test --lib -- --test-threads=1 ${name} 
	@cargo test --tests -- --test-threads=1 ${name}

lint:
	@cargo clippy

coverage-ut: start
coverage-ut:
	@rustup toolchain install nightly
	@cargo install cargo-tarpaulin
	@cargo +nightly tarpaulin --out Xml --verbose --doc --lib --skip-clean --timeout 1200

coverage-it: start
coverage-it:
	@cargo install cargo-tarpaulin
	@cargo tarpaulin --out Xml --verbose --tests --skip-clean --timeout 1200

minio:
	echo "${BLUE}Run Minio server.${NC}"
	echo "${YELLOW}Host: http://localhost:9000 | Credentials: ${BUCKET_ACCESS_KEY_ID}/${BUCKET_SECRET_ACCESS_KEY} ${NC}"
	@docker-compose up -d minio

minio-install:
	echo "${BLUE}Configure Minio server.${NC}"
	@docker-compose run --rm mc config host add s3 http://minio:9000 ${BUCKET_ACCESS_KEY_ID} ${BUCKET_SECRET_ACCESS_KEY} --api s3v4
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

start: minio minio-install httpbin mongo

stop: docker-compose down

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
