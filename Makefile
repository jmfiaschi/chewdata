include .env
export $(shell sed 's/=.*//' .env)

.SILENT:
.PHONY: install build exec test bench help minio minio-install httpbin clean

help: ## Display all commands.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

FILE_PATH := "$(config)"
FILE_STRING_ESCAPE := $$(cat $(FILE_PATH) | tr -d '\n')

file-as-param:
	@echo "$(FILE_STRING_ESCAPE)"

install: $(env-file) ## Install the project for a specific env.
	@echo "${BLUE}Install the project${NC}"
	@if [ -z "$$(command -v cargo)" ]; then\
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh;\
	fi
	@echo "${YELLOW} $(if $(env-file),$(env-file),".env.dist") => .env ${NC}"
	@cp $(if $(env-file),$(env-file),".env.dist") .env

build: ## Build the script in local
	@cargo build

run: ## Launch the script in local
	@if [ -z $(config) ]; then\
		echo "$(RED)USAGE: run config=[CONFIG_FILE_PATH] format=[CONFIG_FILE_FORMAT]${NC}";exit 1;\
	fi
	@cargo run "$$(make file-as-param)" $(format)

example:
	@if [ -z $(name) ]; then\
		echo "$(RED)USAGE: example name=[EXAMPLE_NAME]${NC}";\
		cargo run --example;exit 1;\
	fi
	@cargo run --example $(name)

release: ## Released the script in local
	@cargo build --release

test: minio minio-install httpbin
test: ## Launch all tests in local
	@cargo test ${test}

bench: httpbin | minio ## Launch benchmark in local
	@cargo bench

clean: ## Clean tge repo in local
	echo "${YELLOW}Run this command in sudo${NC}"
	sudo rm -Rf target
	sudo sh -c "truncate -s 0 /var/lib/docker/containers/*/*-json.log"

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
