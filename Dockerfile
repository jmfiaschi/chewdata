FROM rust:latest AS chef
RUN cargo install cargo-chef
WORKDIR /usr/src/chewdata

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /usr/src/chewdata/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /usr/src/chewdata/target/release/chewdata /usr/local/bin/chewdata
ENTRYPOINT ["/usr/local/bin/chewdata"]
