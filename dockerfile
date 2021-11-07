FROM rust:latest
LABEL authors=jmfiaschi

WORKDIR /usr/src/myapp
COPY . .

RUN cargo build --release

CMD ["target/release/chewdata"]
