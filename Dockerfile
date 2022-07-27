FROM rust:latest
LABEL authors=jmfiaschi

WORKDIR /usr/src/myapp
COPY . .

ENTRYPOINT ["target/release/chewdata"]
