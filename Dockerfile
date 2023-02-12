FROM rust as builder

RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y musl-tools musl-dev
RUN update-ca-certificates

RUN cargo init --name dummy
COPY Cargo.toml .
COPY Cargo.lock .
RUN cargo build --target x86_64-unknown-linux-musl --release
COPY src/ src/
RUN touch src/main.rs # force rebuild main.rs
RUN cargo build --target x86_64-unknown-linux-musl --release
COPY frontend/ frontend/

FROM alpine as runtime

COPY --from=builder /target/x86_64-unknown-linux-musl/release/exchange-archive ./
COPY --from=builder /frontend /frontend/

CMD ["./exchange-archive"]
EXPOSE 2137
