FROM rust:1.85-slim AS builder

WORKDIR /usr/src/ember
COPY . .

RUN cargo build --release --bin ember-server

# ---

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/ember/target/release/ember-server /usr/local/bin/ember-server

EXPOSE 6379
VOLUME /data

ENTRYPOINT ["ember-server"]
CMD ["--host", "0.0.0.0"]
