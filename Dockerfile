FROM rust:1.93-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    make \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/ember
COPY . .

RUN cargo build --release --bin ember-server --features grpc

# ---

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 65532 ember && \
    useradd -u 65532 -g ember -s /sbin/nologin -M ember

COPY --from=builder /usr/src/ember/target/release/ember-server /usr/local/bin/ember-server

RUN mkdir -p /data && chown ember:ember /data
VOLUME /data

USER ember

ENV EMBER_HOST=0.0.0.0

EXPOSE 6379
EXPOSE 6380
EXPOSE 9100

LABEL org.opencontainers.image.title="ember" \
      org.opencontainers.image.description="low-latency distributed cache" \
      org.opencontainers.image.source="https://github.com/kacy/ember"

ENTRYPOINT ["ember-server"]
