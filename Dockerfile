# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:latest AS builder

WORKDIR /app

# Fetch dependencies first — this layer is cached as long as Cargo.toml /
# Cargo.lock are unchanged, even when src/ changes.
COPY Cargo.toml Cargo.lock ./
# Stub binary lets Cargo resolve targets; this layer caches dep downloads
# as long as Cargo.toml / Cargo.lock are unchanged.
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs \
 && cargo fetch --locked \
 && rm -rf src

# Build the actual binary.
COPY src ./src
RUN cargo build --release --bin db-proxy-rs

# ── Stage 2: minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/db-proxy-rs /usr/local/bin/

EXPOSE 3307

ENV RUST_LOG=info

ENTRYPOINT ["db-proxy-rs"]
