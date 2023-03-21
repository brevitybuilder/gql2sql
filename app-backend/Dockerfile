# Builder stage
FROM messense/rust-musl-cross:x86_64-musl as builder
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN apt-get update && \
    apt-get install -y \
        musl-tools \
        musl-dev \
        && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src

# Create a new empty shell project
RUN USER=root cargo new --bin app-backend
WORKDIR /usr/src/app-backend
COPY Cargo.toml Cargo.lock ./
RUN echo "caching built dependencies" && \
  cargo build --target x86_64-unknown-linux-musl --release

COPY src ./src
COPY migrations ./migrations
RUN echo "building for release" && \
    rm ./target/x86_64-unknown-linux-musl/release/deps/app_backend* && \
    cargo build --target x86_64-unknown-linux-musl --release && \
    musl-strip ./target/x86_64-unknown-linux-musl/release/app-backend

# Deployment stage
FROM alpine:3.17
RUN adduser -D -u 1000 brevity
RUN apk add --no-cache ca-certificates
COPY --chown=brevity --chmod=765 ./scripts/init.sh .
COPY --from=builder /usr/src/app-backend/target/x86_64-unknown-linux-musl/release/app-backend /usr/local/bin/app-backend
COPY --from=builder /usr/src/app-backend/migrations /usr/local/etc/app-backend/migrations/
COPY --from=supabase/gotrue:v2.54.0 /usr/local/bin/gotrue /usr/local/bin/gotrue
COPY --from=supabase/gotrue:v2.54.0 /usr/local/etc/gotrue/migrations /usr/local/etc/gotrue/migrations/
ENV GOTRUE_DB_MIGRATIONS_PATH /usr/local/etc/gotrue/migrations
EXPOSE 8080
USER brevity
CMD ["./init.sh"]
