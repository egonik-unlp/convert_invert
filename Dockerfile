FROM rust:1-bookworm AS build

WORKDIR /usr/local/src/app
COPY . .
RUN cargo build --release --bin trigger_server --bin convert-invert

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /usr/local/src/app/target/release/trigger_server /usr/local/bin/trigger_server
COPY --from=build /usr/local/src/app/target/release/convert-invert /usr/local/bin/convert-invert
EXPOSE 3124
CMD ["trigger_server"]
