FROM rust:1.75 as builder

ADD ./src ./builder/src
ADD ./client ./builder/client
ADD ./benches ./builder/benches
ADD ./Cargo.toml ./builder/Cargo.toml
ADD ./tests/sqllogictest ./builder/tests/sqllogictest
ADD ./config/Settings.toml ./builder/Settings.toml

WORKDIR /builder

RUN rustup default stable
RUN cargo build --release

FROM ubuntu

ARG APP_SERVER=piggysql

WORKDIR /piggy

ENV IP="127.0.0.1"

EXPOSE 5432

COPY --from=builder /builder/Settings.toml config/Settings.toml
COPY --from=builder /builder/target/release/${APP_SERVER} ${APP_SERVER}

ENTRYPOINT ["./piggysql"]