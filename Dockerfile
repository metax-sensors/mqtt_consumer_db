# syntax=docker/dockerfile:1
#
# Telegraf image with the mqtt_consumer_db execd plugin baked in.
#
# Build from this repository's root:
#
#   docker build -t bestmind/telegraf:latest .
#
# TELEGRAF_VERSION must match the telegraf module version in go.mod.

ARG TELEGRAF_VERSION=1.39.3
ARG GO_VERSION=1.26

# ---- build stage: compile the plugin --------------------------------------
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Download modules first so they are cached independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Only the files needed for the build (skips the local .exe, test/ etc.).
COPY main.go ./
COPY plugins/ ./plugins/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/mqtt_consumer_db .

# ---- runtime stage: stock telegraf + plugin binary ------------------------
FROM telegraf:${TELEGRAF_VERSION}-alpine

COPY --from=build /out/mqtt_consumer_db /etc/telegraf/extern_plugin/mqtt_consumer_db

# plugin.conf (credentials) and telegraf.conf are expected to be mounted at
# runtime, e.g. by the bestmind_cloud docker-compose.yml.
