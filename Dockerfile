FROM golang:1.22-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /dlna-ma-bridge ./cmd/dlna-ma-bridge

EXPOSE 8787

ENTRYPOINT ["/dlna-ma-bridge"]
CMD ["-config", "/config/config.yaml"]
