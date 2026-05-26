ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH

RUN go build \
    -ldflags="-s -w -X github.com/leko/ma-dlna/internal/version.Version=${VERSION} -X github.com/leko/ma-dlna/internal/version.Commit=${COMMIT}" \
    -o /dlna-ma-bridge ./cmd/dlna-ma-bridge

FROM alpine:3.20

RUN apk add --no-cache \
    ffmpeg \
    ca-certificates

COPY --from=builder /dlna-ma-bridge /dlna-ma-bridge

EXPOSE 8787

ENTRYPOINT ["/dlna-ma-bridge"]
CMD ["-config", "/config/config.yaml"]
