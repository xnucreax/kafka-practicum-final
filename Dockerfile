FROM ubuntu:24.04 AS builder

ARG GO_VERSION=1.25.5
ARG TARGETARCH=arm64

RUN apt-get update && apt-get install -y curl gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" \
    | tar -C /usr/local -xz

ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN go build -o /app/bin/app ./cmd/


FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/bin/app /app

ENTRYPOINT ["/app"]
