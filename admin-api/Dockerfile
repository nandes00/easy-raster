FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -o server ./cmd

FROM alpine:latest AS certs

RUN apk add --no-cache curl && \
    curl -fso /global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/server .
COPY --from=certs /global-bundle.pem /etc/ssl/certs/global-bundle.pem

CMD ["./server"]
