FROM golang:alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

CMD ["tail", "-f", "/dev/null"]
