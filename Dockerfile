FROM golang:1.26.4-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags="-s -w" -trimpath -o /bin/timberdb ./cmd/timberdb

FROM alpine:3.21

RUN addgroup -S timberdb && adduser -S timberdb -G timberdb

COPY --from=builder /bin/timberdb /usr/local/bin/timberdb

USER timberdb

ENTRYPOINT ["timberdb"]
