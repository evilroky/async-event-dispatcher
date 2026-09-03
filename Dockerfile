from golang:1.27.1-alpine3.24 AS builder

RUN apk add --no-cache git ca-certificates tzdata
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /app/bin/app ./cmd/api/main.go

FROM alpine:3.24

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -g '' appuser
WORKDIR /app
COPY --from=builder /app/bin/app /app/app

USER appuser
EXPOSE 8080

ENTRYPOINT ["/app/app"]