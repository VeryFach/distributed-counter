FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o counter-server ./cmd/server

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/counter-server .
COPY --from=builder /app/configs ./configs

EXPOSE 50051 8080

CMD ["./counter-server", "-config", "configs/config.yaml"]