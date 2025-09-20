FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY backend/go.mod go.sum ./
RUN go mod download

COPY backend .
RUN go build -o main .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/main .
COPY --from=builder /app/examples ./examples/

EXPOSE 8080
CMD ["./main"]