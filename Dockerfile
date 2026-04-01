FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/sprawl-relay ./cmd/sprawl/main.go

FROM gcr.io/distroless/static:nonroot

WORKDIR /app

COPY --from=builder /out/sprawl-relay /app/sprawl-relay

EXPOSE 27313 8081

USER nonroot:nonroot

ENTRYPOINT ["/app/sprawl-relay"]