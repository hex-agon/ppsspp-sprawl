FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/sprawl ./cmd/sprawl/main.go

FROM gcr.io/distroless/static:nonroot

WORKDIR /app

COPY --from=builder /out/sprawl /app/sprawl

EXPOSE 27312 27313 8081

USER nonroot:nonroot

ENTRYPOINT ["/app/sprawl"]