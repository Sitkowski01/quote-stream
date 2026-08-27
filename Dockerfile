# ---------- budowanie ----------
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Zaleznosci osobno od kodu: dopoki go.mod/go.sum sie nie zmienia,
# Docker odtwarza te warstwe z cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO wylaczone -> statyczny plik binarny, ktory dziala na pustym obrazie.
# -trimpath usuwa sciezki z maszyny budujacej, -s -w tnie tablice symboli.
ARG CEL=consumer
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${CEL}

# ---------- uruchomienie ----------
FROM alpine:3.21 AS runtime

# Certyfikaty sa potrzebne, gdy API stoi za HTTPS.
RUN apk add --no-cache ca-certificates \
    && addgroup -S app && adduser -S -G app app

COPY --from=builder /out/app /usr/local/bin/app

USER app
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/app"]
