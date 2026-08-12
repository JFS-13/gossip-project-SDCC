# ==========================================
# STAGE 1: Build dell'eseguibile
# ==========================================
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copia manifest dipendenze (sfrutta la cache Docker)
COPY go.mod go.sum ./
RUN go mod download

# Copia il codice sorgente
COPY cmd ./cmd
COPY internal ./internal

# Compila il binario statico
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags='-s -w' \
    -o /gossip-agent \
    ./cmd/agent/main.go

# ==========================================
# STAGE 2: Immagine runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /app

# Copia il binario compilato
COPY --from=builder /gossip-agent /app/gossip-agent

# Porta gossip UDP
EXPOSE 7001

# Porta metrics HTTP (node_port + 1000)
EXPOSE 8001

ENTRYPOINT ["/app/gossip-agent"]
CMD ["--config", "/app/configs/config.yaml"]