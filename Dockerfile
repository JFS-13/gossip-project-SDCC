# ==========================================
# STAGE 1: Build dell'eseguibile
# ==========================================
# Usiamo l'immagine ufficiale di Go basata su Alpine (molto leggera)
FROM golang:1.26-alpine AS builder

# Impostiamo la cartella di lavoro dentro il container
WORKDIR /app

# Copiamo i file delle dipendenze per scaricarle (sfruttiamo la cache di Docker)
COPY go.mod go.sum ./
RUN go mod download

# Copiamo tutto il resto del codice sorgente
COPY . .

# Compiliamo l'applicazione creando l'eseguibile chiamato "gossip-node"
# Disabilitiamo CGO per avere un binario statico completamente indipendente
RUN CGO_ENABLED=0 GOOS=linux go build -o /gossip-node ./cmd/node/main.go

# ==========================================
# STAGE 2: Creazione dell'immagine finale
# ==========================================
# Usiamo un'immagine vuota e leggerissima per eseguire il binario
FROM alpine:latest

WORKDIR /app

# Copiamo l'eseguibile compilato dallo STAGE 1
COPY --from=builder /gossip-node /app/gossip-node

# Dichiariamo che l'applicazione userà la porta 8000 (a scopo informativo)
EXPOSE 8000

# Punto di ingresso: esegue il nodo passando il percorso della configurazione
# (Il percorso esatto del file YAML verrà passato tramite Docker Compose in seguito)
ENTRYPOINT ["/app/gossip-node"]