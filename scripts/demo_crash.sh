#!/bin/bash
# ==============================================================================
# Script di Demo per Tolleranza ai Guasti
# ==============================================================================
# Esegue una dimostrazione automatizzata del protocollo Gossip:
#   1. Mostra lo stato convergente di tutti i nodi
#   2. Simula un Hard Crash (docker kill) su un nodo casuale
#   3. Mostra la reazione del cluster (nodo rimosso dall'aggregazione)
#   4. Riavvia il nodo crashato
#   5. Mostra la riconvergenza del cluster
#
# Utilizzo:
#   bash scripts/demo_crash.sh [NUMERO_NODI] [HOST]
# ==============================================================================

set -e

NUM_NODES=${1:-8}
HOST=${2:-localhost}
BASE_PORT=8001

# Colori per il terminale
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

separator() {
    echo ""
    echo -e "${CYAN}----------------------------------------------------------------${NC}"
    echo ""
}

print_header() {
    echo -e "${BOLD}${CYAN}$1${NC}"
}

# ==============================================================================
# Funzione: interroga un singolo nodo e stampa il risultato in modo leggibile
# ==============================================================================
query_node() {
    local node_num=$1
    local port=$((BASE_PORT + node_num - 1))
    local url="http://${HOST}:${port}/metrics"

    local response
    response=$(curl -s --max-time 2 "$url" 2>/dev/null) || {
        echo -e "  ${RED}[!] Node-${node_num} - NON RAGGIUNGIBILE${NC}"
        return
    }

    local node_id=$(echo "$response" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4)
    local known=$(echo "$response" | grep -o '"known_nodes":[0-9]*' | cut -d: -f2)
    local round=$(echo "$response" | grep -o '"round":[0-9]*' | cut -d: -f2)
    local sum=$(echo "$response" | grep -o '"sum":[0-9.]*' | head -1 | cut -d: -f2)
    local avg=$(echo "$response" | grep -o '"average":[0-9.]*' | cut -d: -f2)
    local min=$(echo "$response" | grep -o '"min":[0-9.]*' | cut -d: -f2)
    local max=$(echo "$response" | grep -o '"max":[0-9.]*' | cut -d: -f2)
    local top_k=$(echo "$response" | grep -o '"top_k":\[[^]]*\]' | cut -d: -f2-)

    echo -e "  ${GREEN}[OK] ${node_id}${NC} | known: ${known} | round: ${round} | sum: ${sum}  avg: ${avg}  min: ${min}  max: ${max}  topk: ${top_k}"
}

# ==============================================================================
# Funzione: interroga tutti i nodi in sequenza
# ==============================================================================
query_all_nodes() {
    for i in $(seq 1 $NUM_NODES); do
        query_node $i
    done
}

# ==============================================================================
# Funzione: conto alla rovescia con messaggio
# ==============================================================================
countdown() {
    local seconds=$1
    local message=$2
    for i in $(seq $seconds -1 1); do
        echo -ne "\r  ${YELLOW}[WAIT] ${message} (${i}s)...${NC}  "
        sleep 1
    done
    echo -ne "\r                                                            \r"
}

# ==============================================================================
# INIZIO DEMO
# ==============================================================================
clear
echo ""
echo -e "${BOLD}${CYAN}================================================================${NC}"
echo -e "${BOLD}${CYAN}      DEMO LIVE - Gossip-Based Distributed Aggregation          ${NC}"
echo -e "${BOLD}${CYAN}      Protocollo Gossip con CRDT e Failure Detection            ${NC}"
echo -e "${BOLD}${CYAN}================================================================${NC}"
echo ""
echo -e "  Nodi: ${BOLD}${NUM_NODES}${NC} | Host: ${BOLD}${HOST}${NC} | Porte: ${BOLD}${BASE_PORT}-$((BASE_PORT + NUM_NODES - 1))${NC}"

# --- FASE 1: Stato iniziale convergente ---
separator
print_header "FASE 1 - Stato del Cluster (Convergenza Iniziale)"
echo ""
countdown 5 "Attesa convergenza iniziale"
query_all_nodes

# --- FASE 2: Selezione e crash di un nodo casuale ---
separator
VICTIM=$((RANDOM % NUM_NODES + 1))
VICTIM_CONTAINER="gossip-node${VICTIM}"

print_header "FASE 2 - Hard Crash del Nodo (Simulazione Guasto Improvviso)"
echo ""
echo -e "  ${RED}[!] Nodo selezionato per il crash: ${BOLD}${VICTIM_CONTAINER}${NC}"
echo -e "  ${RED}    Invio segnale SIGKILL (docker kill) - il nodo NON puo inviare il Leave${NC}"
echo ""

countdown 3 "Crash imminente"

docker kill "$VICTIM_CONTAINER" > /dev/null 2>&1 || {
    echo -e "  ${RED}Errore: impossibile killare ${VICTIM_CONTAINER}. Il container esiste?${NC}"
    exit 1
}

echo -e "  ${RED}[!] ${VICTIM_CONTAINER} e' stato terminato forzatamente!${NC}"

# --- FASE 3: Il cluster reagisce al crash ---
separator
print_header "FASE 3 - Reazione del Cluster (Failure Detection SWIM)"
echo ""
echo -e "  Il protocollo SWIM rilevera' l'assenza del nodo tramite timeout."
echo -e "  I nodi vivi ricalcoleranno l'aggregazione escludendo il nodo morto."
echo ""
countdown 25 "Attesa timeout SWIM e ricalcolo (la propagazione richiede tempo)"
query_all_nodes

# --- FASE 4: Riavvio del nodo crashato ---
separator
print_header "FASE 4 - Rejoin del Nodo Crashato"
echo ""
echo -e "  ${GREEN}[*] Riavvio di ${BOLD}${VICTIM_CONTAINER}${NC}${GREEN} con docker start...${NC}"

docker start "$VICTIM_CONTAINER" > /dev/null 2>&1 || {
    echo -e "  ${RED}Errore: impossibile riavviare ${VICTIM_CONTAINER}.${NC}"
    exit 1
}

echo -e "  ${GREEN}[OK] ${VICTIM_CONTAINER} riavviato! Nuovo Incarnation Number generato.${NC}"
echo ""
echo -e "  Il nodo si presentera' con un Incarnation superiore a quello in cache"
echo -e "  negli altri nodi, forzando il protocollo a riammetterlo come Alive."
echo ""
countdown 10 "Attesa riconvergenza"

# --- FASE 5: Cluster riconvertito ---
separator
print_header "FASE 5 - Riconvergenza Completa"
echo ""
query_all_nodes

separator
echo -e "${BOLD}${GREEN}[OK] Demo Crash completata!${NC}"
echo ""
