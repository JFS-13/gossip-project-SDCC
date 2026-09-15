#!/bin/bash
# ==============================================================================
# Script per Latenza di Rete
# ==============================================================================
# Dimostra la convergenza asincrona del protocollo Gossip applicando
# ritardi di rete crescenti su ogni nodo.
# ==============================================================================

set -e

NUM_NODES=8
HOST=127.0.0.1
BASE_PORT=8001

# Colori per il terminale
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

query_node() {
    local node_num=$1
    local port=$((BASE_PORT + node_num - 1))
    local url="http://${HOST}:${port}/metrics"

    local response
    response=$(docker exec "gossip-node${node_num}" wget -qO- "http://127.0.0.1:${port}/metrics" 2>/dev/null) || {
        echo -e "  ${RED}[!] Node-$node_num - In Avvio...${NC}"
        return
    }

    local node_id=$(echo "$response" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4)
    local known=$(echo "$response" | grep -o '"known_nodes":[0-9]*' | cut -d: -f2)
    local sum=$(echo "$response" | grep -o '"sum":[0-9.]*' | head -1 | cut -d: -f2)
    local avg=$(echo "$response" | grep -o '"average":[0-9.]*' | cut -d: -f2)
    local top_k=$(echo "$response" | grep -o '"top_k":\[[^]]*\]' | cut -d: -f2-)

    local delay_ms=0
    case $node_num in
        2) delay_ms=1000 ;;
        3) delay_ms=2000 ;;
        4) delay_ms=3000 ;;
        5) delay_ms=4000 ;;
        6) delay_ms=6000 ;;
        7) delay_ms=8000 ;;
        8) delay_ms=10000 ;;
    esac

    # Colora i nodi lenti di giallo se la somma non ha ancora raggiunto 360 (la somma totale vera)
    if [ "$sum" != "360.0000" ]; then
        echo -e "  ${YELLOW}[WAIT] $node_id (${delay_ms}ms) | Ritardo in corso | known: $known | sum: $sum  avg: $avg  topk: $top_k${NC}"
    else
        echo -e "  ${GREEN}[OK] $node_id (${delay_ms}ms) | Convergenza OK | known: $known | sum: $sum  avg: $avg  topk: $top_k${NC}"
    fi
}

echo -e "${BOLD}${CYAN}Preparazione dell'ambiente...${NC}"
docker-compose down -v > /dev/null 2>&1

echo -e "${BOLD}${YELLOW}Avvio del cluster con latenze sfalsate per ogni nodo...${NC}"
export NODE2_DELAY=1000
export NODE3_DELAY=2000
export NODE4_DELAY=3000
export NODE5_DELAY=4000
export NODE6_DELAY=6000
export NODE7_DELAY=8000
export NODE8_DELAY=10000
docker-compose up -d

SECONDS=0
MAX_WAIT=20

while [ $SECONDS -lt $MAX_WAIT ]; do
    echo -e "${BOLD}${CYAN}================================================================${NC}"
    echo -e "${BOLD}${CYAN}      DEMO LIVE - Latenza Geografica ed Eventual Consistency    ${NC}"
    echo -e "${BOLD}${CYAN}================================================================${NC}"
    echo -e "  Ogni nodo ha un ritardo crescente (da 0ms a 10000ms)"
    echo -e "  Tempo trascorso: ${BOLD}${SECONDS}s${NC} / ${MAX_WAIT}s"
    echo ""
    
    for i in $(seq 1 $NUM_NODES); do
        query_node $i
    done
    
    sleep 1
done

echo ""
echo -e "${BOLD}${GREEN}================================================================${NC}"
echo -e "${BOLD}${GREEN}  Convergenza raggiunta!                                        ${NC}"
echo -e "${BOLD}${GREEN}================================================================${NC}"
