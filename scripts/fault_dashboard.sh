#!/bin/bash
# ========================================================
# GOSSIP PROTOCOL - FAULT INJECTION DASHBOARD
# ========================================================
# NOTA: Le opzioni di Network Partition (disconnect/connect) sono state rimosse 
# in quanto la rimozione dell'interfaccia di rete in Docker manda in stato "sordo" 
# i socket UDP (stateless) in ascolto universale su 0.0.0.0.
# Per simulare correttamente fault in questo progetto UDP, usa le opzioni CRASH e START,
# che forzano il demone a ricreare i socket in modo pulito.

# Colori per un output più leggibile
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

while true; do
    clear
    echo -e "${CYAN}=========================================${NC}"
    echo -e "${YELLOW}       FAULT INJECTION DASHBOARD         ${NC}"
    echo -e "${CYAN}=========================================${NC}"
    echo "1. [INFO]  Mostra lo stato di tutti i Nodi"
    echo "2. [CRASH] Spegni un Nodo (Simula Crash Hardware)"
    echo "3. [START] Riaccendi un Nodo (Simula Recovery)"
    echo "0. [EXIT]  Esci dalla Dashboard"
    echo "-----------------------------------------"

    read -p "Seleziona uno scenario (0-3): " choice

    case $choice in
        1)
            echo -e "\n${CYAN}--- STATO DEI CONTAINER ---${NC}"
            docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep "node" || echo "Nessun nodo trovato."
            ;;
        2)
            echo ""
            read -p "Inserisci il nome del nodo da spegnere (es. gossip-node3): " node
            echo -e "${YELLOW}Iniezione guasto in corso...${NC}"
            docker stop $node > /dev/null
            echo -e "${RED}[CRASH] Crash Hardware simulato: Nodo $node arrestato.${NC}"
            ;;
        3)
            echo ""
            read -p "Inserisci il nome del nodo da riaccendere (es. gossip-node3): " node
            echo -e "${YELLOW}Ripristino in corso...${NC}"
            docker start $node > /dev/null
            echo -e "${GREEN}[START] Recovery simulato: Nodo $node avviato e in fase di riallineamento.${NC}"
            ;;
        0)
            echo -e "${CYAN}Uscita dalla Dashboard di Fault Injection...${NC}"
            break
            ;;
        *)
            echo -e "${RED}Scelta non valida!${NC}"
            ;;
    esac

    echo ""
    read -p "Premi INVIO per tornare al menu principale..." dummy
done
