#!/bin/sh
# ==============================================================================
# entrypoint.sh — Applica il ritardo di rete (se configurato) e avvia il nodo
# ==============================================================================
# Se la variabile d'ambiente NETWORK_DELAY_MS è impostata e maggiore di 0,
# utilizza il Traffic Controller (tc) del kernel Linux per iniettare latenza
# artificiale su tutti i pacchetti in uscita, simulando una rete geografica.
# ==============================================================================

DELAY=${NETWORK_DELAY_MS:-0}
echo "STARTING NODE... NETWORK_DELAY_MS is set to: '$DELAY'"

if [ "$DELAY" -gt 0 ] 2>/dev/null; then
    JITTER=$((DELAY / 5))
    echo "Traffic Control: iniettando ${DELAY}ms di latenza (jitter: ${JITTER}ms) su eth0"
    tc qdisc add dev eth0 root netem delay ${DELAY}ms ${JITTER}ms distribution normal
else
    echo "Nessun ritardo configurato (DELAY=$DELAY)."
fi

# Avvia il binario del nodo gossip passando tutti gli argomenti ricevuti
exec /app/gossip-agent "$@"
