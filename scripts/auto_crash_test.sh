#!/bin/bash
# auto_crash_test.sh
# Questo script esegue il Chaos Engineering su un nodo Docker e ne mostra i log in tempo reale.

if [ -z "$1" ]; then
  echo "Uso: $0 <nome-container>"
  echo "Esempio: $0 gossip-node1"
  exit 1
fi

CONTAINER=$1

echo "=========================================================="
echo "🌩️  Avvio Auto Crash Test per il container: $CONTAINER"
echo "I log del container verranno mostrati qui sotto."
echo "Premi Ctrl+C per interrompere il test."
echo "=========================================================="

# Funzione per fermare i log in caso di uscita con Ctrl+C
cleanup() {
  echo -e "\n🛑 Interruzione del test. Fermo il tail dei log..."
  kill $LOG_PID 2>/dev/null
  exit 0
}
trap cleanup SIGINT SIGTERM

while true; do
  # Mostra i log in tempo reale in background (--tail 0 evita di stampare lo storico passato ogni volta)
  sudo docker logs -f --tail 0 $CONTAINER &
  LOG_PID=$!

  # Calcola un tempo di 'uptime' casuale tra 20 e 40 secondi
  UPTIME=$(( (RANDOM % 20) + 20 ))
  echo -e "\n[$(date +'%H:%M:%S')] 🟢 Il nodo opererà normalmente per ${UPTIME} secondi...\n"
  sleep $UPTIME

  # Simula un crash di rete/hardware brutale (SIGKILL)
  echo -e "\n[$(date +'%H:%M:%S')] 💥 CRASH SIMULATO! Spegnimento brutale del nodo $CONTAINER..."
  sudo docker kill $CONTAINER > /dev/null 2>&1
  
  # Ferma il comando 'docker logs' in background poiché il container è morto
  kill $LOG_PID 2>/dev/null

  # Calcola un tempo di 'downtime' casuale tra 15 e 30 secondi 
  DOWNTIME=$(( (RANDOM % 15) + 15 ))
  echo "[$(date +'%H:%M:%S')] 🔴 Il nodo resterà offline (morto) per ${DOWNTIME} secondi..."
  sleep $DOWNTIME

  # Risuscita il nodo
  echo "[$(date +'%H:%M:%S')] ⚡ RESTART! Il nodo $CONTAINER viene riavviato..."
  sudo docker start $CONTAINER > /dev/null 2>&1
  echo -e "[$(date +'%H:%M:%S')] 🟢 Nodo tornato online! Riprendo la lettura dei log...\n"
  echo "----------------------------------------------------------"
done
