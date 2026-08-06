package main

import (
	"fmt"
	"log"

	"gossip-project/internal/config"
	"gossip-project/internal/transport"
)

func main() {
	configPath := "configs/node1.yaml"

	// 1. Carichiamo la configurazione
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Errore critico durante il caricamento della configurazione: %v", err)
	}

	fmt.Printf("Avvio del nodo %s...\n", cfg.NodeID)

	// 2. Inizializziamo il trasporto UDP usando l'indirizzo dal file YAML
	udpTransport, err := transport.NewUDPTransport(cfg.ListenAddress)
	if err != nil {
		log.Fatalf("Errore durante l'avvio del server UDP: %v", err)
	}
	// Assicuriamoci che la porta venga chiusa correttamente quando il programma termina
	defer udpTransport.Close()

	fmt.Printf("✅ Nodo in ascolto con successo su %s\n", cfg.ListenAddress)
	fmt.Println("In attesa di messaggi di Gossip (Premi CTRL+C per fermare)...")

	// 3. Ciclo infinito per ascoltare i messaggi in arrivo (Temporaneo per test)
	for {
		msg, remoteAddr, err := udpTransport.ReceiveMessage()
		if err != nil {
			log.Printf("Errore durante la ricezione del messaggio: %v\n", err)
			continue
		}

		// Stampiamo a schermo cosa abbiamo ricevuto e da chi
		fmt.Printf("📩 Ricevuto messaggio da %s: Tipo=%v, Valore=%f, Aggregazione=%s\n",
			remoteAddr, msg.Type, msg.Value, msg.Aggregation)
	}
}
