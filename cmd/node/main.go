package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"gossip-project/internal/aggregation"
	"gossip-project/internal/config"
	"gossip-project/internal/gossip"
	"gossip-project/internal/transport"
)

func main() {
	// Definiamo un flag da riga di comando per il file di configurazione
	configPath := flag.String("config", "configs/node1.yaml", "Percorso del file YAML di configurazione")
	flag.Parse()

	// Usiamo *configPath (il valore puntato dal flag)
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Errore critico durante il caricamento della configurazione: %v", err)
	}

	fmt.Printf("🚀 Avvio del %s...\n", cfg.NodeID)

	// 1. Inizializziamo il server di rete UDP
	udpTransport, err := transport.NewUDPTransport(cfg.ListenAddress)
	if err != nil {
		log.Fatalf("Errore durante l'avvio del server UDP: %v", err)
	}
	defer udpTransport.Close()

	// 2. Selezioniamo la logica di aggregazione in base al file YAML
	// Inizializziamo l'aggregatore corretto leggendo la configurazione
	var agg aggregation.Aggregator
	totalNodes := len(cfg.Peers) + 1 // Utile per la funzione SUM

	switch cfg.AggregationType {
	case "MAX":
		agg = aggregation.MaxAggregator{}
	case "MIN":
		agg = aggregation.MinAggregator{}
	case "SUM":
		agg = aggregation.SumAggregator{TotalNodes: totalNodes}
	case "AVERAGE":
		fallthrough
	default:
		agg = aggregation.AverageAggregator{}
	}

	// 3. Generiamo un valore iniziale fittizio (tra 0 e 100) per testare l'algoritmo
	initialValue := rand.Float64() * 100
	fmt.Printf("📊 Valore locale iniziale (generato casualmente): %.2f\n", initialValue)

	// 4. Istanziamo e avviamo il nostro demone di Gossip
	// Il Peso iniziale per tutti i nodi deve essere obbligatoriamente 1.0!
	initialWeight := 1.0

	agent := gossip.NewAgent(
		cfg.NodeID,
		initialValue,
		initialWeight, // <- QUESTO È IL NUOVO PARAMETRO AGGIUNTO
		cfg.Peers,
		udpTransport,
		agg,
		cfg.GossipIntervalMs,
	)

	// Diamogli il tempo di accendere la rete virtuale di Docker
	fmt.Println("⏳ Attesa di 3 secondi per la stabilizzazione della rete Docker...")
	time.Sleep(3 * time.Second)

	agent.Start()
	fmt.Printf("✅ Agent avviato con successo. In ascolto su %s\n", cfg.ListenAddress)

	agent.Start()
	fmt.Printf("✅ Agent avviato con successo. In ascolto su %s\n", cfg.ListenAddress)
	fmt.Println("==========================================================")

	// 5. Ciclo infinito di ricezione: ascoltiamo la rete e deleghiamo all'Agent
	for {
		msg, remoteAddr, err := udpTransport.ReceiveMessage()
		if err != nil {
			log.Printf("⚠️ Errore durante la ricezione del messaggio: %v\n", err)
			continue
		}

		// L'Agent si occuperà di aggiornare il suo stato e rispondere!
		agent.HandleMessage(msg, remoteAddr)
	}
}
