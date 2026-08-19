package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gossip-project/internal/aggregation"
	"gossip-project/internal/core"
	"gossip-project/internal/message"
	"gossip-project/internal/setup"
	"gossip-project/internal/telemetry"
	"gossip-project/internal/topology"
	"gossip-project/internal/transport"
)

func main() {
	// Lettura del percorso del file di configurazione
	configPath := flag.String("config", "", "percorso file di configurazione YAML")
	flag.Parse()

	// Caricamento della configurazione YAML
	cfg, err := setup.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("errore configurazione: %v", err)
	}

	// Setup del logger centralizzato
	telemetry.SetupLogger(cfg.LogLevel)

	slog.Info("avvio nodo gossip",
		"node_id", cfg.NodeID,
		"aggregation", cfg.AggregationType,
		"initial_value", cfg.InitialValue,
		"fanout", cfg.Fanout,
		"peers", cfg.SeedPeers,
	)

	// Inizializzazione del Topology Manager per la failure detection
	advertiseAddr := cfg.AdvertiseEndpoint()
	membershipCfg := topology.Config{
		SuspectTimeout: time.Duration(cfg.MembershipTimeoutMs/2) * time.Millisecond,
		DeadTimeout:    time.Duration(cfg.MembershipTimeoutMs) * time.Millisecond,
		CleanupTimeout: time.Duration(cfg.CleanupTimeoutMs) * time.Millisecond,
	}
	mset := topology.NewManager(
		message.NodeID(cfg.NodeID),
		advertiseAddr,
		membershipCfg,
		cfg.SeedPeers,
	)

	// Inizializzazione dell'aggregatore specifico scelto in configurazione
	var agg aggregation.Aggregator
	if cfg.AggregationType == "topk" {
		agg = aggregation.NewTopK(cfg.TopKSize)
	} else {
		agg, err = aggregation.Factory(cfg.AggregationType)
		if err != nil {
			log.Fatalf("aggregazione non supportata: %v", err)
		}
	}

	// Configurazione dello stato CRDT iniziale del nodo
	engineState := core.NewEngineState(
		message.NodeID(cfg.NodeID),
		cfg.AggregationType,
		cfg.InitialValue,
	)

	if cfg.AggregationType == "topk" {
		topKAgg := agg.(*aggregation.TopKAggregator)
		topKAgg.SetTopKContribution(
			&engineState.Aggregation,
			message.NodeID(cfg.NodeID),
			[]float64{cfg.InitialValue},
		)
	} else {
		agg.SetContribution(&engineState.Aggregation, message.NodeID(cfg.NodeID), cfg.InitialValue)
	}

	// Avvio del layer di Transport (UDP)
	listenAddr := fmt.Sprintf("%s:%d", cfg.BindAddress, cfg.NodePort)
	udpTransport, err := transport.NewUDPTransport(listenAddr)
	if err != nil {
		log.Fatalf("errore avvio transport UDP su %s: %v", listenAddr, err)
	}

	slog.Info("transport UDP avviato", "listen_address", listenAddr, "advertise_address", advertiseAddr)

	// Costruzione dell'Engine Gossip principale
	eng := core.NewEngine(
		message.NodeID(cfg.NodeID),
		advertiseAddr,
		cfg.SeedPeers,
		udpTransport,
		agg,
		mset,
		time.Duration(cfg.GossipIntervalMs)*time.Millisecond,
		cfg.Fanout,
	)
	eng.State = engineState

	// Gestione dei segnali per una chiusura gracefully
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Avvio asincrono dell'engine
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("errore avvio engine gossip: %v", err)
	}
	slog.Info("engine gossip avviato", "node_id", cfg.NodeID, "interval_ms", cfg.GossipIntervalMs)

	// Routine periodica per il controllo dei timeout di membership
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.MembershipTimeoutMs/2) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				mset.CheckTimeouts(now)
			}
		}
	}()

	// Routine di logging periodico della stima dell'aggregazione
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				estimate, knownNodes := eng.GetEstimate()
				slog.Info("stima corrente",
					"node_id", cfg.NodeID,
					"aggregation", cfg.AggregationType,
					"estimate", fmt.Sprintf("%.4f", estimate),
					"known_nodes", knownNodes,
					"round", eng.GetRound(),
				)
				if cfg.AggregationType == "topk" {
					topKAgg := agg.(*aggregation.TopKAggregator)
					snap := eng.State.Snapshot()
					topK := topKAgg.ComputeTopK(&snap, eng.GetAliveNodeIDs())
					slog.Info("top-k elementi", "node_id", cfg.NodeID, "top_k", topK)
				}
			}
		}
	}()

	// Avvio del server HTTP per le metriche
	metricsAddr := fmt.Sprintf(":%d", cfg.NodePort+1000)
	telemetry := telemetry.NewTelemetryServer(metricsAddr, cfg.NodeID, cfg.AggregationType, eng)
	telemetry.Start()

	slog.Info("in attesa di segnali di shutdown", "node_id", cfg.NodeID)
	<-ctx.Done()

	slog.Info("shutdown in corso", "node_id", cfg.NodeID)

	// Graceful shutdown: invia messaggio di leave ai peer
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 2*time.Second)
	eng.AnnounceLeave(leaveCtx)
	leaveCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = telemetry.Shutdown(shutdownCtx)
	shutdownCancel()

	estimate, knownNodes := eng.GetEstimate()
	slog.Info("shutdown nodo completato",
		"node_id", cfg.NodeID,
		"aggregation", cfg.AggregationType,
		"final_estimate", estimate,
		"known_nodes", knownNodes,
		"final_round", eng.GetRound(),
	)

	_ = eng.Stop()
}
