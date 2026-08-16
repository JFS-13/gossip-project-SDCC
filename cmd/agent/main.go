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
	// ---- Flag di avvio ----
	configPath := flag.String("config", "", "percorso file di configurazione YAML")
	flag.Parse()

	// ---- Caricamento configurazione ----
	cfg, err := setup.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("errore configurazione: %v", err)
	}

	// ---- Logger strutturato (delegato al package telemetry) ----
	telemetry.SetupLogger(cfg.LogLevel)

	slog.Info("avvio nodo gossip",
		"node_id", cfg.NodeID,
		"aggregation", cfg.AggregationType,
		"initial_value", cfg.InitialValue,
		"fanout", cfg.Fanout,
		"peers", cfg.SeedPeers,
	)

	// ---- Membership ----
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

	// I seed peers verranno usati dall'engine come fallback di bootstrap:
	// quando la membership è vuota, l'engine invia direttamente a questi indirizzi.
	// Quando i peer rispondono, la membership si auto-popola coi veri NodeID.

	// ---- Aggregatore ----
	var agg aggregation.Aggregator
	if cfg.AggregationType == "topk" {
		agg = aggregation.NewTopK(cfg.TopKSize)
	} else {
		agg, err = aggregation.Factory(cfg.AggregationType)
		if err != nil {
			log.Fatalf("aggregazione non supportata: %v", err)
		}
	}

	// ---- Stato CRDT iniziale ----
	engineState := core.NewEngineState(
		message.NodeID(cfg.NodeID),
		cfg.AggregationType,
		cfg.InitialValue,
	)
	// Per Top-K, imposta il contributo usando il valore iniziale come unico elemento
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

	// ---- Transport UDP ----
	listenAddr := fmt.Sprintf("%s:%d", cfg.BindAddress, cfg.NodePort)
	udpTransport, err := transport.NewUDPTransport(listenAddr)
	if err != nil {
		log.Fatalf("errore avvio transport UDP su %s: %v", listenAddr, err)
	}

	slog.Info("transport UDP avviato", "listen_address", listenAddr, "advertise_address", advertiseAddr)

	// ---- Engine gossip ----
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

	// ---- Context con segnale di shutdown ----
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ---- Avvio engine ----
	if err := eng.Start(ctx); err != nil {
		log.Fatalf("errore avvio engine gossip: %v", err)
	}
	slog.Info("engine gossip avviato", "node_id", cfg.NodeID, "interval_ms", cfg.GossipIntervalMs)

	// ---- Avvio timeout checker per failure detection ----
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

	// ---- Stampa stima periodica ----
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
				// Per Top-K stampa anche la lista completa
				if cfg.AggregationType == "topk" {
					topKAgg := agg.(*aggregation.TopKAggregator)
					snap := eng.State.Snapshot()
					topK := topKAgg.ComputeTopK(&snap, eng.GetAliveNodeIDs())
					slog.Info("top-k elementi", "node_id", cfg.NodeID, "top_k", topK)
				}
			}
		}
	}()

	// ---- Server HTTP per health/metrics (delegato al package telemetry) ----
	metricsAddr := fmt.Sprintf(":%d", cfg.NodePort+1000)
	telemetry := telemetry.NewTelemetryServer(metricsAddr, cfg.NodeID, cfg.AggregationType, eng)
	telemetry.Start()

	slog.Info("in attesa di segnali di shutdown", "node_id", cfg.NodeID)
	<-ctx.Done()

	slog.Info("shutdown in corso", "node_id", cfg.NodeID)

	// Graceful Leave: annuncia ai peer che questo nodo sta uscendo volontariamente
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 2*time.Second)
	eng.AnnounceLeave(leaveCtx)
	leaveCancel()

	// Shutdown del server telemetria
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = telemetry.Shutdown(shutdownCtx)
	shutdownCancel()

	// Stampa stima finale
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
