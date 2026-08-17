package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"gossip-project/internal/message"
	"gossip-project/internal/transport"
)

// Transport definisce l'interfaccia per l'invio e la ricezione di messaggi gossip.
// Usa transport.MessageHandler per garantire compatibilità con UDPTransport e NoopTransport.
type Transport interface {
	Send(ctx context.Context, address string, payload []byte) error
	Start(ctx context.Context, handler transport.MessageHandler) error
	Close() error
}

// Aggregator definisce l'interfaccia per calcolare i risultati dell'aggregazione.
// Sarà implementata dal package internal/aggregation.
type Aggregator interface {
	Type() string
	ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64
	SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64)
}

// MembershipProvider fornisce informazioni sui peer.
// Sarà implementata dal package internal/membership come Manager.
type MembershipProvider interface {
	GetAlivePeers() []message.MembershipEntry
	GetRandomPeers(n int) []message.MembershipEntry
	GetClusterSize() int
	MergeMembership(entries []message.MembershipEntry)
	SnapshotEntries() []message.MembershipEntry
	IncrementIncarnation()
}

// Engine rappresenta il motore gossip che esegue round periodici.
type Engine struct {
	State      *EngineState
	nodeID     message.NodeID
	myAddress  string
	seedPeers  []string // Indirizzi seed per il bootstrap (usati quando la membership è vuota)
	transport  Transport
	aggregator Aggregator
	membership MembershipProvider
	interval   time.Duration
	fanout     int
	logger     *log.Logger
	stopCh     chan struct{}
	mu         sync.RWMutex
}

// NewEngine crea un nuovo motore gossip.
func NewEngine(
	nodeID message.NodeID,
	myAddress string,
	seedPeers []string,
	transport Transport,
	aggregator Aggregator,
	membership MembershipProvider,
	interval time.Duration,
	fanout int,
) *Engine {
	return &Engine{
		nodeID:     nodeID,
		myAddress:  myAddress,
		seedPeers:  seedPeers,
		transport:  transport,
		aggregator: aggregator,
		membership: membership,
		interval:   interval,
		fanout:     fanout,
		logger:     log.Default(),
		stopCh:     make(chan struct{}),
	}
}

// Start avvia il ciclo gossip in una goroutine.
func (e *Engine) Start(ctx context.Context) error {
	err := e.transport.Start(ctx, transport.MessageHandler(e.handleMessage))
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-e.stopCh:
				return
			case <-ticker.C:
				e.executeRound(ctx)
			}
		}
	}()

	return nil
}

// Stop ferma il motore gossip.
func (e *Engine) Stop() error {
	close(e.stopCh)
	return e.transport.Close()
}

// executeRound esegue un round di gossip.
func (e *Engine) executeRound(ctx context.Context) {
	// 1. Incrementa il round
	e.State.IncrementRound()

	// 2. Ottieni peer casuali dalla membership (fino al count fanout)
	peers := e.membership.GetRandomPeers(e.fanout)

	// 3. Prendi uno snapshot dello stato corrente
	aggState := e.State.Snapshot()

	// 4. Incrementa la propria incarnazione (heartbeat) e genera le entry di membership
	e.membership.IncrementIncarnation()
	membershipEntries := e.membership.SnapshotEntries()

	msgID := message.MessageID(fmt.Sprintf("msg-%s-%d", e.nodeID, time.Now().UnixNano()))
	msg := message.GossipMessage{
		MessageID:  msgID,
		SenderID:   e.nodeID,
		SentAt:     time.Now(),
		Round:      e.State.GetRound(),
		State:      aggState,
		Membership: membershipEntries,
	}

	// 5. Serializza in JSON
	payload, err := json.Marshal(msg)
	if err != nil {
		e.logger.Printf("Errore nella serializzazione del messaggio: %v", err)
		return
	}

	// 6. Invia ai peer noti dalla membership
	if len(peers) > 0 {
		for _, peer := range peers {
			err := e.transport.Send(ctx, peer.Addr, payload)
			if err != nil {
				e.logger.Printf("Errore nell'invio al peer %s: %v", peer.Addr, err)
			}
		}
	}

	// 7. Partition Healing & Bootstrap
	// Invia periodicamente ai seed peers anche se abbiamo già dei peer (20% dei round)
	// Questo previene scenari di "Split-Brain" dove sottogruppi isolati non si uniscono mai
	// al cluster principale se si avviano in ordine sparso.
	if len(peers) == 0 || rand.Float32() < 0.20 {
		for _, seedAddr := range e.seedPeers {
			if seedAddr == e.myAddress {
				continue // Non inviare a sé stesso
			}
			err := e.transport.Send(ctx, seedAddr, payload)
			if err != nil {
				e.logger.Printf("Bootstrap/Healing: errore invio al seed %s: %v", seedAddr, err)
			}
		}
	}

	// 7. Ricalcola la stima dopo l'invio
	aliveNodes := make(map[message.NodeID]bool)
	for _, entry := range membershipEntries {
		if entry.Status == "alive" || entry.Status == "suspect" {
			aliveNodes[entry.NodeID] = true
		}
	}
	estimate := e.aggregator.ComputeResult(&aggState, aliveNodes)
	e.State.SetEstimate(estimate)
}

// handleMessage è la callback per il transport: deserializza GossipMessage,
// chiama MergeCRDT sullo stato, unisce la membership e ricalcola la stima.
func (e *Engine) handleMessage(ctx context.Context, payload []byte) error {
	// 1. Deserializza JSON in GossipMessage
	var msg message.GossipMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		e.logger.Printf("Errore nella deserializzazione del payload: %v", err)
		return err
	}

	// 2. Unisci lo stato remoto in quello locale tramite MergeCRDT
	changed := e.State.MergeRemote(&msg.State)

	// 3. Unisci le voci di membership remote
	if len(msg.Membership) > 0 {
		e.membership.MergeMembership(msg.Membership)
	}

	// 4. Ricalcola la stima
	aggState := e.State.Snapshot()
	membershipEntries := e.membership.SnapshotEntries()
	aliveNodes := make(map[message.NodeID]bool)
	for _, entry := range membershipEntries {
		if entry.Status == "alive" || entry.Status == "suspect" {
			aliveNodes[entry.NodeID] = true
		}
	}
	estimate := e.aggregator.ComputeResult(&aggState, aliveNodes)
	e.State.SetEstimate(estimate)

	// 5. Logga il risultato del merge
	if changed {
		e.logger.Printf("Round %d: Unito stato dal peer %s. Nuova stima: %f", msg.Round, msg.SenderID, estimate)
	}

	return nil
}

// GetEstimate restituisce (stima, knownNodes) in modo thread-safe.
func (e *Engine) GetEstimate() (float64, int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	estimate := e.State.GetEstimate()
	knownNodes := e.membership.GetClusterSize()
	return estimate, knownNodes
}

// GetAliveNodeIDs restituisce la mappa dei nodi attualmente vivi/sospetti.
func (e *Engine) GetAliveNodeIDs() map[message.NodeID]bool {
	entries := e.membership.SnapshotEntries()
	alive := make(map[message.NodeID]bool)
	for _, entry := range entries {
		if entry.Status == "alive" || entry.Status == "suspect" {
			alive[entry.NodeID] = true
		}
	}
	return alive
}

// AnnounceLeave invia un messaggio di leave a tutti i peer noti.
// Chiamata durante un graceful shutdown per notificare la rete
// che il nodo sta uscendo volontariamente (non è un crash).
func (e *Engine) AnnounceLeave(ctx context.Context) {
	peers := e.membership.GetAlivePeers()

	// Crea un messaggio con la membership entry di sé stesso con status "leave"
	leaveEntry := message.MembershipEntry{
		NodeID: e.nodeID,
		Addr:   e.myAddress,
		Status: message.StatusLeave,
	}

	aggState := e.State.Snapshot()
	msgID := message.MessageID(fmt.Sprintf("leave-%s-%d", e.nodeID, time.Now().UnixNano()))
	msg := message.GossipMessage{
		MessageID:  msgID,
		SenderID:   e.nodeID,
		SentAt:     time.Now(),
		Round:      e.State.GetRound(),
		State:      aggState,
		Membership: []message.MembershipEntry{leaveEntry},
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		e.logger.Printf("Errore serializzazione leave: %v", err)
		return
	}

	// Invia a tutti i peer noti (non solo fanout, vogliamo massima diffusione)
	for _, peer := range peers {
		if peer.NodeID == e.nodeID {
			continue
		}
		_ = e.transport.Send(ctx, peer.Addr, payload)
	}

	e.logger.Printf("Leave announcement inviato a %d peer", len(peers)-1)
}

// GetRound restituisce il round corrente (delegato a EngineState).
func (e *Engine) GetRound() uint64 {
	return e.State.GetRound()
}

// GetEpoch restituisce l'epoca di avvio del nodo.
func (e *Engine) GetEpoch() int64 {
	return e.State.MyEpoch
}
