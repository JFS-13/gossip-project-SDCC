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
type Transport interface {
	Send(ctx context.Context, address string, payload []byte) error
	Start(ctx context.Context, handler transport.MessageHandler) error
	Close() error
}

// Aggregator definisce l'interfaccia per il calcolo dei risultati dell'aggregazione.
type Aggregator interface {
	Type() string
	ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64
	SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64)
}

// MembershipProvider fornisce le informazioni relative ai peer noti al sistema.
type MembershipProvider interface {
	GetAlivePeers() []message.MembershipEntry
	GetRandomPeers(n int) []message.MembershipEntry
	GetClusterSize() int
	MergeMembership(entries []message.MembershipEntry)
	SnapshotEntries() []message.MembershipEntry
	IncrementIncarnation()
}

// Engine rappresenta il motore gossip che esegue i round periodici di aggiornamento.
type Engine struct {
	State      *EngineState
	nodeID     message.NodeID
	myAddress  string
	seedPeers  []string
	transport  Transport
	aggregator Aggregator
	membership MembershipProvider
	interval   time.Duration
	fanout     int
	logger     *log.Logger
	stopCh     chan struct{}
	mu         sync.RWMutex
}

// NewEngine inizializza un nuovo motore gossip.
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

// Start avvia il ciclo di esecuzione del protocollo gossip.
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

// Stop termina l'esecuzione del motore gossip e chiude il transport.
func (e *Engine) Stop() error {
	close(e.stopCh)
	return e.transport.Close()
}

// executeRound avvia un singolo ciclo di gossip, incrementando lo stato interno,
// inviando messaggi ai peer e ricalcolando la stima di aggregazione.
func (e *Engine) executeRound(ctx context.Context) {
	e.State.IncrementRound()

	// Selezione casuale dei target per il gossip (Fanout)
	peers := e.membership.GetRandomPeers(e.fanout)

	aggState := e.State.Snapshot()

	// Aggiornamento Heartbeat/Incarnation locale per testimoniare l'attività
	e.membership.IncrementIncarnation()
	membershipEntries := e.membership.SnapshotEntries()

	// Preparazione del payload con Piggybacking (Stato Aggregazione + Topologia)
	msgID := message.MessageID(fmt.Sprintf("msg-%s-%d", e.nodeID, time.Now().UnixNano()))
	msg := message.GossipMessage{
		MessageID:  msgID,
		SenderID:   e.nodeID,
		SentAt:     time.Now(),
		Round:      e.State.GetRound(),
		State:      aggState,
		Membership: membershipEntries,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		e.logger.Printf("Errore nella serializzazione del messaggio: %v", err)
		return
	}

	// Disseminazione verso i peer selezionati
	if len(peers) > 0 {
		for _, peer := range peers {
			err := e.transport.Send(ctx, peer.Addr, payload)
			if err != nil {
				e.logger.Printf("Errore nell'invio al peer %s: %v", peer.Addr, err)
			}
		}
	}

	// Risoluzione Partizioni di Rete
	// Per evitare che il sistema si frammenti in sottoreti isolate che si ignorano a vicenda,
	// il nodo tenta periodicamente di contattare i nodi "seed" iniziali.
	// Se il nodo è completamente isolato, contatta i seed al 100%.
	// Se invece ha già dei peer, li contatta solo con il 20% di probabilità per ridurre
	// l'overhead, garantendo comunque che eventuali partizioni separate prima o poi si ricongiungano.
	if len(peers) == 0 || rand.Float32() < 0.20 {
		for _, seedAddr := range e.seedPeers {
			if seedAddr == e.myAddress {
				continue
			}
			err := e.transport.Send(ctx, seedAddr, payload)
			if err != nil {
				e.logger.Printf("Errore invio al seed %s: %v", seedAddr, err)
			}
		}
	}

	// Ricalcolo dell'aggregato filtrando i nodi Dead/Leave
	aliveNodes := make(map[message.NodeID]bool)
	for _, entry := range membershipEntries {
		if entry.Status == "alive" || entry.Status == "suspect" {
			aliveNodes[entry.NodeID] = true
		}
	}
	estimate := e.aggregator.ComputeResult(&aggState, aliveNodes)
	e.State.SetEstimate(estimate)
}

// handleMessage gestisce la ricezione dei messaggi dal transport elaborando
// lo stato CRDT, aggiornando la membership e ricalcolando la stima.
func (e *Engine) handleMessage(ctx context.Context, payload []byte) error {
	var msg message.GossipMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		e.logger.Printf("Errore nella deserializzazione del payload: %v", err)
		return err
	}

	// Merge dello stato applicativo tramite logica CRDT State-Based
	changed := e.State.MergeRemote(&msg.State)

	// Estrazione e propagazione delle informazioni sulla topologia
	if len(msg.Membership) > 0 {
		e.membership.MergeMembership(msg.Membership)
	}

	// Ricalcolo della stima ignorando i nodi che non sono Alive o Suspect
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

	if changed {
		e.logger.Printf("Round %d: Unito stato dal peer %s. Nuova stima: %f", msg.Round, msg.SenderID, estimate)
	}

	return nil
}

// GetEstimate restituisce la stima corrente e il numero di nodi noti in modo concorrente.
func (e *Engine) GetEstimate() (float64, int) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	estimate := e.State.GetEstimate()
	knownNodes := e.membership.GetClusterSize()
	return estimate, knownNodes
}

// GetAliveNodeIDs restituisce la lista dei nodi correntemente in stato alive o suspect.
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

// AnnounceLeave notifica i peer in merito all'uscita volontaria del nodo corrente.
func (e *Engine) AnnounceLeave(ctx context.Context) {
	peers := e.membership.GetAlivePeers()

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

	for _, peer := range peers {
		if peer.NodeID == e.nodeID {
			continue
		}
		_ = e.transport.Send(ctx, peer.Addr, payload)
	}

	e.logger.Printf("Leave announcement inviato a %d peer", len(peers)-1)
}

// GetRound restituisce il round corrente dal motore di stato.
func (e *Engine) GetRound() uint64 {
	return e.State.GetRound()
}

// GetEpoch restituisce l'epoca corrente di avvio del nodo.
func (e *Engine) GetEpoch() int64 {
	return e.State.MyEpoch
}
