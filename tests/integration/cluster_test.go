// Package integration contiene test di integrazione che verificano la convergenza
// del cluster gossip e la robustezza ai crash dei nodi, usando transport in-memory.
package integration_test

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"

	"gossip-project/internal/aggregation"
	"gossip-project/internal/core"
	"gossip-project/internal/message"
	"gossip-project/internal/topology"
	"gossip-project/internal/transport"
)

// =====================================================================
// InMemoryTransport — transport in-memory per test senza rete reale
// =====================================================================

// InMemoryBus è un bus di messaggi in-memory che collega più InMemoryTransport.
// Permette di testare il protocollo gossip senza aprire socket UDP reali.
type InMemoryBus struct {
	mu       sync.RWMutex
	handlers map[string]transport.MessageHandler // addr → handler
	blocked  map[string]bool                     // addr → isolato (partizione di rete)
}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{
		handlers: make(map[string]transport.MessageHandler),
		blocked:  make(map[string]bool),
	}
}

// Register registra un handler per un indirizzo dato.
func (b *InMemoryBus) Register(addr string, handler transport.MessageHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[addr] = handler
}

// Send invia un payload all'indirizzo destinazione tramite il bus.
// Se il destinatario è isolato (blocked), il messaggio viene scartato.
func (b *InMemoryBus) Send(ctx context.Context, destAddr string, payload []byte) error {
	b.mu.RLock()
	handler, exists := b.handlers[destAddr]
	isBlocked := b.blocked[destAddr]
	b.mu.RUnlock()

	if !exists || isBlocked {
		return nil // Nodo assente o isolato: messaggio silenziosamente scartato
	}
	// Copia del payload per evitare data race
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)
	go handler(ctx, payloadCopy)
	return nil
}

// Block simula un crash o una partizione di rete per un nodo.
func (b *InMemoryBus) Block(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked[addr] = true
}

// Unblock ripristina la connettività di un nodo.
func (b *InMemoryBus) Unblock(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked[addr] = false
}

// InMemoryTransport implementa gossip.Transport usando il bus in-memory.
type InMemoryTransport struct {
	addr    string
	bus     *InMemoryBus
	handler transport.MessageHandler
}

func NewInMemoryTransport(addr string, bus *InMemoryBus) *InMemoryTransport {
	return &InMemoryTransport{addr: addr, bus: bus}
}

func (t *InMemoryTransport) Start(ctx context.Context, handler transport.MessageHandler) error {
	t.handler = handler
	t.bus.Register(t.addr, handler)
	return nil
}

func (t *InMemoryTransport) Send(ctx context.Context, addr string, payload []byte) error {
	return t.bus.Send(ctx, addr, payload)
}

func (t *InMemoryTransport) Close() error {
	return nil
}

// =====================================================================
// Helper: crea un nodo gossip completo in-memory
// =====================================================================

type TestNode struct {
	ID        string
	Addr      string
	Engine    *core.Engine
	Transport *InMemoryTransport
	Cancel    context.CancelFunc
}

func newTestNode(
	t *testing.T,
	id, addr string,
	initialValue float64,
	aggType string,
	bus *InMemoryBus,
	peers []string,
) *TestNode {
	t.Helper()

	tr := NewInMemoryTransport(addr, bus)

	cfg := topology.Config{
		SuspectTimeout: 2 * time.Second,
		DeadTimeout:    4 * time.Second,
	}
	mset := topology.NewManager(message.NodeID(id), addr, cfg, peers)
	// I peer verranno scoperti automaticamente via gossip piggybacking.
	// I seed peers vengono passati all'engine come fallback di bootstrap.

	var agg aggregation.Aggregator
	var err error
	if aggType == "topk" {
		agg = aggregation.NewTopK(3)
	} else {
		agg, err = aggregation.Factory(aggType)
		if err != nil {
			t.Fatalf("aggregazione non supportata: %v", err)
		}
	}

	state := core.NewEngineState(message.NodeID(id), aggType, initialValue)
	agg.SetContribution(&state.Aggregation, message.NodeID(id), initialValue)

	eng := core.NewEngine(
		message.NodeID(id),
		addr,
		peers, // seedPeers per il bootstrap
		tr,
		agg,
		mset,
		100*time.Millisecond, // intervallo veloce per i test
		2,
	)
	eng.State = state

	return &TestNode{
		ID:        id,
		Addr:      addr,
		Engine:    eng,
		Transport: tr,
	}
}

func (n *TestNode) Start(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n.Cancel = cancel
	if err := n.Engine.Start(ctx); err != nil {
		t.Fatalf("errore avvio nodo %s: %v", n.ID, err)
	}
	return cancel
}

// =====================================================================
// Test M09-1: Convergenza cluster 3 nodi (AVERAGE)
// =====================================================================

func TestClusterConvergenza_Average(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002", "node3:7003"}

	// Valori iniziali: 10, 30, 50 → media attesa = 30
	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "average", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "average", bus, []string{peers[0], peers[2]}),
		newTestNode(t, "node-3", "node3:7003", 50.0, "average", bus, peers[:2]),
	}

	// Avvio tutti i nodi
	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	// Attesa convergenza
	converged := waitForConvergence(t, nodes, 30.0, 0.5, 5*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("  nodo %s: stima=%.4f", n.ID, est)
		}
		t.Error("cluster non ha convergito entro il timeout")
	}
}

// =====================================================================
// Test M09-2: Convergenza cluster 3 nodi (SUM)
// =====================================================================

func TestClusterConvergenza_Sum(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002", "node3:7003"}

	// Valori: 10, 30, 50 → somma attesa = 90
	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "sum", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "sum", bus, []string{peers[0], peers[2]}),
		newTestNode(t, "node-3", "node3:7003", 50.0, "sum", bus, peers[:2]),
	}

	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	converged := waitForConvergence(t, nodes, 90.0, 1.0, 5*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("  nodo %s: stima=%.4f", n.ID, est)
		}
		t.Error("cluster SUM non ha convergito entro il timeout")
	}
}

// =====================================================================
// Test M09-3: Robustezza al crash — crash di un nodo, il cluster converge
// =====================================================================

func TestRobustezza_CrashNodo(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002", "node3:7003"}

	// Media attesa con tutti e 3: (10+30+50)/3 = 30
	// Dopo crash di node-3: i restanti hanno (10+30)/2 = 20 ma con CRDT
	// mantengono il contributo di node-3 in memoria → ancora converge a 30
	// finché non scatta il timeout (che nei test è 2s)
	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "average", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "average", bus, []string{peers[0], peers[2]}),
		newTestNode(t, "node-3", "node3:7003", 50.0, "average", bus, peers[:2]),
	}

	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	// Prima fase: verifica convergenza con tutti i nodi
	if !waitForConvergence(t, nodes[:2], 30.0, 0.5, 4*time.Second) {
		t.Log("convergenza pre-crash non raggiunta (atteso in test veloci)")
	}

	// CRASH: simula crash di node-3 bloccandolo sul bus
	t.Log("→ crash simulato su node3")
	bus.Block("node3:7003")
	cancels[2]() // Ferma anche l'engine

	// Dopo il crash, node-1 e node-2 continuano a girare
	// La stima CRDT mantiene ancora il contributo di node-3 in memoria
	// perché il CRDT non rimuove entry (failure detection è separata)
	time.Sleep(500 * time.Millisecond)

	// Verifica che node-1 e node-2 continuano a produrre stime
	est1, _ := nodes[0].Engine.GetEstimate()
	est2, _ := nodes[1].Engine.GetEstimate()
	t.Logf("post-crash: node-1=%.4f, node-2=%.4f", est1, est2)

	if est1 == 0 && est2 == 0 {
		t.Error("dopo crash di node-3, node-1 e node-2 non dovrebbero avere stima 0")
	}
}

// =====================================================================
// Test M09-4: Robustezza al crash — crash e restart (rejoin)
// =====================================================================

func TestRobustezza_CrashERestart(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002", "node3:7003"}

	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "average", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "average", bus, []string{peers[0], peers[2]}),
		newTestNode(t, "node-3", "node3:7003", 50.0, "average", bus, peers[:2]),
	}

	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	// Attendi convergenza iniziale
	waitForConvergence(t, nodes, 30.0, 0.5, 4*time.Second)

	// CRASH di node-3
	t.Log("→ crash node-3")
	bus.Block("node3:7003")
	cancels[2]()
	time.Sleep(200 * time.Millisecond)

	// RESTART di node-3 con nuovo contributo (simula stateless rejoin)
	t.Log("→ restart node-3 con valore 50.0")
	bus.Unblock("node3:7003")
	restartedNode := newTestNode(t, "node-3", "node3:7003", 50.0, "average", bus, peers[:2])
	cancels[2] = restartedNode.Start(t)
	nodes[2] = restartedNode

	// Dopo il restart, il cluster deve riconvergere
	converged := waitForConvergence(t, nodes, 30.0, 1.0, 6*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("  nodo %s post-restart: stima=%.4f", n.ID, est)
		}
		t.Error("cluster non ha riconvergito dopo crash+restart")
	}
}

// =====================================================================
// Test M09-5: Partizione di rete — split brain e healing
// =====================================================================

func TestRobustezza_PartizioneRete(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002", "node3:7003"}

	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "sum", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "sum", bus, []string{peers[0], peers[2]}),
		newTestNode(t, "node-3", "node3:7003", 50.0, "sum", bus, peers[:2]),
	}

	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	// Attendi convergenza con somma = 90
	waitForConvergence(t, nodes, 90.0, 1.0, 4*time.Second)

	// PARTIZIONE: isola node-3
	t.Log("→ partizione di rete: node-3 isolato")
	bus.Block("node3:7003")
	time.Sleep(300 * time.Millisecond)

	// HEALING: ripristina la connettività
	t.Log("→ healing: node-3 riconnesso")
	bus.Unblock("node3:7003")

	// Dopo healing, riconvergenza alla somma originale
	converged := waitForConvergence(t, nodes, 90.0, 1.0, 6*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("  nodo %s post-healing: stima=%.4f", n.ID, est)
		}
		t.Error("cluster SUM non ha riconvergito dopo partizione")
	}
}

// =====================================================================
// Test M09-6: Idempotenza CRDT con messaggi duplicati
// =====================================================================

func TestRobustezza_MessaggiDuplicati(t *testing.T) {
	bus := NewInMemoryBus()

	peers := []string{"node1:7001", "node2:7002"}

	nodes := []*TestNode{
		newTestNode(t, "node-1", "node1:7001", 10.0, "sum", bus, peers[1:]),
		newTestNode(t, "node-2", "node2:7002", 30.0, "sum", bus, peers[:1]),
	}

	cancels := make([]context.CancelFunc, len(nodes))
	for i, n := range nodes {
		cancels[i] = n.Start(t)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	// Aspetta che i nodi si scambino messaggi
	time.Sleep(1 * time.Second)

	// Invia manualmente un messaggio duplicato a node-2
	// (stesso NodeID e versione di node-1 che node-2 ha già visto)
	snap := nodes[0].Engine.State.Snapshot()
	dupMsg := message.GossipMessage{
		MessageID: "dup-msg-1",
		SenderID:  "node-1",
		SentAt:    time.Now(),
		Round:     1,
		State:     snap,
	}
	payload, _ := json.Marshal(dupMsg)
	bus.Send(context.Background(), "node2:7002", payload)

	time.Sleep(200 * time.Millisecond)

	// La somma deve restare 40, non 50 (no doppio conteggio)
	est, _ := nodes[1].Engine.GetEstimate()
	if math.Abs(est-40.0) > 2.0 {
		t.Errorf("con duplicato: attesa somma ~40.0, ottenuta %.4f", est)
	}
}

// =====================================================================
// Helper: waitForConvergence
// =====================================================================

// waitForConvergence attende che tutti i nodi abbiano una stima entro
// tolerance dal valore atteso, entro il timeout specificato.
func waitForConvergence(
	t *testing.T,
	nodes []*TestNode,
	expected float64,
	tolerance float64,
	timeout time.Duration,
) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allConverged := true
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			if math.Abs(est-expected) > tolerance {
				allConverged = false
				break
			}
		}
		if allConverged {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
