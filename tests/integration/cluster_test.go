// Package integration contiene test di integrazione per verificare la convergenza
// del cluster gossip e la tolleranza ai guasti utilizzando un transport in-memory.
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

// InMemoryBus gestisce il recapito dei messaggi in-memory tra i nodi di test.
type InMemoryBus struct {
	mu       sync.RWMutex
	handlers map[string]transport.MessageHandler
	blocked  map[string]bool
}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{
		handlers: make(map[string]transport.MessageHandler),
		blocked:  make(map[string]bool),
	}
}

func (b *InMemoryBus) Register(addr string, handler transport.MessageHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[addr] = handler
}

func (b *InMemoryBus) Send(ctx context.Context, destAddr string, payload []byte) error {
	b.mu.RLock()
	handler, exists := b.handlers[destAddr]
	isBlocked := b.blocked[destAddr]
	b.mu.RUnlock()

	if !exists || isBlocked {
		return nil
	}
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)
	go handler(ctx, payloadCopy)
	return nil
}

func (b *InMemoryBus) Block(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked[addr] = true
}

func (b *InMemoryBus) Unblock(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked[addr] = false
}

// InMemoryTransport implementa l'interfaccia transport tramite InMemoryBus.
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

// TestNode aggrega le istanze necessarie per emulare un nodo completo.
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
		peers,
		tr,
		agg,
		mset,
		100*time.Millisecond,
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

func TestClusterConvergenza_Average(t *testing.T) {
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

	converged := waitForConvergence(t, nodes, 30.0, 0.5, 5*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("nodo %s: stima=%.4f", n.ID, est)
		}
		t.Error("cluster non ha convergito entro il timeout")
	}
}

func TestClusterConvergenza_Sum(t *testing.T) {
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

	converged := waitForConvergence(t, nodes, 90.0, 1.0, 5*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("nodo %s: stima=%.4f", n.ID, est)
		}
		t.Error("cluster SUM non ha convergito entro il timeout")
	}
}

func TestRobustezza_CrashNodo(t *testing.T) {
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

	if !waitForConvergence(t, nodes[:2], 30.0, 0.5, 4*time.Second) {
		t.Log("convergenza pre-crash non raggiunta")
	}

	bus.Block("node3:7003")
	cancels[2]()

	time.Sleep(500 * time.Millisecond)

	est1, _ := nodes[0].Engine.GetEstimate()
	est2, _ := nodes[1].Engine.GetEstimate()

	if est1 == 0 && est2 == 0 {
		t.Error("i nodi restanti non dovrebbero avere stima nulla")
	}
}

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

	waitForConvergence(t, nodes, 30.0, 0.5, 4*time.Second)

	bus.Block("node3:7003")
	cancels[2]()
	time.Sleep(200 * time.Millisecond)

	bus.Unblock("node3:7003")
	restartedNode := newTestNode(t, "node-3", "node3:7003", 50.0, "average", bus, peers[:2])
	cancels[2] = restartedNode.Start(t)
	nodes[2] = restartedNode

	converged := waitForConvergence(t, nodes, 30.0, 1.0, 6*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("nodo %s post-restart: stima=%.4f", n.ID, est)
		}
		t.Error("cluster non ha riconvergito dopo restart")
	}
}

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

	waitForConvergence(t, nodes, 90.0, 1.0, 4*time.Second)

	bus.Block("node3:7003")
	time.Sleep(300 * time.Millisecond)

	bus.Unblock("node3:7003")

	converged := waitForConvergence(t, nodes, 90.0, 1.0, 6*time.Second)
	if !converged {
		for _, n := range nodes {
			est, _ := n.Engine.GetEstimate()
			t.Logf("nodo %s post-healing: stima=%.4f", n.ID, est)
		}
		t.Error("cluster non ha riconvergito dopo partizione")
	}
}

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

	time.Sleep(1 * time.Second)

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

	est, _ := nodes[1].Engine.GetEstimate()
	if math.Abs(est-40.0) > 2.0 {
		t.Errorf("con duplicato: attesa stima ~40.0, ottenuta %.4f", est)
	}
}

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
