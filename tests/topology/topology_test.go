package topology_test

import (
	"testing"
	"time"

	"gossip-project/internal/message"
	"gossip-project/internal/topology"
)

// Inizializza un manager con timeout ridotti specifici per i test in-memory
func newTestManager(selfID string, peers ...string) *topology.Manager {
	cfg := topology.Config{
		SuspectTimeout: 500 * time.Millisecond,
		DeadTimeout:    1000 * time.Millisecond,
	}
	return topology.NewManager(message.NodeID(selfID), selfID+":7001", cfg, peers)
}

// --- Test di Inizializzazione e Discovery Base ---
// Verifica che il nodo corrente sia sempre registrato come vivo (Alive) nella propria tabella
func TestNewManager_SelfIsAlive(t *testing.T) {
	m := newTestManager("node-1")
	peers := m.GetAlivePeers()
	found := false
	for _, p := range peers {
		if p.NodeID == "node-1" {
			found = true
			if p.Status != message.StatusAlive {
				t.Errorf("self status atteso 'alive', ottenuto %q", p.Status)
			}
		}
	}
	if !found {
		t.Error("il nodo se stesso non è presente in GetAlivePeers")
	}
}

// Assicura che un nuovo peer aggiunto esplicitamente venga immediatamente visto come vivo
func TestAddPeer_NuovoPeer(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")

	peers := m.GetAlivePeers()
	found := false
	for _, p := range peers {
		if p.NodeID == "node-2" {
			found = true
			if p.Status != message.StatusAlive {
				t.Errorf("peer status atteso 'alive', ottenuto %q", p.Status)
			}
		}
	}
	if !found {
		t.Error("node-2 non trovato dopo AddPeer")
	}
}

// --- Test per la selezione Fanout ---
// Garantisce che il nodo non includa mai sé stesso tra i target per l'invio del gossip
func TestGetRandomPeers_EscludeSelf(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")
	m.AddPeer("node-3", "node3:7003")

	for i := 0; i < 20; i++ {
		peers := m.GetRandomPeers(2)
		for _, p := range peers {
			if p.NodeID == "node-1" {
				t.Error("GetRandomPeers non dovrebbe restituire se stesso")
			}
		}
	}
}

// Verifica che il fanout non ecceda mai il numero reale di nodi vivi e sospetti disponibili
func TestGetRandomPeers_NonSuperaDisponibili(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")

	peers := m.GetRandomPeers(10)
	if len(peers) > 1 {
		t.Errorf("attesi al massimo 1 peer, ottenuti %d", len(peers))
	}
}

func TestGetClusterSize(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")
	m.AddPeer("node-3", "node3:7003")

	size := m.GetClusterSize()
	if size != 3 {
		t.Errorf("GetClusterSize atteso 3, ottenuto %d", size)
	}
}

// --- Test sul Ciclo di Vita ---
// Un nodo che smette di mandare messaggi per `SuspectTimeout` viene declassato a Suspect
func TestCheckTimeouts_AliveToSuspect(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 50 * time.Millisecond,
		DeadTimeout:    200 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-2" && p.Status != message.StatusSuspect {
			t.Errorf("node-2 avrebbe dovuto essere 'suspect', ha status %q", p.Status)
		}
	}
}

// Un nodo Suspect passa a Dead dopo che scade anche il `DeadTimeout` senza segnali di vita
func TestCheckTimeouts_SuspectToDead(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 30 * time.Millisecond,
		DeadTimeout:    50 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	time.Sleep(40 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-2" {
			t.Errorf("node-2 morto non dovrebbe apparire in GetAlivePeers, status: %q", p.Status)
		}
	}
}

// --- Test sul Merge e l'Incarnation ---
// Verifica che il gossip propaghi l'esistenza di nodi scoperti indirettamente (P2P Discovery)
func TestMergeMembership_NuovoPeer(t *testing.T) {
	m := newTestManager("node-1")

	entries := []message.MembershipEntry{
		{
			NodeID:      "node-4",
			Addr:        "node4:7004",
			Status:      message.StatusAlive,
			Incarnation: 0,
			LastSeen:    time.Now(),
		},
	}
	m.MergeMembership(entries)

	size := m.GetClusterSize()
	if size < 2 {
		t.Errorf("dopo MergeMembership atteso almeno 2 nodi, ottenuto %d", size)
	}
}

// Previene gli attacchi o i falsi negativi impedendo ad altri nodi di marcare il nodo corrente come Dead
func TestMergeMembership_NonSovrascriveSelf(t *testing.T) {
	m := newTestManager("node-1")

	entries := []message.MembershipEntry{
		{
			NodeID:      "node-1",
			Addr:        "node1:7001",
			Status:      message.StatusDead,
			Incarnation: 99,
			LastSeen:    time.Now(),
		},
	}
	m.MergeMembership(entries)

	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-1" && p.Status != message.StatusAlive {
			t.Errorf("self non dovrebbe essere override da merge remoto, status: %q", p.Status)
		}
	}
}

// Quando un nodo supera abbondantemente il timeout e resta Dead, viene espulso (pruned) dalla tabella
func TestMembership_Cleanup(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 20 * time.Millisecond,
		DeadTimeout:    30 * time.Millisecond,
		CleanupTimeout: 50 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	time.Sleep(25 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	time.Sleep(35 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	size := m.GetClusterSize()
	if size != 1 {
		t.Errorf("dopo cleanup atteso 1 nodo (solo self), ottenuto %d", size)
	}
}
