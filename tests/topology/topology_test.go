package topology_test

import (
	"testing"
	"time"

	"gossip-project/internal/message"
	"gossip-project/internal/topology"
)

func newTestManager(selfID string, peers ...string) *topology.Manager {
	cfg := topology.Config{
		SuspectTimeout: 500 * time.Millisecond,
		DeadTimeout:    1000 * time.Millisecond,
	}
	return topology.NewManager(message.NodeID(selfID), selfID+":7001", cfg, peers)
}

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

func TestGetRandomPeers_NonSuperaDisponibili(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")

	peers := m.GetRandomPeers(10) // richiede 10, ma ne esistono solo 1
	if len(peers) > 1 {
		t.Errorf("attesi al massimo 1 peer, ottenuti %d", len(peers))
	}
}

func TestGetClusterSize(t *testing.T) {
	m := newTestManager("node-1")
	m.AddPeer("node-2", "node2:7002")
	m.AddPeer("node-3", "node3:7003")

	size := m.GetClusterSize()
	if size != 3 { // self + 2 peer
		t.Errorf("GetClusterSize atteso 3, ottenuto %d", size)
	}
}

func TestCheckTimeouts_AliveToSuspect(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 50 * time.Millisecond,
		DeadTimeout:    200 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	// Simula il timeout: LastSeen nel passato oltre SuspectTimeout
	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-2" && p.Status != message.StatusSuspect {
			t.Errorf("node-2 avrebbe dovuto essere 'suspect', ha status %q", p.Status)
		}
	}
}

func TestCheckTimeouts_SuspectToDead(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 30 * time.Millisecond,
		DeadTimeout:    50 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	// Prima chiamata: elapsed > SuspectTimeout → alive → suspect
	time.Sleep(40 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	// Seconda chiamata: elapsed > SuspectTimeout + DeadTimeout → suspect → dead
	// Aspettiamo ulteriori 60ms (totale ~100ms > 30+50=80ms)
	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	// Un nodo dead non deve apparire in GetAlivePeers (che include solo alive+suspect)
	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-2" {
			t.Errorf("node-2 morto non dovrebbe apparire in GetAlivePeers, status: %q", p.Status)
		}
	}
}

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

func TestMergeMembership_NonSovrascriveSelf(t *testing.T) {
	m := newTestManager("node-1")

	// Tenta di impostare se stesso come dead via merge remoto
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

	// Il self deve restare alive
	for _, p := range m.GetAlivePeers() {
		if p.NodeID == "node-1" && p.Status != message.StatusAlive {
			t.Errorf("self non dovrebbe essere override da merge remoto, status: %q", p.Status)
		}
	}
}

func TestMembership_Cleanup(t *testing.T) {
	cfg := topology.Config{
		SuspectTimeout: 20 * time.Millisecond,
		DeadTimeout:    30 * time.Millisecond,
		CleanupTimeout: 50 * time.Millisecond,
	}
	m := topology.NewManager("node-1", "node1:7001", cfg, nil)
	m.AddPeer("node-2", "node2:7002")

	// Fase 1: alive → suspect
	time.Sleep(25 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	// Fase 2: suspect → dead
	time.Sleep(35 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	// Fase 3: dead → cleanup (dopo CleanupTimeout)
	time.Sleep(60 * time.Millisecond)
	m.CheckTimeouts(time.Now())

	// Dopo cleanup, il cluster deve avere solo il nodo self
	size := m.GetClusterSize()
	if size != 1 {
		t.Errorf("dopo cleanup atteso 1 nodo (solo self), ottenuto %d", size)
	}
}
