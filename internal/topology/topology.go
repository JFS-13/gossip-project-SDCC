package topology

import (
	"math/rand"
	"sync"
	"time"

	"gossip-project/internal/message"
)

// MemberStatus rappresenta lo stato di un nodo nel cluster.
type MemberStatus string

const (
	StatusAlive   MemberStatus = "alive"
	StatusSuspect MemberStatus = "suspect"
	StatusDead    MemberStatus = "dead"
	StatusLeave   MemberStatus = "leave"
)

// Member rappresenta un nodo noto al sistema.
type Member struct {
	NodeID      message.NodeID
	Addr        string
	Status      MemberStatus
	Incarnation uint64
	LastSeen    time.Time
	DeadSince   time.Time // Timestamp di quando il nodo è stato dichiarato dead (zero se non dead)
}

// Config contiene le tempistiche per il failure detector.
type Config struct {
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	CleanupTimeout time.Duration // Tempo dopo il quale un nodo dead viene rimosso dalla memoria (0 = mai)
}

// Manager gestisce l'elenco dei membri e il failure detection.
type Manager struct {
	selfID   message.NodeID
	selfAddr string
	members  map[message.NodeID]*Member
	config   Config
	mu       sync.RWMutex
}

// NewManager inizializza un nuovo gestore della membership.
func NewManager(selfID message.NodeID, selfAddr string, config Config, initialPeers []string) *Manager {
	m := &Manager{
		selfID:   selfID,
		selfAddr: selfAddr,
		members:  make(map[message.NodeID]*Member),
		config:   config,
	}

	// Aggiunge sé stesso con incarnazione 0
	m.members[selfID] = &Member{
		NodeID:      selfID,
		Addr:        selfAddr,
		Status:      StatusAlive,
		Incarnation: 0,
		LastSeen:    time.Now(),
	}

	return m
}

// IncrementIncarnation incrementa l'incarnazione del nodo locale.
// Questo funge da heartbeat: le altre istanze nel cluster, vedendo un
// 'incarnazione più alta, aggiorneranno il LastSeen prevenendo falsi positivi.
func (m *Manager) IncrementIncarnation() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if self, ok := m.members[m.selfID]; ok {
		self.Incarnation++
		self.LastSeen = time.Now()
	}
}

// AddPeer aggiunge un peer all'elenco o ne aggiorna l'ultimo avvistamento.
func (m *Manager) AddPeer(nodeID message.NodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[nodeID]; exists {
		member.Status = StatusAlive
		member.LastSeen = time.Now()
		if addr != "" {
			member.Addr = addr
		}
	} else {
		m.members[nodeID] = &Member{
			NodeID:      nodeID,
			Addr:        addr,
			Status:      StatusAlive,
			Incarnation: 0,
			LastSeen:    time.Now(),
		}
	}
}

// TouchPeer aggiorna il LastSeen di un peer conosciuto.
func (m *Manager) TouchPeer(nodeID message.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[nodeID]; exists {
		member.LastSeen = time.Now()
	}
}

// GetAlivePeers restituisce tutti i peer in stato alive o suspect.
func (m *Manager) GetAlivePeers() []message.MembershipEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var alive []message.MembershipEntry
	for _, member := range m.members {
		if member.Status == StatusAlive || member.Status == StatusSuspect {
			alive = append(alive, message.MembershipEntry{
				NodeID:      member.NodeID,
				Addr:        member.Addr,
				Status:      string(member.Status),
				Incarnation: member.Incarnation,
				LastSeen:    member.LastSeen,
			})
		}
	}
	return alive
}

// GetRandomPeers seleziona un massimo di n peer casuali (escludendo sé stesso).
func (m *Manager) GetRandomPeers(n int) []message.MembershipEntry {
	alive := m.GetAlivePeers()
	var candidates []message.MembershipEntry

	for _, p := range alive {
		if p.NodeID != m.selfID {
			candidates = append(candidates, p)
		}
	}

	if len(candidates) <= n {
		return candidates
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	return candidates[:n]
}

// GetClusterSize restituisce il numero totale di nodi vivi e sospetti (incluso se stesso).
func (m *Manager) GetClusterSize() int {
	return len(m.GetAlivePeers())
}

// MergeMembership unisce le entry ricevute da remoto.
func (m *Manager) MergeMembership(entries []message.MembershipEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, entry := range entries {
		// Non si aggiorna lo stato di sé stessi da informazioni esterne
		if entry.NodeID == m.selfID {
			continue
		}

		local, exists := m.members[entry.NodeID]
		if !exists {
			// Nuovo nodo
			m.members[entry.NodeID] = &Member{
				NodeID:      entry.NodeID,
				Addr:        entry.Addr,
				Status:      MemberStatus(entry.Status),
				Incarnation: entry.Incarnation,
				LastSeen:    now,
			}
			continue
		}

		// Se l'incarnazione ricevuta è maggiore, si accetta il nuovo stato
		if entry.Incarnation > local.Incarnation {
			local.Incarnation = entry.Incarnation
			local.Status = MemberStatus(entry.Status)
			local.LastSeen = now
			local.Addr = entry.Addr
		} else if entry.Incarnation == local.Incarnation {
			// A parità di incarnazione, suspect prevale su alive, dead prevale su suspect, leave è definitivo
			if local.Status == StatusAlive && entry.Status == string(StatusSuspect) {
				local.Status = StatusSuspect
			} else if (local.Status == StatusAlive || local.Status == StatusSuspect) && entry.Status == string(StatusDead) {
				local.Status = StatusDead
			} else if entry.Status == string(StatusLeave) {
				local.Status = StatusLeave
			}
		}
	}
}

// SnapshotEntries restituisce lo stato completo dei membri.
func (m *Manager) SnapshotEntries() []message.MembershipEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []message.MembershipEntry
	for _, member := range m.members {
		entries = append(entries, message.MembershipEntry{
			NodeID:      member.NodeID,
			Addr:        member.Addr,
			Status:      string(member.Status),
			Incarnation: member.Incarnation,
			LastSeen:    member.LastSeen,
		})
	}
	return entries
}

// CheckTimeouts verifica le scadenze temporali per rilevare fallimenti
// e rimuove i nodi dead da troppo tempo (tombstone cleanup).
func (m *Manager) CheckTimeouts(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var toRemove []message.NodeID

	for _, member := range m.members {
		if member.NodeID == m.selfID {
			continue
		}

		// Skip nodi in leave (già rimossi logicamente)
		if member.Status == StatusLeave {
			// I nodi leave possono essere rimossi dopo il cleanup timeout
			if m.config.CleanupTimeout > 0 && !member.DeadSince.IsZero() &&
				now.Sub(member.DeadSince) > m.config.CleanupTimeout {
				toRemove = append(toRemove, member.NodeID)
			}
			continue
		}

		// Skip nodi già dead
		if member.Status == StatusDead {
			// Tombstone cleanup: rimuovi nodi dead da troppo tempo
			if m.config.CleanupTimeout > 0 && !member.DeadSince.IsZero() &&
				now.Sub(member.DeadSince) > m.config.CleanupTimeout {
				toRemove = append(toRemove, member.NodeID)
			}
			continue
		}

		elapsed := now.Sub(member.LastSeen)

		if member.Status == StatusAlive && elapsed > m.config.SuspectTimeout {
			member.Status = StatusSuspect
		} else if member.Status == StatusSuspect && elapsed > (m.config.SuspectTimeout+m.config.DeadTimeout) {
			member.Status = StatusDead
			member.DeadSince = now
		}
	}

	// Rimuovi i nodi dead/leave da troppo tempo
	for _, nodeID := range toRemove {
		delete(m.members, nodeID)
	}
}

// RemovePeer rimuove completamente un nodo dall'elenco.
func (m *Manager) RemovePeer(nodeID message.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, nodeID)
}
