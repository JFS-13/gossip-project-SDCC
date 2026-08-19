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
	DeadSince   time.Time
}

// Config definisce le tempistiche operative per il failure detector.
type Config struct {
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	CleanupTimeout time.Duration
}

// Manager gestisce l'elenco dei membri e il failure detection locale.
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

	m.members[selfID] = &Member{
		NodeID:      selfID,
		Addr:        selfAddr,
		Status:      StatusAlive,
		Incarnation: uint64(time.Now().UnixNano()),
		LastSeen:    time.Now(),
	}

	return m
}

// IncrementIncarnation incrementa l'incarnazione del nodo locale fungendo da heartbeat.
func (m *Manager) IncrementIncarnation() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if self, ok := m.members[m.selfID]; ok {
		self.Incarnation++
		self.LastSeen = time.Now()
	}
}

// AddPeer inserisce un nuovo peer o ne aggiorna il tempo di ultimo contatto.
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

// TouchPeer aggiorna il tempo di ultimo contatto di un peer conosciuto.
func (m *Manager) TouchPeer(nodeID message.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[nodeID]; exists {
		member.LastSeen = time.Now()
	}
}

// GetAlivePeers restituisce tutti i peer correntemente attivi o sospetti.
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

// GetRandomPeers seleziona un sottoinsieme casuale di peer attivi, escludendo il nodo locale.
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

// GetClusterSize calcola il numero totale di nodi vivi e sospetti.
func (m *Manager) GetClusterSize() int {
	return len(m.GetAlivePeers())
}

// MergeMembership applica gli aggiornamenti di stato pervenuti dai peer remoti.
func (m *Manager) MergeMembership(entries []message.MembershipEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, entry := range entries {
		if entry.NodeID == m.selfID {
			continue
		}

		local, exists := m.members[entry.NodeID]
		if !exists {
			m.members[entry.NodeID] = &Member{
				NodeID:      entry.NodeID,
				Addr:        entry.Addr,
				Status:      MemberStatus(entry.Status),
				Incarnation: entry.Incarnation,
				LastSeen:    now,
			}
			continue
		}

		if entry.Incarnation > local.Incarnation {
			local.Incarnation = entry.Incarnation
			local.Status = MemberStatus(entry.Status)
			local.LastSeen = now
			local.Addr = entry.Addr
		} else if entry.Incarnation == local.Incarnation {
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

// SnapshotEntries cattura un'istantanea di tutte le voci della membership locale.
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

// CheckTimeouts analizza i timer di inattività per identificare e rimuovere i nodi falliti.
func (m *Manager) CheckTimeouts(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var toRemove []message.NodeID

	for _, member := range m.members {
		if member.NodeID == m.selfID {
			continue
		}

		if member.Status == StatusLeave {
			if m.config.CleanupTimeout > 0 && !member.DeadSince.IsZero() &&
				now.Sub(member.DeadSince) > m.config.CleanupTimeout {
				toRemove = append(toRemove, member.NodeID)
			}
			continue
		}

		if member.Status == StatusDead {
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

	for _, nodeID := range toRemove {
		delete(m.members, nodeID)
	}
}

// RemovePeer elimina in modo esplicito un nodo dalla membership.
func (m *Manager) RemovePeer(nodeID message.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, nodeID)
}
