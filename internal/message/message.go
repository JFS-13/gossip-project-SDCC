package message

import (
	"crypto/rand"
	"fmt"
	"time"
)

// NodeID identifica univocamente un nodo nel cluster.
type NodeID string

// MessageID identifica univocamente un messaggio gossip.
type MessageID string

// GenerateMessageID genera un identificatore causale univoco per un messaggio.
func GenerateMessageID(sender NodeID, round uint64) MessageID {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return MessageID(fmt.Sprintf("%s-r%d-%x", sender, round, b))
}

// Contribution rappresenta il contributo scalare fornito da un singolo nodo all'aggregazione complessiva.
type Contribution struct {
	Value   float64   `json:"value,omitempty"`
	Sum     float64   `json:"sum,omitempty"`
	Count   uint64    `json:"count,omitempty"`
	TopK    []float64 `json:"top_k,omitempty"`
	Epoch   int64     `json:"epoch"`
	Version uint64    `json:"version"`
}

// AggregationState mappa l'identità di ciascun nodo al proprio contributo corrente.
type AggregationState struct {
	Type          string                  `json:"type"`
	Contributions map[NodeID]Contribution `json:"contributions"`
}

// EnsureContributions verifica ed eventualmente inizializza la mappa interna di stato.
func (s *AggregationState) EnsureContributions() {
	if s.Contributions == nil {
		s.Contributions = make(map[NodeID]Contribution)
	}
}

// Clone produce una copia profonda della struttura per prevenire data race durante l'uso asincrono.
func (s *AggregationState) Clone() AggregationState {
	clone := AggregationState{
		Type:          s.Type,
		Contributions: make(map[NodeID]Contribution, len(s.Contributions)),
	}
	for nodeID, contrib := range s.Contributions {
		c := Contribution{
			Value:   contrib.Value,
			Sum:     contrib.Sum,
			Count:   contrib.Count,
			Epoch:   contrib.Epoch,
			Version: contrib.Version,
		}
		if contrib.TopK != nil {
			c.TopK = make([]float64, len(contrib.TopK))
			copy(c.TopK, contrib.TopK)
		}
		clone.Contributions[nodeID] = c
	}
	return clone
}

// MergeCRDT unisce lo stato remoto in quello locale applicando le logiche di precedenza e monotonicità.
func (s *AggregationState) MergeCRDT(remote *AggregationState) bool {
	if remote == nil {
		return false
	}
	s.EnsureContributions()

	changed := false
	for nodeID, remoteContrib := range remote.Contributions {
		localContrib, exists := s.Contributions[nodeID]

		accept := !exists ||
			remoteContrib.Epoch > localContrib.Epoch ||
			(remoteContrib.Epoch == localContrib.Epoch && remoteContrib.Version > localContrib.Version)

		if accept {
			merged := Contribution{
				Value:   remoteContrib.Value,
				Sum:     remoteContrib.Sum,
				Count:   remoteContrib.Count,
				Epoch:   remoteContrib.Epoch,
				Version: remoteContrib.Version,
			}
			if remoteContrib.TopK != nil {
				merged.TopK = make([]float64, len(remoteContrib.TopK))
				copy(merged.TopK, remoteContrib.TopK)
			}
			s.Contributions[nodeID] = merged
			changed = true
		}
	}
	return changed
}

// ContributionCount restituisce il numero di partecipanti attuali all'aggregazione.
func (s *AggregationState) ContributionCount() int {
	if s.Contributions == nil {
		return 0
	}
	return len(s.Contributions)
}

// GossipMessage definisce l'involucro contenente lo stato corrente da propagare via rete.
type GossipMessage struct {
	MessageID  MessageID         `json:"message_id"`
	SenderID   NodeID            `json:"sender_id"`
	SentAt     time.Time         `json:"sent_at"`
	Round      uint64            `json:"round"`
	State      AggregationState  `json:"state"`
	Membership []MembershipEntry `json:"membership,omitempty"`
}

const (
	StatusAlive   = "alive"
	StatusSuspect = "suspect"
	StatusDead    = "dead"
	StatusLeave   = "leave"
)

// MembershipEntry rappresenta le metriche temporali e di stato relative a un peer noto della rete.
type MembershipEntry struct {
	NodeID      NodeID    `json:"node_id"`
	Addr        string    `json:"addr"`
	Status      string    `json:"status"`
	Incarnation uint64    `json:"incarnation"`
	LastSeen    time.Time `json:"last_seen"`
}

// IsReachable restituisce un booleano per determinare se la destinazione è correntemente accessibile per le comunicazioni.
func (e MembershipEntry) IsReachable() bool {
	return e.Status == StatusAlive || e.Status == StatusSuspect
}
