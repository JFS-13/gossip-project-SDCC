package message

import (
	"crypto/rand"
	"fmt"
	"time"
)

// =====================================================================
// Identificatori
// =====================================================================

// NodeID identifica univocamente un nodo nel cluster.
type NodeID string

// MessageID identifica univocamente un messaggio gossip.
type MessageID string

// GenerateMessageID genera un ID univoco per un messaggio gossip
// usando 16 byte casuali (equivalente a UUID v4).
func GenerateMessageID(sender NodeID, round uint64) MessageID {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return MessageID(fmt.Sprintf("%s-r%d-%x", sender, round, b))
}

// =====================================================================
// Contribution — tipo unificato CRDT per-contributo
// =====================================================================

// Contribution rappresenta il contributo di un singolo nodo a un'aggregazione.
// Ogni tipo di aggregazione usa i campi che gli servono:
//
//   - SUM:     Value + Version
//   - AVERAGE: Sum + Count + Version
//   - MIN:     Value + Version
//   - MAX:     Value + Version
//   - TOP_K:   TopK + Version
//
// Il merge CRDT è generico: per ogni NodeID, si prende il contributo
// con (Epoch, Version) più alto. L'Epoch cambia ad ogni riavvio del nodo,
// garantendo che un nodo riavviato non venga ignorato dal CRDT.
type Contribution struct {
	Value   float64   `json:"value,omitempty"` // Valore scalare (SUM, MIN, MAX)
	Sum     float64   `json:"sum,omitempty"`   // Somma parziale (AVERAGE)
	Count   uint64    `json:"count,omitempty"` // Conteggio (AVERAGE)
	TopK    []float64 `json:"top_k,omitempty"` // Valori top-K ordinati desc
	Epoch   int64     `json:"epoch"`           // Epoca: timestamp di avvio del nodo (UnixNano)
	Version uint64    `json:"version"`         // Versione monotona crescente dentro la stessa epoca
}

// =====================================================================
// AggregationState — stato CRDT con mappa per-contributo
// =====================================================================

// AggregationState contiene lo stato CRDT per un'aggregazione.
// La mappa Contributions associa ogni NodeID al suo contributo corrente.
// Il merge tra due AggregationState è idempotente:
//
//	per ogni NodeID, si prende il contributo con Version maggiore.
type AggregationState struct {
	Type          string                  `json:"type"`          // "sum", "average", "min", "max", "topk"
	Contributions map[NodeID]Contribution `json:"contributions"` // {nodeID → contributo}
}

// EnsureContributions inizializza la mappa contributions se nil.
func (s *AggregationState) EnsureContributions() {
	if s.Contributions == nil {
		s.Contributions = make(map[NodeID]Contribution)
	}
}

// Clone restituisce una copia profonda dell'AggregationState.
// Necessario per evitare race condition quando si invia lo stato via gossip.
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

// MergeCRDT esegue il merge CRDT tra lo stato locale e quello remoto.
// Per ogni NodeID, prende il contributo con (Epoch, Version) più alto.
// L'ordine di confronto è:
//  1. Epoch maggiore vince (indica un riavvio più recente)
//  2. A parità di Epoch, Version maggiore vince
//
// Questa operazione è:
//   - Idempotente: merge(A, A) = A
//   - Commutativa: merge(A, B) = merge(B, A)
//   - Associativa: merge(merge(A, B), C) = merge(A, merge(B, C))
//
// Restituisce true se lo stato locale è stato modificato.
func (s *AggregationState) MergeCRDT(remote *AggregationState) bool {
	if remote == nil {
		return false
	}
	s.EnsureContributions()

	changed := false
	for nodeID, remoteContrib := range remote.Contributions {
		localContrib, exists := s.Contributions[nodeID]

		// Accetta il contributo remoto se:
		// - il nodo è sconosciuto (!exists), oppure
		// - l'Epoch remoto è più recente (il nodo si è riavviato), oppure
		// - a parità di Epoch, la Version remota è maggiore
		accept := !exists ||
			remoteContrib.Epoch > localContrib.Epoch ||
			(remoteContrib.Epoch == localContrib.Epoch && remoteContrib.Version > localContrib.Version)

		if accept {
			// Copia profonda del contributo remoto
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

// ContributionCount restituisce il numero di nodi che hanno contribuito.
func (s *AggregationState) ContributionCount() int {
	if s.Contributions == nil {
		return 0
	}
	return len(s.Contributions)
}

// =====================================================================
// GossipMessage — envelope del messaggio gossip
// =====================================================================

// GossipMessage è il payload completo scambiato tra nodi via gossip.
// Contiene l'envelope (chi, quando, quale round) e lo stato CRDT.
// Il campo Membership trasporta informazioni di membership via piggybacking,
// permettendo la scoperta automatica di nuovi nodi senza un servizio separato.
type GossipMessage struct {
	MessageID  MessageID         `json:"message_id"`           // ID univoco del messaggio
	SenderID   NodeID            `json:"sender_id"`            // Nodo che ha inviato
	SentAt     time.Time         `json:"sent_at"`              // Timestamp di invio
	Round      uint64            `json:"round"`                // Round gossip del sender
	State      AggregationState  `json:"state"`                // Stato CRDT da mergiare
	Membership []MembershipEntry `json:"membership,omitempty"` // Piggybacking membership
}

// =====================================================================
// Membership — tipi per la gestione della topologia di rete
// =====================================================================

// Costanti per lo stato di un membro del cluster.
const (
	StatusAlive   = "alive"   // Nodo attivo e raggiungibile
	StatusSuspect = "suspect" // Nodo potenzialmente guasto (timeout heartbeat)
	StatusDead    = "dead"    // Nodo dichiarato morto (rimosso dal cluster)
	StatusLeave   = "leave"   // Nodo uscito volontariamente (graceful shutdown)
)

// MembershipEntry rappresenta la vista serializzabile di un peer nel cluster.
// Viene scambiata via piggybacking nei messaggi gossip per permettere
// la scoperta automatica di nuovi nodi e il failure detection distribuito.
type MembershipEntry struct {
	NodeID      NodeID    `json:"node_id"`     // Identificativo del nodo
	Addr        string    `json:"addr"`        // Indirizzo di rete (host:port)
	Status      string    `json:"status"`      // alive, suspect, dead, leave
	Incarnation uint64    `json:"incarnation"` // Numero di incarnazione (monotono)
	LastSeen    time.Time `json:"last_seen"`   // Ultimo heartbeat ricevuto
}

// IsReachable restituisce true se il nodo è in uno stato raggiungibile
// (alive o suspect). I nodi dead o leave non ricevono messaggi gossip.
func (e MembershipEntry) IsReachable() bool {
	return e.Status == StatusAlive || e.Status == StatusSuspect
}
