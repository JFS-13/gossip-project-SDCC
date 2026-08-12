package core

import (
	"sync"
	"time"

	"gossip-project/internal/message"
)

// EngineState rappresenta lo stato completo di un nodo gossip.
type EngineState struct {
	NodeID          message.NodeID
	MyEpoch         int64 // Epoca di avvio del nodo (UnixNano)
	Round           uint64
	AggregationType string                   // "sum", "average", "min", "max", "topk"
	LocalValue      float64                  // Contributo locale originale
	Estimate        float64                  // Stima di aggregazione attualmente calcolata
	Aggregation     message.AggregationState // Stato CRDT per contributo
	mu              sync.RWMutex
}

// NewEngineState inizializza lo stato e imposta il contributo del nodo.
// L'Epoch viene generato al momento della creazione (boot del nodo).
func NewEngineState(nodeID message.NodeID, aggType string, localValue float64) *EngineState {
	s := &EngineState{
		NodeID:          nodeID,
		MyEpoch:         time.Now().UnixNano(),
		AggregationType: aggType,
		LocalValue:      localValue,
		Aggregation: message.AggregationState{
			Type:          aggType,
			Contributions: make(map[message.NodeID]message.Contribution),
		},
	}
	s.UpdateLocalContribution(localValue)
	return s
}

// UpdateLocalContribution aggiorna il contributo locale e incrementa la versione.
func (s *EngineState) UpdateLocalContribution(value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.LocalValue = value

	// Ottieni la versione attuale, se esiste
	version := uint64(1)
	if curr, exists := s.Aggregation.Contributions[s.NodeID]; exists {
		version = curr.Version + 1
	}

	contrib := message.Contribution{
		Value:   value,
		Sum:     value,
		Count:   1,
		Epoch:   s.MyEpoch,
		Version: version,
	}

	s.Aggregation.Contributions[s.NodeID] = contrib
}

// UpdateLocalTopK aggiorna il contributo locale per l'aggregazione Top-K.
func (s *EngineState) UpdateLocalTopK(values []float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	version := uint64(1)
	if curr, exists := s.Aggregation.Contributions[s.NodeID]; exists {
		version = curr.Version + 1
	}

	contrib := message.Contribution{
		TopK:    values,
		Epoch:   s.MyEpoch,
		Version: version,
	}

	s.Aggregation.Contributions[s.NodeID] = contrib
}

// MergeRemote effettua un merge thread-safe del CRDT.
func (s *EngineState) MergeRemote(remote *message.AggregationState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Aggregation.MergeCRDT(remote)
}

// Snapshot restituisce una copia profonda thread-safe per l'invio.
func (s *EngineState) Snapshot() message.AggregationState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := message.AggregationState{
		Type:          s.Aggregation.Type,
		Contributions: make(map[message.NodeID]message.Contribution, len(s.Aggregation.Contributions)),
	}

	for k, v := range s.Aggregation.Contributions {
		// TopK richiede una deep copy della slice
		var topKCopy []float64
		if len(v.TopK) > 0 {
			topKCopy = make([]float64, len(v.TopK))
			copy(topKCopy, v.TopK)
		}

		snap.Contributions[k] = message.Contribution{
			Value:   v.Value,
			Sum:     v.Sum,
			Count:   v.Count,
			TopK:    topKCopy,
			Epoch:   v.Epoch,
			Version: v.Version,
		}
	}

	return snap
}

// GetEstimate restituisce la stima attuale.
func (s *EngineState) GetEstimate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Estimate
}

// SetEstimate imposta la stima attuale.
func (s *EngineState) SetEstimate(estimate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Estimate = estimate
}

// GetRound restituisce il round corrente.
func (s *EngineState) GetRound() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Round
}

// IncrementRound incrementa il round di 1.
func (s *EngineState) IncrementRound() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Round++
}
