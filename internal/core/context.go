package core

import (
	"sync"
	"time"

	"gossip-project/internal/message"
)

// EngineState rappresenta lo stato completo di un nodo gossip.
type EngineState struct {
	NodeID          message.NodeID
	MyEpoch         int64
	Round           uint64
	AggregationType string
	LocalValue      float64
	Estimate        float64
	Aggregation     message.AggregationState
	mu              sync.RWMutex
}

// NewEngineState inizializza lo stato e imposta il contributo del nodo.
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

// UpdateLocalContribution aggiorna il contributo locale e incrementa la versione associata.
func (s *EngineState) UpdateLocalContribution(value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.LocalValue = value

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

// MergeRemote applica le modifiche di uno stato remoto a quello locale.
func (s *EngineState) MergeRemote(remote *message.AggregationState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Aggregation.MergeCRDT(remote)
}

// Snapshot restituisce una copia sicura e indipendente dello stato di aggregazione.
func (s *EngineState) Snapshot() message.AggregationState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := message.AggregationState{
		Type:          s.Aggregation.Type,
		Contributions: make(map[message.NodeID]message.Contribution, len(s.Aggregation.Contributions)),
	}

	for k, v := range s.Aggregation.Contributions {
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

// GetEstimate restituisce il valore stimato.
func (s *EngineState) GetEstimate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Estimate
}

// SetEstimate imposta il valore stimato.
func (s *EngineState) SetEstimate(estimate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Estimate = estimate
}

// GetRound restituisce l'identificativo del round corrente.
func (s *EngineState) GetRound() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Round
}

// IncrementRound incrementa il round corrente.
func (s *EngineState) IncrementRound() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Round++
}
