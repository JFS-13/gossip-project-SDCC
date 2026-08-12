package aggregation

import (
	"gossip-project/internal/message"
	"math"
)

// MinAggregator implementa il minimo globale con CRDT per-contributo.
//
// Ogni nodo contribuisce il suo valore locale. Il risultato è:
//
//	MIN = min(contributions[nodeID].Value per ogni nodeID)
//
// Il merge CRDT (versione più alta per ogni nodeID) garantisce convergenza
// monotona: il minimo globale non può crescere se nessun nodo aggiorna il proprio valore.
type MinAggregator struct{}

func (a *MinAggregator) Type() string { return "min" }

func (a *MinAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	state.Contributions[nodeID] = message.Contribution{
		Value:   value,
		Version: version,
	}
}

func (a *MinAggregator) ComputeResult(state *message.AggregationState) float64 {
	if state == nil || len(state.Contributions) == 0 {
		return 0
	}
	result := math.Inf(1) // +Inf
	for _, contrib := range state.Contributions {
		if contrib.Value < result {
			result = contrib.Value
		}
	}
	return result
}

// MaxAggregator implementa il massimo globale con CRDT per-contributo.
//
// Ogni nodo contribuisce il suo valore locale. Il risultato è:
//
//	MAX = max(contributions[nodeID].Value per ogni nodeID)
type MaxAggregator struct{}

func (a *MaxAggregator) Type() string { return "max" }

func (a *MaxAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	state.Contributions[nodeID] = message.Contribution{
		Value:   value,
		Version: version,
	}
}

func (a *MaxAggregator) ComputeResult(state *message.AggregationState) float64 {
	if state == nil || len(state.Contributions) == 0 {
		return 0
	}
	result := math.Inf(-1) // -Inf
	for _, contrib := range state.Contributions {
		if contrib.Value > result {
			result = contrib.Value
		}
	}
	return result
}
