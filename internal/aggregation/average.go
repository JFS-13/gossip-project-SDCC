package aggregation

import "gossip-project/internal/message"

// AverageAggregator implementa la media con CRDT per-contributo.
//
// Ogni nodo contribuisce una coppia (Sum, Count). Il risultato è:
//
//	AVERAGE = Σ contributions[nodeID].Sum / Σ contributions[nodeID].Count
//
// Usando Sum e Count separati per nodo, la media è calcolata correttamente
// anche con nodi che contribuiscono più volte (il CRDT prende la versione più alta).
type AverageAggregator struct{}

func (a *AverageAggregator) Type() string { return "average" }

func (a *AverageAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	state.Contributions[nodeID] = message.Contribution{
		Sum:     value,
		Count:   1,
		Version: version,
	}
}

func (a *AverageAggregator) ComputeResult(state *message.AggregationState) float64 {
	if state == nil || state.Contributions == nil {
		return 0
	}
	var totalSum float64
	var totalCount uint64
	for _, contrib := range state.Contributions {
		totalSum += contrib.Sum
		totalCount += contrib.Count
	}
	if totalCount == 0 {
		return 0
	}
	return totalSum / float64(totalCount)
}
