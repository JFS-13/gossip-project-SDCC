package aggregation

import "gossip-project/internal/message"

// SumAggregator implementa l'aggregazione di somma con CRDT per-contributo.
//
// Ogni nodo contribuisce un valore scalare. Il risultato è la somma
// di tutti i contributi: SUM = Σ contributions[nodeID].Value
//
// Grazie al CRDT per-contributo, la somma è idempotente e converge
// anche in presenza di messaggi duplicati o riordinati.
type SumAggregator struct{}

func (a *SumAggregator) Type() string { return "sum" }

func (a *SumAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
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

func (a *SumAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	if state == nil || state.Contributions == nil {
		return 0
	}
	var sum float64
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		sum += contrib.Value
	}
	return sum
}
