package aggregation

import (
	"gossip-project/internal/message"
	"math"
)

// MinAggregator individua il valore minimo globale tra tutti i nodi partecipanti.
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

func (a *MinAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	if state == nil || len(state.Contributions) == 0 {
		return 0
	}
	result := math.Inf(1)
	hasAlive := false
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		hasAlive = true
		if contrib.Value < result {
			result = contrib.Value
		}
	}
	if !hasAlive {
		return 0
	}
	return result
}

// MaxAggregator individua il valore massimo globale tra tutti i nodi partecipanti.
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

func (a *MaxAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	if state == nil || len(state.Contributions) == 0 {
		return 0
	}
	result := math.Inf(-1)
	hasAlive := false
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		hasAlive = true
		if contrib.Value > result {
			result = contrib.Value
		}
	}
	if !hasAlive {
		return 0
	}
	return result
}
