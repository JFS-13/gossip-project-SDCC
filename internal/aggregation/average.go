package aggregation

import "gossip-project/internal/message"

// AverageAggregator esegue il calcolo della media distribuita sfruttando la semantica CRDT.
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

func (a *AverageAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	if state == nil || state.Contributions == nil {
		return 0
	}
	var totalSum float64
	var totalCount uint64
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		totalSum += contrib.Sum
		totalCount += contrib.Count
	}
	if totalCount == 0 {
		return 0
	}
	return totalSum / float64(totalCount)
}
