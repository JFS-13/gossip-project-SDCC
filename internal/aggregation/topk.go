package aggregation

import (
	"gossip-project/internal/message"
	"sort"
)

// TopKAggregator calcola e aggrega gli elementi con i valori più alti tra tutti i nodi partecipanti.
type TopKAggregator struct {
	K int
}

func (a *TopKAggregator) Type() string { return "topk" }

// SetContribution inserisce un singolo valore nell'insieme Top-K del nodo.
func (a *TopKAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
		topk := append(curr.TopK, value)
		sort.Float64s(topk)
		if len(topk) > a.K {
			topk = topk[len(topk)-a.K:]
		}
		state.Contributions[nodeID] = message.Contribution{
			TopK:    topk,
			Version: version,
		}
		return
	}
	state.Contributions[nodeID] = message.Contribution{
		TopK:    []float64{value},
		Version: version,
	}
}

// SetTopKContribution sovrascrive direttamente la collezione Top-K associata al nodo specificato.
func (a *TopKAggregator) SetTopKContribution(state *message.AggregationState, nodeID message.NodeID, values []float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	if len(sorted) > a.K {
		sorted = sorted[len(sorted)-a.K:]
	}
	state.Contributions[nodeID] = message.Contribution{
		TopK:    sorted,
		Version: version,
	}
}

// ComputeResult determina il risultato scalare complessivo estraendo il valore massimo dai Top-K globali.
func (a *TopKAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	topK := a.ComputeTopK(state, aliveNodes)
	if len(topK) == 0 {
		return 0
	}
	return topK[len(topK)-1]
}

// ComputeTopK estrae e restituisce in modo ordinato la lista completa dei Top-K globali.
func (a *TopKAggregator) ComputeTopK(state *message.AggregationState, aliveNodes map[message.NodeID]bool) []float64 {
	if state == nil || len(state.Contributions) == 0 {
		return nil
	}
	var all []float64
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		all = append(all, contrib.TopK...)
	}
	sort.Float64s(all)
	if len(all) > a.K {
		all = all[len(all)-a.K:]
	}
	return all
}
