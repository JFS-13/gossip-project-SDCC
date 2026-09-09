package aggregation

import "gossip-project/internal/message"

// CompositeAggregator calcola simultaneamente tutte e 5 le funzioni di aggregazione CRDT.
// Ogni nodo popola un'unica Contribution con tutti i campi necessari (Value, Sum, Count, TopK),
// permettendo a ciascun sotto-aggregatore di estrarre il proprio risultato dalla stessa mappa.
type CompositeAggregator struct {
	sum     *SumAggregator
	average *AverageAggregator
	min     *MinAggregator
	max     *MaxAggregator
	topK    *TopKAggregator
	primary string
}

// NewCompositeAggregator istanzia un aggregatore composito.
// Il parametro primary indica quale funzione viene considerata "principale".
func NewCompositeAggregator(primary string, topKSize int) *CompositeAggregator {
	return &CompositeAggregator{
		sum:     &SumAggregator{},
		average: &AverageAggregator{},
		min:     &MinAggregator{},
		max:     &MaxAggregator{},
		topK:    NewTopK(topKSize),
		primary: primary,
	}
}

func (c *CompositeAggregator) Type() string { return c.primary }

// SetContribution popola tutti i campi della Contribution per alimentare simultaneamente
// tutte le funzioni di aggregazione a partire da un singolo valore scalare.
func (c *CompositeAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	state.Contributions[nodeID] = message.Contribution{
		Value:   value,
		Sum:     value,
		Count:   1,
		TopK:    []float64{value},
		Version: version,
	}
}

// ComputeResult restituisce il risultato della funzione primaria configurata.
func (c *CompositeAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	switch c.primary {
	case "average":
		return c.average.ComputeResult(state, aliveNodes)
	case "min":
		return c.min.ComputeResult(state, aliveNodes)
	case "max":
		return c.max.ComputeResult(state, aliveNodes)
	case "topk":
		return c.topK.ComputeResult(state, aliveNodes)
	default:
		return c.sum.ComputeResult(state, aliveNodes)
	}
}

// ComputeAll calcola e restituisce simultaneamente i risultati di tutte e 5 le aggregazioni.
func (c *CompositeAggregator) ComputeAll(state *message.AggregationState, aliveNodes map[message.NodeID]bool) message.AllResults {
	return message.AllResults{
		Sum:     c.sum.ComputeResult(state, aliveNodes),
		Average: c.average.ComputeResult(state, aliveNodes),
		Min:     c.min.ComputeResult(state, aliveNodes),
		Max:     c.max.ComputeResult(state, aliveNodes),
		TopK:    c.topK.ComputeTopK(state, aliveNodes),
	}
}

// ComputeTopK restituisce la lista ordinata dei Top-K globali.
func (c *CompositeAggregator) ComputeTopK(state *message.AggregationState, aliveNodes map[message.NodeID]bool) []float64 {
	return c.topK.ComputeTopK(state, aliveNodes)
}
