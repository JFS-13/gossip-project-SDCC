package aggregation

import (
	"fmt"
	"gossip-project/internal/message"
)

// Aggregator definisce il contratto per le aggregazioni supportate.
// Ogni aggregatore sa come:
//   - Impostare il contributo di un nodo (SetContribution)
//   - Calcolare il risultato dall'insieme dei contributi (ComputeResult)
type Aggregator interface {
	Type() string
	SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64)
	ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64
}

// Factory crea un'implementazione di Aggregator in base al tipo richiesto.
func Factory(kind string) (Aggregator, error) {
	switch kind {
	case "sum":
		return &SumAggregator{}, nil
	case "average":
		return &AverageAggregator{}, nil
	case "min":
		return &MinAggregator{}, nil
	case "max":
		return &MaxAggregator{}, nil
	case "topk":
		return &TopKAggregator{K: 5}, nil // Default K=5
	default:
		return nil, fmt.Errorf("aggregazione non supportata: %s", kind)
	}
}

// NewTopK creates a TopKAggregator with a specific K value.
func NewTopK(k int) *TopKAggregator {
	if k < 1 {
		k = 5
	}
	return &TopKAggregator{K: k}
}
