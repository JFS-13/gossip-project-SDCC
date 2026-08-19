package aggregation

import (
	"fmt"
	"gossip-project/internal/message"
)

// Aggregator definisce l'interfaccia per le logiche di aggregazione.
type Aggregator interface {
	Type() string
	SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64)
	ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64
}

// Factory istanzia e restituisce l'implementazione dell'aggregatore corrispondente al tipo indicato.
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
		return &TopKAggregator{K: 5}, nil
	default:
		return nil, fmt.Errorf("aggregazione non supportata: %s", kind)
	}
}

// NewTopK istanzia un aggregatore Top-K specificando la cardinalità K.
func NewTopK(k int) *TopKAggregator {
	if k < 1 {
		k = 5
	}
	return &TopKAggregator{K: k}
}
