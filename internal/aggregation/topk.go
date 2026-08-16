package aggregation

import (
	"gossip-project/internal/message"
	"sort"
)

// TopKAggregator implementa l'aggregazione dei Top-K elementi con CRDT per-contributo.
//
// Ogni nodo contribuisce una lista ordinata dei suoi K valori più alti.
// Il risultato globale è: unisci tutti i contributi, ordina, prendi i top-K.
//
// Esempio con K=3 e 3 nodi:
//
//	node-1: [90, 85, 70]
//	node-2: [95, 80, 60]
//	node-3: [88, 75, 65]
//	Risultato globale: [95, 90, 88]  (top-3 di tutti i valori)
//
// Il merge CRDT (versione più alta per ogni nodeID) garantisce convergenza.
type TopKAggregator struct {
	K int
}

func (a *TopKAggregator) Type() string { return "topk" }

// SetContribution imposta il contributo Top-K per un nodo.
// Nota: usa il campo Value per passare un singolo valore. Per impostare
// una lista Top-K completa, usa SetTopKContribution direttamente.
func (a *TopKAggregator) SetContribution(state *message.AggregationState, nodeID message.NodeID, value float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
		// Aggiungi il nuovo valore alla lista Top-K esistente
		topk := append(curr.TopK, value)
		sort.Float64s(topk)
		// Tieni solo i K più alti (in ordine decrescente)
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

// SetTopKContribution imposta direttamente una lista Top-K per un nodo.
func (a *TopKAggregator) SetTopKContribution(state *message.AggregationState, nodeID message.NodeID, values []float64) {
	state.EnsureContributions()
	version := uint64(1)
	if curr, exists := state.Contributions[nodeID]; exists {
		version = curr.Version + 1
	}
	// Ordina e tieni solo i K più alti
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

// ComputeResult calcola il risultato Top-K globale.
// Unisce tutte le liste Top-K di ogni nodo, ordina e restituisce il valore
// più alto come risultato scalare. Per ottenere la lista completa dei Top-K,
// usare ComputeTopK.
func (a *TopKAggregator) ComputeResult(state *message.AggregationState, aliveNodes map[message.NodeID]bool) float64 {
	topK := a.ComputeTopK(state, aliveNodes)
	if len(topK) == 0 {
		return 0
	}
	// Restituisce il valore più alto
	return topK[len(topK)-1]
}

// ComputeTopK restituisce la lista completa dei Top-K globali ordinata.
func (a *TopKAggregator) ComputeTopK(state *message.AggregationState, aliveNodes map[message.NodeID]bool) []float64 {
	if state == nil || len(state.Contributions) == 0 {
		return nil
	}
	// Unisci tutti i valori Top-K di ogni nodo
	var all []float64
	for nodeID, contrib := range state.Contributions {
		if !aliveNodes[nodeID] {
			continue
		}
		all = append(all, contrib.TopK...)
	}
	// Ordina e prendi i Top-K globali
	sort.Float64s(all)
	if len(all) > a.K {
		all = all[len(all)-a.K:]
	}
	return all
}
