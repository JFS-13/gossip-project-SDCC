package aggregation

// Aggregator definisce il contratto che ogni funzione di aggregazione deve rispettare.
// Questo ci permette di aggiungere nuove funzioni in futuro (es. Somma, Top-K) senza modificare il motore di Gossip.
type Aggregator interface {
	Aggregate(localValue float64, receivedValue float64) float64
	Name() string
}

// ==========================================
// IMPLEMENTAZIONE 1: AVERAGE (Media)
// ==========================================

type AverageAggregator struct{}

// Aggregate esegue il pair-wise averaging (Push-Pull)
func (a AverageAggregator) Aggregate(localValue float64, receivedValue float64) float64 {
	// Formula matematica per la conservazione della massa nella media
	return (localValue + receivedValue) / 2.0
}

func (a AverageAggregator) Name() string {
	return "AVERAGE"
}

// ==========================================
// IMPLEMENTAZIONE 2: MAX (Massimo)
// ==========================================

type MaxAggregator struct{}

// Aggregate tiene traccia del valore più alto visto nella rete
func (m MaxAggregator) Aggregate(localValue float64, receivedValue float64) float64 {
	if receivedValue > localValue {
		return receivedValue
	}
	return localValue
}

func (m MaxAggregator) Name() string {
	return "MAX"
}
