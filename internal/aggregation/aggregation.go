package aggregation

import "math"

const (
	IdempotentFamily       = "IDEMPOTENT"
	MassConservationFamily = "MASS_CONSERVATION"
)

// Interfaccia aggiornata: ora l'aggregatore sa anche come estrarre il risultato finale!
type Aggregator interface {
	Name() string
	Family() string
	Aggregate(localVal, localWeight, recvVal, recvWeight float64) (float64, float64)
	GetResult(val, weight float64) float64 // <- NUOVO METODO
}

// === 1. MAX ===
type MaxAggregator struct{}

func (a MaxAggregator) Name() string   { return "MAX" }
func (a MaxAggregator) Family() string { return IdempotentFamily }
func (a MaxAggregator) Aggregate(localVal, localWeight, recvVal, recvWeight float64) (float64, float64) {
	return math.Max(localVal, recvVal), localWeight
}
func (a MaxAggregator) GetResult(val, weight float64) float64 { return val }

// === 2. MIN ===
type MinAggregator struct{}

func (a MinAggregator) Name() string   { return "MIN" }
func (a MinAggregator) Family() string { return IdempotentFamily }
func (a MinAggregator) Aggregate(localVal, localWeight, recvVal, recvWeight float64) (float64, float64) {
	return math.Min(localVal, recvVal), localWeight
}
func (a MinAggregator) GetResult(val, weight float64) float64 { return val }

// === 3. AVERAGE ===
type AverageAggregator struct{}

func (a AverageAggregator) Name() string   { return "AVERAGE" }
func (a AverageAggregator) Family() string { return MassConservationFamily }
func (a AverageAggregator) Aggregate(localVal, localWeight, recvVal, recvWeight float64) (float64, float64) {
	return localVal + recvVal, localWeight + recvWeight
}
func (a AverageAggregator) GetResult(val, weight float64) float64 {
	if weight > 0 {
		return val / weight
	}
	return val
}

// === 4. SUM (LA NOVITÀ) ===
type SumAggregator struct {
	TotalNodes int // La somma ha bisogno di sapere quanti nodi ci sono
}

func (a SumAggregator) Name() string   { return "SUM" }
func (a SumAggregator) Family() string { return MassConservationFamily }
func (a SumAggregator) Aggregate(localVal, localWeight, recvVal, recvWeight float64) (float64, float64) {
	// Sotto il cofano si comporta esattamente come la media (conserva la massa)
	return localVal + recvVal, localWeight + recvWeight
}
func (a SumAggregator) GetResult(val, weight float64) float64 {
	// Il trucco magico: Media * N
	if weight > 0 {
		return (val / weight) * float64(a.TotalNodes)
	}
	return val
}
