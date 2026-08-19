package aggregation_test

import (
	"math"
	"testing"

	"gossip-project/internal/aggregation"
	"gossip-project/internal/message"
)

// --- Test per l'aggregatore SUM ---

func TestSumAggregator_Type(t *testing.T) {
	agg := &aggregation.SumAggregator{}
	if agg.Type() != "sum" {
		t.Errorf("atteso 'sum', ottenuto %q", agg.Type())
	}
}

// Verifica che il merge di più contributi calcoli correttamente la somma
func TestSumAggregator_Convergenza(t *testing.T) {
	agg := &aggregation.SumAggregator{}
	state := &message.AggregationState{Type: "sum"}

	agg.SetContribution(state, "node-1", 10.0)
	agg.SetContribution(state, "node-2", 30.0)
	agg.SetContribution(state, "node-3", 50.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-90.0) > 1e-9 {
		t.Errorf("SUM attesa 90.0, ottenuta %.4f", result)
	}
}

// Verifica l'idempotenza CRDT in caso di messaggi duplicati o vecchi
func TestSumAggregator_Idempotenza(t *testing.T) {
	agg := &aggregation.SumAggregator{}
	state := &message.AggregationState{Type: "sum"}

	agg.SetContribution(state, "node-1", 10.0)
	agg.SetContribution(state, "node-2", 30.0)

	remote := &message.AggregationState{Type: "sum"}
	agg.SetContribution(remote, "node-1", 10.0)
	state.MergeCRDT(remote)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-40.0) > 1e-9 {
		t.Errorf("SUM con duplicato: attesa 40.0, ottenuta %.4f", result)
	}
}

func TestSumAggregator_AggiornamentoContributo(t *testing.T) {
	agg := &aggregation.SumAggregator{}
	state := &message.AggregationState{Type: "sum"}

	agg.SetContribution(state, "node-1", 10.0)
	agg.SetContribution(state, "node-1", 20.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-20.0) > 1e-9 {
		t.Errorf("SUM dopo aggiornamento: attesa 20.0, ottenuta %.4f", result)
	}
}

// --- Test per l'aggregatore AVERAGE ---

func TestAverageAggregator_Type(t *testing.T) {
	agg := &aggregation.AverageAggregator{}
	if agg.Type() != "average" {
		t.Errorf("atteso 'average', ottenuto %q", agg.Type())
	}
}

func TestAverageAggregator_Convergenza(t *testing.T) {
	agg := &aggregation.AverageAggregator{}
	state := &message.AggregationState{Type: "average"}

	agg.SetContribution(state, "node-1", 10.0)
	agg.SetContribution(state, "node-2", 30.0)
	agg.SetContribution(state, "node-3", 50.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-30.0) > 1e-9 {
		t.Errorf("AVERAGE attesa 30.0, ottenuta %.4f", result)
	}
}

func TestAverageAggregator_NessunContributo(t *testing.T) {
	agg := &aggregation.AverageAggregator{}
	state := &message.AggregationState{Type: "average"}

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if result != 0 {
		t.Errorf("AVERAGE senza contributi: attesa 0.0, ottenuta %.4f", result)
	}
}

// --- Test per l'aggregatore MIN ---

func TestMinAggregator_Type(t *testing.T) {
	agg := &aggregation.MinAggregator{}
	if agg.Type() != "min" {
		t.Errorf("atteso 'min', ottenuto %q", agg.Type())
	}
}

func TestMinAggregator_Convergenza(t *testing.T) {
	agg := &aggregation.MinAggregator{}
	state := &message.AggregationState{Type: "min"}

	agg.SetContribution(state, "node-1", 42.0)
	agg.SetContribution(state, "node-2", 7.0)
	agg.SetContribution(state, "node-3", 100.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-7.0) > 1e-9 {
		t.Errorf("MIN attesa 7.0, ottenuta %.4f", result)
	}
}

func TestMinAggregator_ValoriNegativi(t *testing.T) {
	agg := &aggregation.MinAggregator{}
	state := &message.AggregationState{Type: "min"}

	agg.SetContribution(state, "node-1", -5.0)
	agg.SetContribution(state, "node-2", 3.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-(-5.0)) > 1e-9 {
		t.Errorf("MIN con negativi: attesa -5.0, ottenuta %.4f", result)
	}
}

// --- Test per l'aggregatore MAX ---

func TestMaxAggregator_Type(t *testing.T) {
	agg := &aggregation.MaxAggregator{}
	if agg.Type() != "max" {
		t.Errorf("atteso 'max', ottenuto %q", agg.Type())
	}
}

func TestMaxAggregator_Convergenza(t *testing.T) {
	agg := &aggregation.MaxAggregator{}
	state := &message.AggregationState{Type: "max"}

	agg.SetContribution(state, "node-1", 42.0)
	agg.SetContribution(state, "node-2", 7.0)
	agg.SetContribution(state, "node-3", 100.0)

	result := agg.ComputeResult(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-100.0) > 1e-9 {
		t.Errorf("MAX attesa 100.0, ottenuta %.4f", result)
	}
}

// --- Test per l'aggregatore TOP-K ---

func TestTopKAggregator_Type(t *testing.T) {
	agg := aggregation.NewTopK(3)
	if agg.Type() != "topk" {
		t.Errorf("atteso 'topk', ottenuto %q", agg.Type())
	}
}

// Verifica che il merge top-k conservi e ordini globalmente solo gli elementi corretti
func TestTopKAggregator_Convergenza(t *testing.T) {
	agg := aggregation.NewTopK(3)
	state := &message.AggregationState{Type: "topk"}

	agg.SetTopKContribution(state, "node-1", []float64{90.0, 85.0, 70.0})
	agg.SetTopKContribution(state, "node-2", []float64{95.0, 80.0, 60.0})
	agg.SetTopKContribution(state, "node-3", []float64{88.0, 75.0, 65.0})

	topK := agg.ComputeTopK(state, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})

	if len(topK) != 3 {
		t.Fatalf("TOP-K: attesa lista di 3 elementi, ottenuta lunghezza %d", len(topK))
	}
	if math.Abs(topK[2]-95.0) > 1e-9 {
		t.Errorf("TOP-K[0] atteso 95.0, ottenuto %.4f", topK[2])
	}
	if math.Abs(topK[1]-90.0) > 1e-9 {
		t.Errorf("TOP-K[1] atteso 90.0, ottenuto %.4f", topK[1])
	}
	if math.Abs(topK[0]-88.0) > 1e-9 {
		t.Errorf("TOP-K[2] atteso 88.0, ottenuto %.4f", topK[0])
	}
}

// --- Test della Factory e del Merge globale ---

// Assicura che unire un payload identico non generi loop infiniti o doppi conteggi
func TestMergeCRDT_Idempotenza(t *testing.T) {
	stateA := &message.AggregationState{Type: "sum"}
	agg := &aggregation.SumAggregator{}
	agg.SetContribution(stateA, "node-1", 10.0)

	changed := stateA.MergeCRDT(stateA)
	result := agg.ComputeResult(stateA, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-10.0) > 1e-9 {
		t.Errorf("idempotenza MergeCRDT: attesa 10.0, ottenuta %.4f (changed=%v)", result, changed)
	}
}

func TestMergeCRDT_PreferisceVersioneAlta(t *testing.T) {
	stateLocal := &message.AggregationState{Type: "sum"}
	stateRemote := &message.AggregationState{Type: "sum"}
	agg := &aggregation.SumAggregator{}

	agg.SetContribution(stateLocal, "node-1", 10.0)
	agg.SetContribution(stateRemote, "node-1", 10.0)
	agg.SetContribution(stateRemote, "node-1", 20.0)

	stateLocal.MergeCRDT(stateRemote)
	result := agg.ComputeResult(stateLocal, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-20.0) > 1e-9 {
		t.Errorf("MergeCRDT versione alta: attesa 20.0, ottenuta %.4f", result)
	}
}

func TestMergeCRDT_NuovoContributo(t *testing.T) {
	stateLocal := &message.AggregationState{Type: "sum"}
	stateRemote := &message.AggregationState{Type: "sum"}
	agg := &aggregation.SumAggregator{}

	agg.SetContribution(stateLocal, "node-1", 10.0)
	agg.SetContribution(stateRemote, "node-2", 30.0)

	stateLocal.MergeCRDT(stateRemote)
	result := agg.ComputeResult(stateLocal, map[message.NodeID]bool{"node-1": true, "node-2": true, "node-3": true})
	if math.Abs(result-40.0) > 1e-9 {
		t.Errorf("MergeCRDT nuovo nodo: attesa 40.0, ottenuta %.4f", result)
	}
}

func TestFactory_TipiSupportati(t *testing.T) {
	tipi := []string{"sum", "average", "min", "max"}
	for _, tipo := range tipi {
		agg, err := aggregation.Factory(tipo)
		if err != nil {
			t.Errorf("Factory(%q) errore inatteso: %v", tipo, err)
		}
		if agg.Type() != tipo {
			t.Errorf("Factory(%q): Type() restituisce %q", tipo, agg.Type())
		}
	}
}

func TestFactory_TipoNonSupportato(t *testing.T) {
	_, err := aggregation.Factory("invalid")
	if err == nil {
		t.Error("Factory('invalid') avrebbe dovuto restituire un errore")
	}
}
