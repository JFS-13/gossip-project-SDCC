package gossip

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"gossip-project/internal/aggregation"
	"gossip-project/internal/transport"
	"gossip-project/internal/types"
)

type LocalState struct {
	mu     sync.RWMutex
	Value  float64
	Weight float64 // Peso (essenziale per Push-Sum)
	Peers  []string
}

type Agent struct {
	NodeID     string
	State      *LocalState
	Transport  *transport.UDPTransport
	Aggregator aggregation.Aggregator
	Interval   time.Duration
}

// Aggiunto il parametro initialWeight (sarà sempre 1.0 all'avvio)
func NewAgent(nodeID string, initialValue float64, initialWeight float64, peers []string, t *transport.UDPTransport, agg aggregation.Aggregator, intervalMs int) *Agent {
	return &Agent{
		NodeID: nodeID,
		State: &LocalState{
			Value:  initialValue,
			Weight: initialWeight,
			Peers:  peers,
		},
		Transport:  t,
		Aggregator: agg,
		Interval:   time.Duration(intervalMs) * time.Millisecond,
	}
}

// GetEstimate restituisce il vero valore finale calcolato (V/W per la media, V per il Max)
func (a *Agent) GetEstimate() float64 {
	a.State.mu.RLock()
	defer a.State.mu.RUnlock()
	return a.Aggregator.GetResult(a.State.Value, a.State.Weight)
}

func (a *Agent) Start() {
	ticker := time.NewTicker(a.Interval)
	go func() {
		for {
			<-ticker.C
			a.executeRound()
		}
	}()
}

func (a *Agent) executeRound() {
	a.State.mu.Lock()
	if len(a.State.Peers) == 0 {
		a.State.mu.Unlock()
		return
	}

	randomPeer := a.State.Peers[rand.Intn(len(a.State.Peers))]
	var sendVal, sendWeight float64

	if a.Aggregator.Family() == aggregation.MassConservationFamily {
		// Dimezziamo per il Push-Sum
		a.State.Value /= 2.0
		a.State.Weight /= 2.0
		sendVal = a.State.Value
		sendWeight = a.State.Weight
	} else {
		sendVal = a.State.Value
		sendWeight = 1.0
	}
	a.State.mu.Unlock()

	msg := types.GossipMessage{
		SenderID:    a.NodeID,
		Type:        types.PushMessage,
		Value:       sendVal,
		Weight:      sendWeight,
		Aggregation: a.Aggregator.Name(),
	}

	// Proviamo a inviare...
	err := a.Transport.SendMessage(randomPeer, msg)
	if err != nil {
		fmt.Printf("⚠️ Errore invio a %s (Rete non pronta?). Ripristino della massa...\n", randomPeer)

		// REFUND: Se il messaggio non è partito, ci restituiamo la massa!
		if a.Aggregator.Family() == aggregation.MassConservationFamily {
			a.State.mu.Lock()
			a.State.Value += sendVal
			a.State.Weight += sendWeight
			a.State.mu.Unlock()
		}
	}
}

func (a *Agent) HandleMessage(msg types.GossipMessage, senderAddr string) {
	a.State.mu.Lock()
	oldEstimate := a.GetEstimateInternal() // Helper interno senza mutex

	// Deleghiamo la fusione matematica all'interfaccia!
	newV, newW := a.Aggregator.Aggregate(a.State.Value, a.State.Weight, msg.Value, msg.Weight)
	a.State.Value = newV
	a.State.Weight = newW

	newEstimate := a.GetEstimateInternal()
	a.State.mu.Unlock()

	// Log di debug
	fmt.Printf("📥 [%s] da %s. Stima aggiornata: %.4f -> %.4f\n", msg.Type, senderAddr, oldEstimate, newEstimate)

	// COMPORTAMENTO DINAMICO 2:
	// Il PULL si usa SOLO per le funzioni Idempotenti per velocizzare la convergenza.
	// In Push-Sum, il PULL è distruttivo, quindi lo vietiamo matematicamente.
	if msg.Type == types.PushMessage && a.Aggregator.Family() == aggregation.IdempotentFamily {
		replyMsg := types.GossipMessage{
			SenderID:    a.NodeID,
			Type:        types.PullMessage,
			Value:       newV,
			Weight:      newW,
			Aggregation: a.Aggregator.Name(),
		}
		a.Transport.SendMessage(senderAddr, replyMsg)
	}
}

// GetEstimateInternal è identica a GetEstimate ma senza bloccare il Mutex (usata dentro i lock)
func (a *Agent) GetEstimateInternal() float64 {
	return a.Aggregator.GetResult(a.State.Value, a.State.Weight)
}
