package types

// MessageType definisce se il messaggio è di andata (Push) o di risposta (Pull)
type MessageType string

const (
	PushMessage MessageType = "PUSH"
	PullMessage MessageType = "PULL"
)

// GossipMessage rappresenta il pacchetto scambiato tra due nodi
type GossipMessage struct {
	SenderID    string      `json:"sender_id"`   // Identificativo di chi invia il messaggio (es. "node-1")
	Type        MessageType `json:"type"`        // PushMessage o PullMessage
	Value       float64     `json:"value"`       // Il valore corrente dell'aggregato (es. la media locale)
	Weight      float64     `json:"weight"`      // Il "peso" per la conservazione della massa
	Aggregation string      `json:"aggregation"` // Quale operazione stiamo facendo (es. "AVERAGE" o "MAX")
}
