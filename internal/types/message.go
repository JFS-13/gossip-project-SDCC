package types

// MessageType definisce se il messaggio è di andata (Push) o di risposta (Pull)
type MessageType int

const (
	PushMessage MessageType = iota
	PullMessage
)

// GossipMessage rappresenta il pacchetto scambiato tra due nodi
type GossipMessage struct {
	SenderID    string      // Identificativo di chi invia il messaggio (es. "node-1")
	Type        MessageType // PushMessage o PullMessage
	Value       float64     // Il valore corrente dell'aggregato (es. la media locale)
	Aggregation string      // Quale operazione stiamo facendo (es. "AVERAGE" o "MAX")
}
