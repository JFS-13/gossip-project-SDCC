package transport

import (
	"encoding/json"
	"fmt"
	"net"

	"gossip-project/internal/types"
)

// UDPTransport gestisce la comunicazione di rete per il nodo
type UDPTransport struct {
	conn *net.UDPConn
}

// NewUDPTransport crea una nuova connessione UDP in ascolto sull'indirizzo specificato
func NewUDPTransport(listenAddr string) (*UDPTransport, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("impossibile risolvere l'indirizzo: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("impossibile avviare il server UDP: %v", err)
	}

	return &UDPTransport{
		conn: conn,
	}, nil
}

// SendMessage invia un GossipMessage a un indirizzo di destinazione
func (t *UDPTransport) SendMessage(destAddr string, msg types.GossipMessage) error {
	addr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		return err
	}

	// Serializziamo il messaggio in formato JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Inviamo i byte tramite UDP
	_, err = t.conn.WriteToUDP(data, addr)
	return err
}

// ReceiveMessage resta in ascolto e blocca finché non riceve un nuovo messaggio
func (t *UDPTransport) ReceiveMessage() (types.GossipMessage, string, error) {
	buffer := make([]byte, 1024) // Buffer di 1KB (ampiamente sufficiente per il nostro messaggio)

	n, remoteAddr, err := t.conn.ReadFromUDP(buffer)
	if err != nil {
		return types.GossipMessage{}, "", err
	}

	var msg types.GossipMessage
	// Deserializziamo il JSON ricevuto per ricostruire la struct Go
	err = json.Unmarshal(buffer[:n], &msg)
	if err != nil {
		return types.GossipMessage{}, "", err
	}

	return msg, remoteAddr.String(), nil
}

// Close chiude la connessione di rete
func (t *UDPTransport) Close() error {
	return t.conn.Close()
}
