package transport

import "context"

// MessageHandler definisce la callback per i payload ricevuti dal livello transport.
type MessageHandler func(ctx context.Context, payload []byte) error

// Transport definisce l'interfaccia di comunicazione di rete per i nodi gossip.
type Transport interface {
	Start(ctx context.Context, handler MessageHandler) error
	Send(ctx context.Context, address string, payload []byte) error
	Close() error
}

// NoopTransport fornisce un'implementazione vuota di Transport per scopi di testing.
type NoopTransport struct{}

func (NoopTransport) Start(context.Context, MessageHandler) error { return nil }
func (NoopTransport) Send(context.Context, string, []byte) error  { return nil }
func (NoopTransport) Close() error                                { return nil }
