package transport

import "context"

// MessageHandler gestisce payload ricevuti dal layer transport.
type MessageHandler func(ctx context.Context, payload []byte) error

// Transport definisce il contratto di invio/ricezione tra nodi gossip.
type Transport interface {
	Start(ctx context.Context, handler MessageHandler) error
	Send(ctx context.Context, address string, payload []byte) error
	Close() error
}

// NoopTransport è uno stub per test senza rete reale.
type NoopTransport struct{}

func (NoopTransport) Start(context.Context, MessageHandler) error { return nil }
func (NoopTransport) Send(context.Context, string, []byte) error  { return nil }
func (NoopTransport) Close() error                                { return nil }
