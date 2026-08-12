package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// UDPTransport implementa l'interfaccia Transport utilizzando UDP.
type UDPTransport struct {
	listenAddr string
	conn       *net.UDPConn
	handler    MessageHandler
	mu         sync.RWMutex
	closed     bool
	started    bool
	closeOnce  sync.Once
	done       chan struct{}
	wg         sync.WaitGroup
}

// NewUDPTransport crea una nuova istanza di UDPTransport.
func NewUDPTransport(listenAddr string) (*UDPTransport, error) {
	if listenAddr == "" {
		return nil, errors.New("indirizzo di ascolto non valido")
	}
	return &UDPTransport{
		listenAddr: listenAddr,
		done:       make(chan struct{}),
	}, nil
}

// Start avvia l'ascolto su UDP e il loop di lettura.
func (t *UDPTransport) Start(ctx context.Context, handler MessageHandler) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started {
		return errors.New("transport già avviato")
	}
	if t.closed {
		return errors.New("transport chiuso")
	}

	addr, err := net.ResolveUDPAddr("udp", t.listenAddr)
	if err != nil {
		return fmt.Errorf("risoluzione indirizzo fallita: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen UDP fallito: %w", err)
	}

	t.conn = conn
	t.handler = handler
	t.started = true

	t.wg.Add(1)
	go t.readLoop(ctx)

	return nil
}

// Send invia un payload tramite il socket UDP connesso o un dial fallback.
func (t *UDPTransport) Send(ctx context.Context, address string, payload []byte) error {
	t.mu.RLock()
	closed := t.closed
	conn := t.conn
	t.mu.RUnlock()

	if closed {
		return errors.New("transport chiuso")
	}

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	if conn != nil {
		_, err = conn.WriteToUDP(payload, addr)
		return err
	}

	// Fallback se il transport non è ancora avviato in ascolto
	dialConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	defer dialConn.Close()

	_, err = dialConn.Write(payload)
	return err
}

// Close chiude la connessione UDP e ferma il loop di lettura in modo idempotente.
func (t *UDPTransport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		close(t.done)
		if t.conn != nil {
			err = t.conn.Close()
		}
		t.mu.Unlock()
		t.wg.Wait()
	})
	return err
}

// readLoop è il ciclo in background per la lettura dei messaggi UDP in arrivo.
func (t *UDPTransport) readLoop(ctx context.Context) {
	defer t.wg.Done()
	buffer := make([]byte, 65535)

	for {
		select {
		case <-t.done:
			return
		case <-ctx.Done():
			return
		default:
		}

		t.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		n, _, err := t.conn.ReadFromUDP(buffer)

		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			// Se il socket è chiuso esce dal loop
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}

		if t.handler != nil && n > 0 {
			// Crea una copia del buffer per non avere data race se l'handler è asincrono
			payload := make([]byte, n)
			copy(payload, buffer[:n])
			_ = t.handler(ctx, payload)
		}
	}
}
