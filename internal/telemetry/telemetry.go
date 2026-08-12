// Package observability fornisce il server HTTP per health check e metriche,
// e il setup del logger strutturato.
// Separato da main.go per rispettare il principio di Single Responsibility.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// MetricsProvider fornisce i dati per le metriche.
// Implementato dall'Engine gossip nel package principale.
type MetricsProvider interface {
	GetEstimate() (float64, int)
	GetRound() uint64
	GetEpoch() int64
}

// TelemetryServer gestisce il server HTTP per health check e metriche.
type TelemetryServer struct {
	nodeID    string
	aggType   string
	startTime time.Time
	provider  MetricsProvider
	server    *http.Server
}

// NewTelemetryServer crea un nuovo server di telemetria.
func NewTelemetryServer(addr, nodeID, aggType string, provider MetricsProvider) *TelemetryServer {
	ts := &TelemetryServer{
		nodeID:    nodeID,
		aggType:   aggType,
		startTime: time.Now(),
		provider:  provider,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", ts.handleHealth)
	mux.HandleFunc("/metrics", ts.handleMetrics)

	ts.server = &http.Server{Addr: addr, Handler: mux}
	return ts
}

// Start avvia il server HTTP in una goroutine.
func (ts *TelemetryServer) Start() {
	go func() {
		slog.Info("server telemetria avviato", "addr", ts.server.Addr)
		if err := ts.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("server telemetria terminato", "error", err)
		}
	}()
}

// Shutdown esegue un graceful shutdown del server HTTP.
func (ts *TelemetryServer) Shutdown(ctx context.Context) error {
	return ts.server.Shutdown(ctx)
}

// handleHealth gestisce GET /health — restituisce 200 se il nodo è attivo.
func (ts *TelemetryServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","node_id":%q,"uptime_seconds":%.0f}`,
		ts.nodeID, time.Since(ts.startTime).Seconds())
}

// handleMetrics gestisce GET /metrics — restituisce stima corrente e metriche gossip.
func (ts *TelemetryServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	estimate, knownNodes := ts.provider.GetEstimate()
	round := ts.provider.GetRound()
	epoch := ts.provider.GetEpoch()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w,
		`{"node_id":%q,"aggregation":%q,"estimate":%.4f,"known_nodes":%d,"round":%d,"epoch":%d,"uptime_seconds":%.0f}`,
		ts.nodeID, ts.aggType, estimate, knownNodes, round, epoch, time.Since(ts.startTime).Seconds())
}

// SetupLogger configura il logger strutturato slog con output JSON.
func SetupLogger(level string) *slog.Logger {
	logLevel := slog.LevelInfo
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)
	return logger
}
