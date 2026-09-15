// Implementa un tool a riga di comando che interroga periodicamente
// gli endpoint /metrics dei nodi gossip e genera grafici di convergenza statici (PNG).
//
// Flags:
//
//	-auto       Avvia e spegne automaticamente docker-compose (default: false)
//	-delay      Latenza di rete in ms da iniettare se -auto è true (default: 500)
//	-nodes      Numero di nodi del cluster (default: 8)
//	-host       Hostname/IP base degli endpoint (default: "localhost")
//	-duration   Durata del campionamento in secondi (default: 5)
//	-interval   Intervallo di polling in millisecondi (default: 100)
//	-output     Cartella di destinazione dei grafici PNG (default: ".")
//
// Eseguire ad esempio go run scripts/plot_convergence.go -auto -delay 500
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"image/color"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// Modella la risposta JSON dell'endpoint /metrics.
type metricsResponse struct {
	NodeID          string `json:"node_id"`
	Round           uint64 `json:"round"`
	AllAggregations struct {
		Sum     float64   `json:"sum"`
		Average float64   `json:"average"`
		Min     float64   `json:"min"`
		Max     float64   `json:"max"`
		TopK    []float64 `json:"top_k"`
	} `json:"all_aggregations"`
}

// Rappresenta un singolo campione temporale raccolto da un nodo.
type sample struct {
	Elapsed float64 `json:"elapsed"`
	Sum     float64 `json:"sum"`
	Average float64 `json:"average"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`
}

// Definisce i colori assegnati ai nodi nel grafico.
var palette = []color.RGBA{
	{R: 31, G: 119, B: 180, A: 255},  // blu
	{R: 255, G: 127, B: 14, A: 255},  // arancione
	{R: 44, G: 160, B: 44, A: 255},   // verde
	{R: 214, G: 39, B: 40, A: 255},   // rosso
	{R: 148, G: 103, B: 189, A: 255}, // viola
	{R: 140, G: 86, B: 75, A: 255},   // marrone
	{R: 227, G: 119, B: 194, A: 255}, // rosa
	{R: 127, G: 127, B: 127, A: 255}, // grigio
}

func main() {
	nodes := flag.Int("nodes", 8, "numero di nodi del cluster")
	host := flag.String("host", "localhost", "hostname o IP dei nodi")
	duration := flag.Int("duration", 5, "durata campionamento in secondi dopo l'avvio")
	interval := flag.Int("interval", 100, "intervallo polling in millisecondi")
	output := flag.String("output", ".", "cartella output per le immagini PNG")
	auto := flag.Bool("auto", false, "gestisce automaticamente l'avvio e spegnimento di docker-compose")
	delay := flag.Int("delay", 500, "latenza di rete in ms da iniettare se -auto è true")
	flag.Parse()

	if *auto {
		log.Println("--- Modalità Automatica Attiva ---")
		log.Println("Pulisco eventuali container precedenti...")
		runCommand("docker-compose", "down", "-v")

		log.Printf("Avvio il cluster iniettando %dms di latenza (il polling è in ascolto)...", *delay)
		os.Setenv("NETWORK_DELAY_MS", fmt.Sprintf("%d", *delay))
	} else {
		log.Printf("Avvio campionamento manuale: %d nodi su %s, intervallo %dms", *nodes, *host, *interval)
	}

	samples := make([][]sample, *nodes)
	for i := range samples {
		samples[i] = []sample{}
	}

	stopPolling := make(chan struct{})
	pollingDone := make(chan struct{})

	// Avvia il polling in background ORA (dopo che i vecchi container sono stati distrutti)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		start := time.Now()
		tick := time.NewTicker(time.Duration(*interval) * time.Millisecond)
		defer tick.Stop()

		for {
			select {
			case <-stopPolling:
				close(pollingDone)
				return
			case now := <-tick.C:
				elapsed := now.Sub(start).Seconds()
				var wg sync.WaitGroup
				var mu sync.Mutex

				for i := 0; i < *nodes; i++ {
					wg.Add(1)
					go func(nodeIdx int) {
						defer wg.Done()
						port := 8001 + nodeIdx
						url := fmt.Sprintf("http://%s:%d/metrics", *host, port)

						// Timeout per ogni singola richiesta
						resp, err := client.Get(url)
						if err != nil {
							return
						}
						defer resp.Body.Close()

						var m metricsResponse
						if err := json.NewDecoder(resp.Body).Decode(&m); err == nil {
							mu.Lock()
							samples[nodeIdx] = append(samples[nodeIdx], sample{
								Elapsed: elapsed,
								Sum:     m.AllAggregations.Sum,
								Average: m.AllAggregations.Average,
								Min:     m.AllAggregations.Min,
								Max:     m.AllAggregations.Max,
							})
							mu.Unlock()
						}
					}(i)
				}
				if int(elapsed*10)%10 == 0 {
					log.Printf("  [DEBUG] t=%.1fs", elapsed)
				}
			}
		}
	}()

	if *auto {
		runCommand("docker-compose", "up", "-d")

		defer func() {
			log.Println("Spegnimento del cluster in corso...")
			runCommand("docker-compose", "down", "-v")
			log.Println("Cluster spento correttamente.")
		}()
	}

	log.Printf("Cluster avviato. Continuo il campionamento per altri %d secondi...", *duration)
	time.Sleep(time.Duration(*duration) * time.Second)

	log.Println("Campionamento terminato. Generazione grafici...")
	close(stopPolling)
	<-pollingDone

	// Generazione dei grafici per ciascuna funzione di aggregazione
	aggregations := []struct {
		name    string
		extract func(sample) float64
	}{
		{"sum", func(s sample) float64 { return s.Sum }},
		{"average", func(s sample) float64 { return s.Average }},
		{"min", func(s sample) float64 { return s.Min }},
		{"max", func(s sample) float64 { return s.Max }},
	}

	for _, agg := range aggregations {
		if err := generatePlot(samples, agg.name, agg.extract, *output, *nodes, *duration); err != nil {
			log.Printf("Errore generazione grafico %s: %v", agg.name, err)
		}
	}

	log.Println("Grafici generati con successo!")
}

// generatePlot crea e salva un grafico PNG per una specifica funzione di aggregazione.
func generatePlot(samples [][]sample, aggName string, extract func(sample) float64, outputDir string, numNodes int, maxDuration int) error {
	p := plot.New()
	p.Title.Text = fmt.Sprintf("Convergenza CRDT — %s", aggName)
	p.X.Label.Text = "Tempo (secondi)"
	p.Y.Label.Text = fmt.Sprintf("Valore %s", aggName)
	p.Legend.Top = true

	// Imposta l'asse X in modo fisso da 0 a maxDuration (es. 5 secondi)
	p.X.Min = 0
	p.X.Max = float64(maxDuration)

	// Trova il tempo del primissimo campione raccolto per normalizzare l'asse X
	minTime := -1.0
	for i := 0; i < numNodes; i++ {
		if len(samples[i]) > 0 {
			if minTime == -1.0 || samples[i][0].Elapsed < minTime {
				minTime = samples[i][0].Elapsed
			}
		}
	}

	for i := 0; i < numNodes; i++ {
		if len(samples[i]) == 0 {
			continue
		}

		pts := make(plotter.XYs, len(samples[i]))
		for j, s := range samples[i] {
			// Normalizziamo in modo che il primissimo campione sia esattamente a t=0
			normalizedTime := s.Elapsed - minTime
			pts[j].X = normalizedTime
			pts[j].Y = extract(s)
		}

		line, err := plotter.NewLine(pts)
		if err != nil {
			return fmt.Errorf("errore creazione linea nodo %d: %w", i+1, err)
		}

		line.Color = palette[i%len(palette)]
		line.Width = vg.Points(2)

		p.Add(line)
		p.Legend.Add(fmt.Sprintf("node-%d", i+1), line)
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("convergence_%s.png", aggName))
	if err := p.Save(10*vg.Inch, 5*vg.Inch, filename); err != nil {
		return fmt.Errorf("errore salvataggio %s: %w", filename, err)
	}

	log.Printf("  ✓ Salvato: %s", filename)
	return nil
}

// runCommand esegue un comando di sistema e ne gestisce l'output.
func runCommand(name string, arg ...string) {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Printf("[Warning] Errore esecuzione comando %s: %v", name, err)
	}
}

// ensureDir crea la cartella di output se non esiste.
func init() {
	// Verifica che i flag siano parsati prima dell'uso
	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			if arg == "-h" || arg == "--help" {
				return
			}
		}
	}
}
