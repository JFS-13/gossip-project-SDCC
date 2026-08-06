package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config rappresenta i parametri configurabili del nodo
type Config struct {
	NodeID           string   `yaml:"node_id"`
	ListenAddress    string   `yaml:"listen_address"`
	GossipIntervalMs int      `yaml:"gossip_interval_ms"`
	AggregationType  string   `yaml:"aggregation_type"`
	Peers            []string `yaml:"peers"`
}

// LoadConfig legge un file YAML dal percorso specificato e restituisce la struttura Config
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
