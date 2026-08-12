package setup

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config rappresenta la configurazione del nodo gossip.
type Config struct {
	NodeID              string   `yaml:"node_id"`
	BindAddress         string   `yaml:"bind_address"`
	AdvertiseAddr       string   `yaml:"advertise_addr"`
	NodePort            int      `yaml:"node_port"`
	SeedPeers           []string `yaml:"seed_peers"`
	GossipIntervalMs    int      `yaml:"gossip_interval_ms"`
	Fanout              int      `yaml:"fanout"`
	MembershipTimeoutMs int      `yaml:"membership_timeout_ms"`
	CleanupTimeoutMs    int      `yaml:"cleanup_timeout_ms"`
	AggregationType     string   `yaml:"aggregation_type"`
	InitialValue        float64  `yaml:"initial_value"`
	TopKSize            int      `yaml:"top_k_size"`
	LogLevel            string   `yaml:"log_level"`
}

// Default restituisce una configurazione con valori di default ragionevoli.
func Default() Config {
	return Config{
		NodeID:              "node-default",
		BindAddress:         "0.0.0.0",
		AdvertiseAddr:       "",
		NodePort:            8080,
		SeedPeers:           []string{},
		GossipIntervalMs:    1000,
		Fanout:              3,
		MembershipTimeoutMs: 5000,
		CleanupTimeoutMs:    300000, // 5 minuti default
		AggregationType:     "sum",
		InitialValue:        0.0,
		TopKSize:            5,
		LogLevel:            "info",
	}
}

// LoadConfig carica la configurazione da un file YAML e applica eventuali override dalle variabili d'ambiente.
func LoadConfig(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("errore nella lettura del file di configurazione: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("errore nel parsing del file YAML: %w", err)
		}
	}

	applyEnvOverrides(&cfg)

	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("configurazione non valida: %w", err)
	}

	return &cfg, nil
}

// applyEnvOverrides sovrascrive i campi con le variabili d'ambiente corrispondenti, se presenti.
func applyEnvOverrides(cfg *Config) {
	if val := os.Getenv("NODE_ID"); val != "" {
		cfg.NodeID = val
	}
	if val := os.Getenv("BIND_ADDRESS"); val != "" {
		cfg.BindAddress = val
	}
	if val := os.Getenv("ADVERTISE_ADDR"); val != "" {
		cfg.AdvertiseAddr = val
	}
	if val := os.Getenv("NODE_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil {
			cfg.NodePort = p
		}
	}
	if val := os.Getenv("SEED_PEERS"); val != "" {
		cfg.SeedPeers = strings.Split(val, ",")
	}
	if val := os.Getenv("GOSSIP_INTERVAL_MS"); val != "" {
		if g, err := strconv.Atoi(val); err == nil {
			cfg.GossipIntervalMs = g
		}
	}
	if val := os.Getenv("FANOUT"); val != "" {
		if f, err := strconv.Atoi(val); err == nil {
			cfg.Fanout = f
		}
	}
	if val := os.Getenv("MEMBERSHIP_TIMEOUT_MS"); val != "" {
		if m, err := strconv.Atoi(val); err == nil {
			cfg.MembershipTimeoutMs = m
		}
	}
	if val := os.Getenv("AGGREGATION_TYPE"); val != "" {
		cfg.AggregationType = val
	}
	if val := os.Getenv("INITIAL_VALUE"); val != "" {
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			cfg.InitialValue = v
		}
	}
	if val := os.Getenv("TOP_K_SIZE"); val != "" {
		if t, err := strconv.Atoi(val); err == nil {
			cfg.TopKSize = t
		}
	}
	if val := os.Getenv("CLEANUP_TIMEOUT_MS"); val != "" {
		if c, err := strconv.Atoi(val); err == nil {
			cfg.CleanupTimeoutMs = c
		}
	}
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		cfg.LogLevel = val
	}
}

// Validate controlla che i campi della configurazione siano validi.
func Validate(cfg *Config) error {
	if cfg.NodeID == "" {
		return errors.New("node_id non può essere vuoto")
	}
	if cfg.NodePort < 1 || cfg.NodePort > 65535 {
		return errors.New("node_port deve essere compreso tra 1 e 65535")
	}
	if cfg.GossipIntervalMs <= 0 {
		return errors.New("gossip_interval_ms deve essere maggiore di 0")
	}
	if cfg.Fanout <= 0 {
		return errors.New("fanout deve essere maggiore di 0")
	}
	if cfg.MembershipTimeoutMs <= 0 {
		return errors.New("membership_timeout_ms deve essere maggiore di 0")
	}
	if cfg.TopKSize <= 0 {
		return errors.New("top_k_size deve essere maggiore di 0")
	}
	return nil
}

// AdvertiseEndpoint restituisce l'indirizzo da pubblicizzare nel cluster.
func (c *Config) AdvertiseEndpoint() string {
	if c.AdvertiseAddr != "" {
		return fmt.Sprintf("%s:%d", c.AdvertiseAddr, c.NodePort)
	}
	return fmt.Sprintf("%s:%d", c.BindAddress, c.NodePort)
}
