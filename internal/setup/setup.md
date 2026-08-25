# internal/config/config.go

Questo file definisce e gestisce le impostazioni globali del nodo gossip, garantendo che ogni parametro critico sia personalizzabile all'avvio. Rispetta rigorosamente i pattern delle architetture Cloud-Native (ad esempio le 12-Factor Apps), separando il codice dalla configurazione.

## La Struct `Config` (Righe 13-28)
Contiene i campi mappati direttamente sui file YAML:
- Parametri di Rete: `BindAddress`, `AdvertiseAddr`, `NodePort`, `SeedPeers`.
- Parametri Gossip: `GossipIntervalMs`, `Fanout`.
- Parametri Membership: `MembershipTimeoutMs`, `CleanupTimeoutMs`.
- Parametri Aggregazione: `AggregationType`, `InitialValue`, `TopKSize`.

## Logica di Inizializzazione (Righe 49-70)
Il processo di caricamento, guidato da `LoadConfig()`, segue una precisa catena di fallback (gerarchia di precedenza):
1. **Valori di Default**: Viene invocato `Default()` (Riga 30) per garantire che il nodo possa partire anche se non viene passato alcun parametro (evitando crash per configurazioni omesse).
2. **File YAML**: Se viene fornito un path `--config`, la libreria `yaml.v3` effettua l'unmarshalling del file, sovrascrivendo i valori di default.
3. **Variabili d'Ambiente (Environment Variables)**: Viene chiamato `applyEnvOverrides()`. Questo passaggio è fondamentale per gli ambienti containerizzati (come Docker Compose o Kubernetes). Qualsiasi variabile d'ambiente (es. `NODE_ID="node-1"`) sovrascrive sia i default che lo YAML.

## Validazione (Righe 129-150)
`Validate()` si assicura che il nodo non parta in uno stato incoerente. Ad esempio, previene l'avvio se la porta specificata è fuori range, o se i timeout per il failure detector o l'intervallo gossip sono nulli/negativi.

## `AdvertiseEndpoint` (Riga 152)
Un metodo *utility* fondamentale per le reti Docker/Cloud. Un nodo potrebbe mettersi in ascolto internamente su `0.0.0.0` (il `BindAddress`), ma dover comunicare al resto della rete un IP o un DNS pubblico (l'`AdvertiseAddr`) con cui deve essere contattato. Se l'AdvertiseAddr non viene esplicitato, assume lo stesso valore del BindAddress.
