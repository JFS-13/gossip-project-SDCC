# main.go

Questo file rappresenta l'entry point (il punto d'ingresso) principale per l'applicazione nodo del protocollo Gossip.
Segue i principi dello Standard Go Project Layout, occupandosi esclusivamente di fare *wiring* (collegamento e iniezione delle dipendenze) e di gestire il ciclo di vita (startup e shutdown) del processo, delegando la vera business logic ai pacchetti interni.

## Struttura e Flusso di Esecuzione

### 1. Inizializzazione e Configurazione (Righe 24-32)
Il programma usa il pacchetto standard `flag` per leggere gli argomenti a riga di comando (es. `--config`).
La configurazione viene poi caricata tramite `setup.LoadConfig()`, che si occupa di parsare il file YAML ed eventualmente applicare override tramite variabili d'ambiente.

### 2. Setup del Logger (Righe 34-43)
Viene inizializzato il logger strutturato chiamando `telemetry.SetupLogger()`. Questo garantisce che da questo punto in poi tutti i log siano in formato JSON (ottimo per sistemi di monitoraggio come ELK o Loki) e filtrati secondo il `log_level` desiderato.

### 3. Bootstrap della Membership (Righe 45-62)
Viene istanziato il `topology.Manager`. 
Qui vengono definiti i timeout critici per il Failure Detector (es. `SuspectTimeout`, `DeadTimeout`, `CleanupTimeout`). Vengono anche passati i `SeedPeers`, fondamentali per il bootstrap iniziale: se un nodo non conosce nessuno, inizierà a inviare pacchetti a questi seed per farsi scoprire.

### 4. Setup dell'Aggregatore (Righe 63-72)
Sulla base del parametro `AggregationType` passato nella configurazione (es. "average", "sum", "topk"), viene invocata la `aggregation.Factory` che istanzia la strategia corretta (implementazione del pattern *Strategy*).

### 5. Inizializzazione Stato CRDT (Righe 74-90)
Viene creato l'`EngineState`, il contenitore thread-safe che manterrà lo stato aggregato. 
Al nodo viene assegnato il suo `InitialValue` tramite il metodo `SetContribution`, che popola la mappa CRDT locale.

### 6. Layer di Trasporto (Righe 92-99)
Si apre il socket UDP (`transport.NewUDPTransport`), che sarà usato dal nodo per ricevere e inviare i payload JSON (i `GossipMessage`).

### 7. Creazione e Avvio dell'Engine (Righe 101-127)
Tutte le dipendenze costruite finora (Stato, Transport, Aggregator, Membership) vengono iniettate nel motore gossip: `core.NewEngine`. 
L'Engine viene quindi avviato in background `eng.Start(ctx)`.

### 8. Background Workers: Timeout e Stampa (Righe 129-164)
Vengono lanciate due goroutine fondamentali:
- **Timeout Checker**: Ogni metà del `SuspectTimeout`, invoca `mset.CheckTimeouts(now)`. Questo loop analizza lo stato dei membri e dichiara `suspect` o `dead` chi non invia heartbeat da troppo tempo, occupandosi anche del "Tombstoning" (rimozione fisica dalla memoria dopo il `CleanupTimeout`).
- **Printer**: Ogni 2 secondi stampa sul logger lo stato della stima corrente (`estimate`), utile per l'osservabilità via console.

### 9. Avvio Telemetria HTTP (Righe 166-169)
Viene istanziato e avviato in modo asincrono un server HTTP minimale su `NodePort + 1000`. Questo server espone l'endpoint `/metrics`, essenziale per monitorare la salute e l'uptime del cluster dall'esterno (utilizzato pesantemente nel deployment su AWS EC2).

### 10. Graceful Shutdown (Righe 171-197)
Il main thread si mette in attesa bloccandosi su `<-ctx.Done()` (in attesa di segnali di interruzione come Ctrl+C).
Quando riceve il segnale, esegue uno shutdown controllato:
- Chiama `eng.AnnounceLeave(leaveCtx)`, che spedisce un pacchetto di addio a tutti i peer per avvisarli che l'uscita è volontaria (evitando che attivino i timeout di sospetto).
- Chiude il server di telemetria.
- Ferma l'Engine e chiude il transport UDP.
