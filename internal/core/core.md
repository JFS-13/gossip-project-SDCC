# internal/core/core.go

Questo file contiene l'implementazione del "motore" (Engine) del protocollo Gossip. 
L'`Engine` è il componente centrale (il cuore battente) che si occupa di orchestrare l'intero ciclo di vita di un nodo all'interno della rete P2P: coordina l'invio periodico dei messaggi, la ricezione, l'unione degli stati e l'aggiornamento della membership.

## Interfacce (Righe 15-40)
Per garantire la testabilità (Dependency Injection) e separare le responsabilità, il file definisce tre interfacce chiave che verranno iniettate nell'Engine:
- **`Transport`**: Astrae il livello di rete (UDP o mock in memoria per i test). Consente di avviare il server in ascolto e inviare payload di byte.
- **`Aggregator`**: Definisce la firma per i calcoli matematici (Media, Somma, ecc.). 
- **`MembershipProvider`**: Fornisce i peer attuali e si occupa di fare il merge della lista dei nodi ricevuta da remoto. Include il metodo fondamentale `IncrementIncarnation()` usato come Heartbeat.

## La Struct `Engine` (Righe 42-56)
Contiene lo stato interno del motore. Tra i campi principali:
- `State`: Puntatore a `EngineState`, che contiene la mappa CRDT vera e propria.
- `seedPeers`: Utilizzati solo quando la membership è vuota per fare bootstrap.
- `interval` e `fanout`: Parametri che definiscono quanto spesso "spettegolare" e con quante persone alla volta.
- `stopCh`: Un channel per garantire uno shutdown asincrono pulito.

## Metodi Principali

### `Start(ctx)` (Riga 83)
Avvia due processi fondamentali in background:
1. Chiama `e.transport.Start()` passando la callback `handleMessage`, che verrà invocata automaticamente per ogni pacchetto UDP in ingresso.
2. Lancia una *goroutine* con un `time.Ticker` settato sull'intervallo di gossip (es. 1000ms). Ad ogni *tick*, viene invocato `executeRound(ctx)`.

### `executeRound(ctx)` (Riga 115)
È il ciclo epidemico vero e proprio ("Push phase"):
1. Incrementa il round logico locale.
2. Richiede alla `membership` un gruppo di peer casuali (limitato dal `fanout`).
3. Effettua il calcolo dell'Heartbeat (`e.membership.IncrementIncarnation()`), in modo che gli altri nodi sappiano che questo nodo è vivo.
4. "Scatta una foto" (Snapshot) del CRDT locale e della Membership, impacchettandoli in una struct `GossipMessage`.
5. Serializza tutto in JSON.
6. Invia il payload ai peer selezionati.
7. **Partition Healing & Bootstrap:** Se la lista di peer casuali è vuota (es. al boot), o nel 20% dei casi (tramite `rand.Float32() < 0.20`), spedisce il payload ai `seedPeers`. Questa tecnica, tipica dei sistemi industriali, previene scenari di "Split-Brain" assicurando che sottogruppi isolati si ricongiungano sempre al cluster principale.
8. Ricalcola immediatamente la stima locale aggiornata.

### `handleMessage(ctx, payload)` (Riga 174)
È la callback invocata dal layer di trasporto quando arriva un UDP ("Pull phase"):
1. Deserializza il JSON in un `GossipMessage`.
2. Invia lo stato ricevuto al CRDT locale chiamando `e.State.MergeRemote()`. Essendo un CRDT, l'ordine di ricezione non conta e i conflitti vengono risolti deterministicamente.
3. Passa la lista dei peer ricevuta al manager della membership (`MergeMembership`), in modo che il nodo possa "scoprire" per vie traverse nodi con cui non ha parlato direttamente (piggybacking).
4. Ricalcola la stima e, se ci sono state modifiche rilevanti, genera un log (ad es. "Nuova stima: 45.0").

### `AnnounceLeave(ctx)` (Riga 215)
Supporto vitale per il *Graceful Shutdown*.
Invece di terminare bruscamente il processo (che obbligherebbe gli altri nodi ad attendere svariati secondi per i timeout di Suspect e Dead), questo metodo crea un pacchetto speciale con `StatusLeave`. Viene inviato in broadcast a *tutti* i peer conosciuti (bypassando il limite del `fanout`).
Chi riceve questo pacchetto elimina immediatamente il nodo dall'aggregazione.
