# internal/gossip/state.go

Questo file definisce `EngineState`, ovvero il contenitore *thread-safe* per i dati applicativi del nodo. 
Il suo scopo principale è memorizzare i contributi dell'aggregazione e isolare la complessità della concorrenza (tramite `sync.RWMutex`) rispetto al motore Gossip vero e proprio.

## La Struttura `EngineState` (Righe 10-20)
Contiene tutte le informazioni relative allo stato logico del protocollo:
- `NodeID`: L'identificativo univoco del nodo locale.
- `MyEpoch`: Un timestamp generato al boot (`time.Now().UnixNano()`). È fondamentale per la gestione dei crash: se un nodo si spegne improvvisamente e riparte, perderà la sua "versione" CRDT; grazie all'Epoch, i vecchi dati vengono sovrascritti immediatamente senza dover aspettare che la versione raggiunga i valori precedenti al crash.
- `Round`: Un contatore incrementato localmente ad ogni ciclo gossip (usato a scopi di log e metriche).
- `Aggregation`: Una struct di tipo `message.AggregationState` che funge da mappa CRDT vera e propria (associa un NodeID a una struct `Contribution`).
- `Estimate`: Il calcolo matematico finale aggiornato in tempo reale.

## Metodi Principali

### `NewEngineState(...)` (Riga 22)
È il costruttore. Inizializza lo stato generando immediatamente l'Epoch e popolando il dizionario CRDT con il proprio valore iniziale (invocando `UpdateLocalContribution`).

### `UpdateLocalContribution(value)` e `UpdateLocalTopK(values)` (Righe 39, 63)
Questi metodi permettono al nodo di aggiornare il proprio contributo all'interno del CRDT. 
L'operazione chiave che svolgono è **l'incremento della Versione**:
1. Cercano nella mappa se esiste già un contributo per il `NodeID` locale.
2. Se esiste, prendono la `Version` attuale e aggiungono 1.
3. Salvano la nuova `Contribution` (che contiene il nuovo valore, la nuova versione e il `MyEpoch`).

### `MergeRemote(remote *message.AggregationState)` (Riga 82)
Questo metodo è un semplice wrapper thread-safe. Quando il layer di rete riceve un pacchetto UDP, il payload remoto viene passato qui. Il metodo acquisisce il lock esclusivo e delega il merge vero e proprio al metodo `MergeCRDT` interno al `message.AggregationState`. Restituisce un booleano che indica se lo stato locale è stato effettivamente alterato o meno.

### `Snapshot()` (Riga 90)
Garantisce la "Deep Copy" dello stato (essenziale prima di spedirlo via rete per evitare data race). 
Acquisisce un lock in lettura (`RLock`), itera sulla mappa delle *Contributions* e ne fa una copia esatta. Presta un'attenzione particolare al caso della `TopK`, allocando fisicamente un nuovo slice in memoria con `copy()` per evitare che lo slice inviato condivida i puntatori con lo stato interno protetto.

### Metodi Getter e Setter
I metodi `GetEstimate`, `SetEstimate`, `GetRound`, e `IncrementRound` racchiudono le classiche logiche di accesso concorrente (usando `RLock` per la lettura e `Lock` per la scrittura), nascondendo la complessità dei Mutex a chi chiama queste funzioni (in particolar modo il server HTTP delle metriche, che interroga lo stato in modo asincrono).
