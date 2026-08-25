# internal/topology/topology.go

Questo file contiene l'implementazione del **Manager della Membership** (Topology), ovvero il sottosistema che gestisce l'elenco dei peer conosciuti e si occupa di rilevare eventuali malfunzionamenti (Failure Detector). Il design si ispira ai protocolli epidemici come SWIM, ma senza un meccanismo esplicito di probing/ping (si basa sul piggybacking).

## Strutture Principali

### `Member` (Righe 21-29)
Rappresenta un singolo nodo all'interno della rete. Contiene:
- `NodeID` e `Addr`: Identificativo e indirizzo (ip:porta).
- `Status`: Lo stato attuale (`alive`, `suspect`, `dead`, `leave`).
- `Incarnation`: Il contatore di "Heartbeat". Più è alto, più l'informazione è recente.
- `LastSeen`: L'ora locale in cui il nodo è stato "visto" (o ne è stata ricevuta un'incarnazione aggiornata) per l'ultima volta.
- `DeadSince`: Registra l'istante in cui un nodo è stato dichiarato morto per la logica di *Tombstoning*.

### `Manager` (Righe 38-45)
È il contenitore *thread-safe* della membership. 
- Mantiene una mappa `members` di tutti i peer.
- `config` contiene i timeout di riferimento per il Failure Detection (`SuspectTimeout`, `DeadTimeout`, `CleanupTimeout`).
- Espone metodi concorrenti (`mu sync.RWMutex`) per aggiungere, aggiornare o recuperare peer casuali.

## Logiche Chiave e Metodi

### `IncrementIncarnation()` (Riga 68)
### `NewManager(...)` (Inizializzazione)
Un dettaglio industriale fondamentale risiede in come viene inizializzata l'`Incarnation` locale. Invece di partire da `0`, parte da `time.Now().UnixNano()`.
Questo risolve elegantemente il problema dell'**Incarnation Reset**: se un nodo crascia e viene riavviato (es. riavvio del container Docker), riparte con un timestamp astronomicamente superiore rispetto alla sua vita precedente. Gli altri nodi accetteranno immediatamente il suo ritorno nel cluster (rispettando rigorosamente le regole del CRDT senza necessitare di hack o bypass).

### `IncrementIncarnation()`
Ad ogni round gossip, il nodo chiama questo metodo su se stesso. L'Incarnation sale di 1. Questo funge da "Heartbeat": quando il dato viaggia per la rete e arriva agli altri nodi, essi vedranno che l'Incarnation è superiore a quella che avevano in memoria, aggiornando così il `LastSeen` e prevenendo la falsa morte del nodo.

### `GetRandomPeers(n int)` (Riga 132)
Filtra i nodi estraendo solo quelli `alive` o `suspect` (ignorando se stessi e i nodi morti/usciti). Dopodiché mescola l'array (`rand.Shuffle`) e ne estrae al massimo `n` (il *fanout*). Questo garantisce la natura stocastica ed epidemica del protocollo.

### `MergeMembership(entries []message.MembershipEntry)` (Riga 159)
Questo metodo unisce la tabella di routing locale con quella ricevuta da un peer remoto (Piggybacking). Le regole di risoluzione sono:
1. Se il nodo è sconosciuto, viene aggiunto (`alive`).
2. **Heartbeat vince**: Se l'entry ha una `Incarnation` maggiore di quella locale, si accetta il nuovo stato e si aggiorna `LastSeen = now`.
3. A parità di Incarnation, gli stati peggiorativi vincono: `suspect` prevale su `alive`, `dead` prevale su `suspect`, e `leave` è definitivo.

### `CheckTimeouts(now time.Time)` (Riga 221)
Eseguito periodicamente in background dal `main.go`. Itera su tutti i membri:
1. **Tombstone Cleanup**: Se un nodo è in stato `leave` o `dead`, controlla da quanto tempo lo è. Se supera il `CleanupTimeout`, viene fisicamente rimosso dalla mappa con `delete()`. Questo previene i memory leak e garantisce che un nodo sostituito o spento non rimanga nei registri in eterno.
2. **Failure Detector**: Calcola il delta temporale `now.Sub(LastSeen)`. 
   - Se supera `SuspectTimeout`, lo stato diventa `suspect` (probabile partizione o rete lenta).
   - Se supera `SuspectTimeout + DeadTimeout`, il nodo viene considerato perso e marcato come `dead` (registrando il `DeadSince` per il futuro Tombstone Cleanup).
