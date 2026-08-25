# internal/aggregation/aggregation.go

Questo file rappresenta il punto di ingresso per tutta la logica matematica del protocollo. Definisce il "contratto" che tutte le funzioni di aggregazione devono rispettare e fornisce i meccanismi per instanziarle dinamicamente.

## L'Interfaccia `Aggregator` (Righe 8-16)
Implementa il *Strategy Pattern*. Questo è fondamentale perché l'`Engine` gossip non deve sapere **come** si calcola una media o un minimo; gli basta avere un riferimento a questa interfaccia. 
Ogni aggregatore concreto (es. Average, Sum) deve saper fare tre cose:
1. `Type()`: Restituire il proprio nome (es. "average").
2. `SetContribution(...)`: Scrivere o inizializzare il contributo numerico per un dato nodo all'interno del CRDT (`message.AggregationState`).
3. `ComputeResult(...)`: Scorrere tutti i contributi presenti nello stato corrente e calcolare il valore matematico risultante. A partire dalla M10, questa funzione riceve dinamicamente in ingresso la mappa dei nodi vivi (`aliveNodes`), abilitando la **Membership-Aware Aggregation**: i contributi dei nodi morti vengono ignorati in tempo reale, rendendo il sistema estremamente fault-tolerant.

## La `Factory` (Righe 18-34)
Implementa il *Factory Pattern*. Viene invocata dal `main.go` durante l'avvio del nodo.
Riceve una stringa in input (letta dalla configurazione YAML o env) e restituisce l'istanza corretta dell'aggregatore.
Supporta nativamente:
- `sum`
- `average`
- `min`
- `max`
- `topk` (con un $K$ di default impostato a 5).

## Il Costruttore `NewTopK` (Righe 36-43)
Poiché l'aggregatore `topk` ha bisogno di un parametro aggiuntivo (la dimensione $K$ della classifica), questo metodo permette di creare un'istanza specificando il valore di $K$ dinamicamente (letto anch'esso dal file di configurazione). Se viene passato un valore non valido (es. $\le 0$), viene impostato un *fallback* ragionevole di $K=5$.
