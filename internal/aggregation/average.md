# internal/aggregation/average.go

Questo file contiene l'implementazione dell'aggregatore per il calcolo della **Media (Average)**.
Nel contesto dei sistemi distribuiti basati su Gossip, calcolare la media è storicamente una delle operazioni più complesse, perché bisogna garantire che il valore di un nodo non venga sommato due o più volte a causa dei cicli nella rete (il problema del *Double Counting*).

## La Soluzione Adottata: CRDT Per-Contributo

Invece di usare protocolli Push-Sum classici basati su mass-conservation (che sono sensibili alla perdita di messaggi UDP), abbiamo scelto una soluzione molto più robusta e moderna basata su **State-based CRDT** (CvRDT).

La struct `AverageAggregator` implementa l'interfaccia dell'Aggregator:

### `SetContribution(...)` (Riga 16)
Quando un nodo imposta il proprio valore, non salva semplicemente il numero puro, ma crea una struct `Contribution` contenente una coppia di valori:
- `Sum`: Il valore numerico.
- `Count`: Il "peso", impostato rigidamente a `1`.

Viene anche incrementata la `Version`. Grazie a questa versione, quando i nodi si scambiano le mappe CRDT, sanno sempre riconoscere se una coppia (Sum, Count) è obsoleta, assicurandosi che il contributo di uno specifico `NodeID` esista **una e una sola volta** nella mappa globale, annientando del tutto il problema del *Double Counting*.

### `ComputeResult(...)` (Riga 29)
Calcola la stima finale scorrendo i valori attualmente registrati nella mappa CRDT convergente.
La matematica sottostante è banalissima, ma potente:
$$ \text{Average} = \frac{\sum_{i=1}^{n} \text{Sum}_i}{\sum_{i=1}^{n} \text{Count}_i} $$
Il metodo somma separatamente i contatori `Sum` (accumulatore dei valori) e i contatori `Count` (accumulatore del numero di nodi effettivi), e restituisce la divisione finale. 
Se non ci sono contributi (es. `totalCount == 0`), restituisce prudentemente `0` per evitare il panico per divisione per zero.
