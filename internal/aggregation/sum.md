# internal/aggregation/sum.go

Questo file contiene l'implementazione dell'aggregatore per il calcolo della **Somma (Sum)** globale distribuita.

Come per la Media, calcolare la somma in una rete P2P non strutturata pone una sfida immensa: evitare che il valore di uno stesso nodo venga sommato più volte (il problema del *Double Counting*).

## La Soluzione Adottata: CRDT Per-Contributo

La struct `SumAggregator` risolve il problema usando lo stesso meccanismo `State-based CRDT` basato su dizionario utilizzato dalla media.

### `SetContribution(...)` (Riga 16)
Quando il nodo locale imposta il suo valore, crea un oggetto `Contribution` in cui salva il proprio valore numerico (`Value`) e genera una `Version` monotona.
Questo garantisce che nel calcolo finale il contributo del `nodeID` sarà calcolato sempre **una volta sola**, ed eventuali aggiornamenti del valore (rappresentati da versioni superiori) sovrascriveranno in modo deterministico il vecchio valore registrato ovunque nella rete.

### `ComputeResult(...)` (Riga 28)
Calcola la somma globale finale iterando sulla mappa del CRDT. 
Dato che il `MergeRemote` nell'engine si è già assicurato che la mappa contenga le versioni più aggiornate di ciascun nodo, a questo metodo basta fare un puro accumulatore lineare:
$$ \text{Sum} = \sum_{i=1}^{n} \text{Value}_i $$
Se la mappa è vuota (stato iniziale), ritorna semplicemente `0` per evitare panic.
