# internal/aggregation/topk.go

Questo file contiene un'aggregazione avanzata: il calcolo dei **Top-K** elementi.
Mentre Sum o Average restituiscono uno scalare, questa funzione calcola la "classifica" dei $K$ valori più grandi presenti nell'intera rete.

## Come funziona il Top-K decentralizzato?
Se si vuole trovare, ad esempio, i 3 valori più alti registrati in un cluster di 100 nodi, ogni nodo locale non manda un singolo valore, ma mantiene un array dei *suoi* 3 valori più alti (`TopK []float64`).
Quando l'`Engine` unisce le tabelle (MergeRemote), finisce per possedere l'array *TopK* di tutti i nodi della rete.
L'aggregazione finale consiste nell'unire tutti questi array, ordinarli, e prendere solo i primi $K$ assoluti.

## Metodi Principali

### `SetContribution(...)` (Riga 26)
Questo metodo gestisce l'aggiornamento dinamico di un valore. Se il sensore di un nodo registra una nuova lettura (`value`), il metodo la appende (`append`) all'array `TopK` esistente per quel nodo, riordina l'array con `sort.Float64s()`, e **tronca** la lista scartando i valori più bassi se la dimensione supera la capienza massima stabilita `$K$`. Incrementa quindi la `Version`.

### `SetTopKContribution(...)` (Riga 53)
Metodo ausiliario, utile principalmente in fase di inizializzazione o per test, che permette di sovrascrivere l'intera lista in un colpo solo, occupandosi di fare *sort* e *slice* prima di salvarla nel dizionario CRDT locale.

### `ComputeResult(...)` e `ComputeTopK(...)` (Righe 73, 86)
Poiché l'interfaccia `Aggregator` prevede che `ComputeResult` ritorni uno scalare (un singolo `float64`), questo metodo è stato implementato per restituire solo **il valore assoluto più alto** in assoluto della classifica (l'ultimo elemento dell'array sortato).
Per stampare la classifica completa (utile ad es. nei log di sistema o per debug), viene esposto il metodo nativo `ComputeTopK`, che concatena matematicamente tutti gli slice, li ordina, ed estrae i top $K$ globali restituendo lo slice `[]float64`.
