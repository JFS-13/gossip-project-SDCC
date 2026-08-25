# tests/aggregation/aggregation_test.go

Questo file contiene la suite di test unitari (Unit Test) dedicata esclusivamente al core matematico e all'implementazione dei CRDT (Conflict-free Replicated Data Types) per ogni tipologia di aggregazione.
A differenza dei test di integrazione, questi test sono deterministici, istantanei e non usano alcun motore gossip o rete (mock o reale).

## Logica dei Test
La struttura del file è suddivisa logicamente per tipologia di aggregazione, coprendo in maniera esaustiva tutti i corner case matematici:

### 1. Test SUM
Verifica che la somma iterata funzioni. Ma, soprattutto, il `TestSumAggregator_Idempotenza` certifica la correttezza del CRDT: simulando l'arrivo di due pacchetti identici via rete, lo stato finale non viene alterato (il valore 10.0 non diventa 20.0, prevenendo il "Double Counting"). Il `TestSumAggregator_AggiornamentoContributo` valida che un nodo possa legittimamente correggere al rialzo il proprio valore semplicemente inviandolo con una *Version* superiore.

### 2. Test AVERAGE
Verifica che la divisione tra i contatori "Sum" e "Count" ritorni la media aritmetica esatta. Il `TestAverageAggregator_NessunContributo` assicura che il sistema gestisca in totale sicurezza la divisione per zero restituendo un fallback `0.0`.

### 3. Test MIN e MAX
Controllano i casi limite di convergenza per la ricerca degli estremi, testando specificatamente la resilienza dell'algoritmo al passaggio di **Valori Negativi** (`-5.0`), che in passato poteva rompere algoritmi basati su inizializzazioni a zero invece che a $\pm\infty$.

### 4. Test TOP-K
Simula l'inserimento di 3 liste separate di valori top. Verifica che la funzione riesca a concatenarle in un'unica classifica e, sopratutto, che la troncata avvenga correttamente espellendo i numeri bassi per mantenere solo i 3 valori assoluti maggiori nell'ordine corretto (`[88, 90, 95]`).

### 5. Test MergeCRDT (Il cuore dell'Idempotenza)
Questi sono i test più critici dell'intero file:
- `TestMergeCRDT_Idempotenza`: $merge(A, A) = A$
- `TestMergeCRDT_PreferisceVersioneAlta`: Dimostra che se arriva un valore con `Epoch` uguale ma `Version` superiore, questo sovrascrive quello locale.
- `TestMergeCRDT_NuovoContributo`: Dimostra che valori appartenenti a `NodeID` sconosciuti vengono aggiunti al dizionario locale senza corromperne lo stato.
