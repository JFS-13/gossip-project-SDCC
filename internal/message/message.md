# internal/types/message.go

Questo file è il vocabolario condiviso di tutto il progetto. Mantenendo qui i tipi fondamentali, preveniamo il fastidioso problema degli *import cycle* di Go (quando due package si importano a vicenda) e forniamo un punto centrale per capire la forma dei dati in transito.

## 1. La Struttura `Contribution` (Righe 31-50)
È l'atomo fondamentale del nostro State-based CRDT. Rappresenta il contributo matematico di un singolo nodo.
A seconda del tipo di aggregazione scelta, un nodo userà solo i campi necessari (es. `Value` per Min/Max/Sum, `Sum`+`Count` per Average, `TopK` per Top-K).
I due campi vitali per il funzionamento distribuito sono:
- **`Epoch`**: Generato al boot.
- **`Version`**: Contatore monotono crescente.

## 2. Lo Stato Globale `AggregationState` (Righe 56-153)
Contiene la mappa di tutti i contributi conosciuti (associa un `NodeID` alla sua `Contribution`).
Il cuore di questa struct è la funzione **`MergeCRDT(remote)`**. Questa funzione definisce le regole matematiche per fondere lo stato locale con uno in arrivo dalla rete:
1. Valuta il contributo di ogni singolo nodo.
2. Accetta il dato remoto solo se è "più fresco". La freschezza è data dalla tupla `(Epoch, Version)`. 
   - Se l'Epoch remoto è *maggiore*, significa che il nodo è crasciato e si è riavviato (e riparte da versione 0); quindi si scartano i vecchi dati locali (Epoch minore) e si accetta quello nuovo.
   - A parità di Epoch, vince chi ha la `Version` più alta.
Essendo un CRDT, questa operazione di merge gode di proprietà matematiche rigorose: è **Idempotente** (mergiare lo stesso stato non cambia nulla), **Commutativa** (l'ordine di arrivo dei messaggi UDP non conta) e **Associativa**.

## 3. L'Envelope `GossipMessage` (Righe 159-170)
È l'esatta rappresentazione JSON del pacchetto UDP che viaggia sul cavo. Oltre ai metadati del mittente e allo `State` CRDT completo, include un array `Membership`. 
Inserire la membership all'interno dello stesso pacchetto dell'aggregazione prende il nome di **Piggybacking**: risparmiamo banda ed evitiamo di aprire ulteriori socket dedicati al Failure Detection.

## 4. `MembershipEntry` (Righe 184-193)
È la struct che viene serializzata nel Piggybacking di cui sopra. Riassume per il mondo esterno come appare un nodo: il suo IP/porta, il suo status (vivo, sospetto, andato) e l'`Incarnation` (il famoso battito cardiaco che il ricevente valuterà per aggiornare i timeout).
