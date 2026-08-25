# internal/aggregation/minmax.go

Questo file raggruppa l'implementazione di due funzioni di aggregazione strettamente correlate: la ricerca del **Minimo Globale (Min)** e del **Massimo Globale (Max)**.

## Caratteristiche Generali
A differenza di Average o Sum, Min e Max sono operazioni intrinsecamente **idempotenti**. Questo significa che, teoricamente, anche nei protocolli gossip più banali, ricevere due volte lo stesso valore non sfalserebbe il risultato. Tuttavia, utilizzando un dizionario CRDT *per-contributo*, otteniamo un vantaggio architetturale enorme: **la tolleranza alla modifica dinamica**.

Se usassimo solo una variabile globale (es. `globalMin = min(globalMin, receivedValue)`), se un nodo correggesse al rialzo il suo sensore (es. da 5 a 20), il minimo globale rimarrebbe "incastrato" a 5 per sempre.
Registrando invece il valore per *NodeID*, quando un nodo aggiorna il suo valore incrementandone la versione, il vecchio 5 viene sovrascritto, consentendo al minimo globale di risalire.

## `MinAggregator` (Righe 8-42)
- **`SetContribution(...)`**: Salva il campo `Value` nella struct `Contribution` e incrementa la versione associata al `NodeID`.
- **`ComputeResult(...)`**: 
  - Inizializza il risultato temporaneo a $+\infty$ (`math.Inf(1)`).
  - Itera in sola lettura sulla mappa del CRDT. 
  - Sostituisce il risultato se incontra un valore inferiore. Restituisce $0$ se la mappa è vuota.

## `MaxAggregator` (Righe 44-76)
Logica speculare e opposta:
- **`SetContribution(...)`**: Identico al `MinAggregator`.
- **`ComputeResult(...)`**: 
  - Inizializza il risultato temporaneo a $-\infty$ (`math.Inf(-1)`).
  - Itera sulla mappa del CRDT e salva il valore se risulta strettamente superiore a quello correntemente salvato.
