# tests/integration/cluster_test.go

Questo file contiene i **Test di Integrazione e Robustezza** dell'intero sistema Gossip.
È probabilmente il file di test più importante del progetto, poiché non si limita a verificare una singola funzione, ma orchestra interi cluster in memoria (simulando 3 nodi) e verifica che la matematica distribuita funzioni davvero in condizioni ostili.

## `InMemoryBus` e `InMemoryTransport` (Righe 20-101)
Per testare il gossip non potevamo avviare 3 server UDP veri e propri ad ogni esecuzione dei test (sarebbe lento, instabile nei CI/CD e prono a errori di porte occupate). 
Queste due struct realizzano un mock (una simulazione) del layer di trasporto di rete: i nodi credono di mandare pacchetti su una socket UDP (`Send`), ma in realtà il `InMemoryBus` passa semplicemente le slice di byte da una *goroutine* all'altra istantaneamente.
Inoltre, il bus implementa i metodi `Block(addr)` e `Unblock(addr)`: intercettando il traffico, possiamo simulare perfettamente **crash di nodi** e **partizioni di rete**.

## I Test di Convergenza Base (M09-1 e M09-2)
- **`TestClusterConvergenza_Average`**: Istanzia 3 nodi con valori `10`, `30` e `50`. Esegue l'engine e attende asincronamente che tutti i nodi raggiungano la media corretta (`30`). Verifica inoltre la "Membership-Aware Aggregation": alla disconnessione dei nodi, la stima converge ai nuovi valori reali.
- **`TestClusterConvergenza_Sum`**: Stesso test, ma con `SumAggregator`. Verifica che la somma totale arrivi a `90`.

## I Test di Robustezza (Chaos Engineering)
Questi test (Milestone 09) validano la resilienza dei CRDT sviluppati:
- **`TestRobustezza_CrashNodo`**: Aspetta la prima convergenza, poi "uccide" brutalmente `node-3` (`bus.Block`). Assicura che i restanti nodi mantengano in memoria il suo contributo e la media rimanga intatta.
- **`TestRobustezza_CrashERestart`**: Un nodo crascia e poi torna online (rejoin) spacciandosi per una nuova iterazione (grazie all'avanzamento del suo `Epoch`). Il test verifica che il cluster scarti la vecchia copia morta e accetti il nuovo stato riconvergendo.
- **`TestRobustezza_PartizioneRete`**: Simula il classico "Split-Brain": un nodo viene tagliato fuori dalla rete temporaneamente per poi ricongiungersi, dimostrando l'auto-healing del protocollo.
- **`TestRobustezza_MessaggiDuplicati`**: Invia intenzionalmente pacchetti clonati (attacco replay/duplicazione UDP). Dimostra che grazie alla monotonia delle versioni del CRDT, i nodi semplicemente ignorano il doppione (idempotenza).

Tutti i test usano la funzione helper asincrona `waitForConvergence`, che interroga a campione gli engine finché la stima non rientra nella `tolerance` matematica attesa.
