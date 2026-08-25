# tests/topology/topology_test.go

Questo file testa rigorosamente il **Failure Detector** distribuito del sistema gossip e le regole del *Piggybacking*. L'affidabilità di questa logica è ciò che impedisce a un cluster distribuito di implodere a causa dei falsi positivi (nodi marcati ingiustamente come morti a causa del traffico di rete).

## Test sulla Discovery
- **`TestNewManager_SelfIsAlive`**: Verifica la logica di base secondo cui, all'avvio, un nodo deve inserire se stesso nella propria tabella di routing come *Alive* (essenziale per poter inviare il proprio heartbeat al primo ciclo gossip).
- **`TestAddPeer_NuovoPeer`**: Verifica che aggiungere manualmente un nodo (es. tramite *SeedPeers*) popoli correttamente la tabella.
- **`TestGetRandomPeers_...`**: Testa l'estrazione stocastica del *fanout*. Si assicura matematicamente che l'estrazione non includa **mai** il nodo stesso (che manderebbe pacchetti a se stesso intasando la socket) e che se si richiedono $N$ nodi ma ce ne sono di meno, la funzione gestisca l'underflow ritornando solo quelli esistenti.

## Test sul Failure Detector (Ciclo di Vita)
Questi test usano pause calibrate con precisione tramite `time.Sleep` per simulare l'invecchiamento delle entry senza alterare il clock di sistema:
- **`TestCheckTimeouts_AliveToSuspect`**: Fa invecchiare un peer oltre il `SuspectTimeout` e assicura che il suo stato decada ad *Alive* $\rightarrow$ *Suspect*.
- **`TestCheckTimeouts_SuspectToDead`**: Ripete lo step precedente e attende ulteriore tempo, verificando la transizione *Suspect* $\rightarrow$ *Dead*. Quando un nodo diventa *Dead*, la funzione aspetta che scompaia totalmente dall'elenco ritornato da `GetAlivePeers()`, che alimenta l'iterazione gossip.
- **`TestMembership_Cleanup`**: Collauda la *Tombstone*. Un nodo morto non deve rimanere in memoria RAM per sempre. Dopo un `CleanupTimeout` (in produzione settato tipicamente a 5 minuti), questo test verifica che il nodo venga esplicitamente cancellato tramite `delete()` dalla mappa interna, liberando memoria.

## Test sul Merge (Piggybacking)
- **`TestMergeMembership_NuovoPeer`**: Simulando un pacchetto gossip in arrivo, inietta l'informazione di un nodo finora ignoto e verifica che venga inserito automaticamente nella topologia locale (Discovery epidemica).
- **`TestMergeMembership_NonSovrascriveSelf`**: Sicurezza critica. Se per un errore di rete remoto arrivasse un pacchetto in cui qualche nodo sostiene che *noi* (nodo locale) siamo morti, questo test assicura che la logica locale rifiuti tassativamente l'informazione, mantenendo sempre il *Self* ad *Alive*.
