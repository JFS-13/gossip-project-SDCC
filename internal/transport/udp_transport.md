# internal/transport/udp_transport.go

Questo file contiene la vera e propria implementazione operativa per la rete: un layer UDP non bloccante e thread-safe.
Il protocollo Gossip, per sua natura, produce un volume elevatissimo di messaggi molto piccoli e non si aspetta ricevute di ritorno (ACK). Il protocollo **UDP** (User Datagram Protocol) è quindi la scelta tecnicamente perfetta rispetto a TCP (che introdurrebbe latenza per l'handshake e appesantirebbe lo stack di rete).

## `UDPTransport` Struct (Righe 13-23)
Contiene lo stato del transport di rete, come il puntatore al `net.UDPConn`, flag di stato protetti da `sync.RWMutex`, un handler per i messaggi in arrivo e costrutti per una chiusura pulita (`sync.Once`, `chan struct{}`, `sync.WaitGroup`).

## Metodi Principali

### `Start(...)` (Riga 37)
Risolve l'indirizzo locale fornito in configurazione e apre una socket UDP per ascoltare il traffico in ingresso (`net.ListenUDP`). Se ha successo, avvia una goroutine in background asincrona (`readLoop`) e ritorna.

### `Send(ctx, address, payload)` (Riga 69)
Invia i byte del `GossipMessage` a uno specifico indirizzo di destinazione (`ip:porta`).
Viene usata la stessa socket aperta in ascolto (`WriteToUDP`). Questo è cruciale perché assicura che il pacchetto in uscita provenga dalla stessa porta in cui il nodo aspetta le risposte. Se per qualche motivo il transport non fosse stato ancora avviato ufficialmente, implementa un meccanismo di *fallback* tramite `net.DialUDP` (utile in certi unit test).

### `Close()` (Riga 101)
Assicura la chiusura del file descriptor della socket e notifica ai processi in ascolto di fermarsi. L'uso di `sync.Once` è una best practice Go per rendere il metodo idempotente (chiamarlo più volte di fila non causerà panic né errori). Usa un `WaitGroup` per aspettare che il `readLoop` sia effettivamente terminato prima di uscire.

### `readLoop(ctx)` (Riga 117)
È il ciclo infinito di ascolto in background:
- Istanzia un buffer di ricezione (`65535` byte, il massimo teorico per un pacchetto UDP, sebbene i messaggi gossip siano ampiamente inferiori alla MTU di 1500 byte).
- Usa un timeout di lettura ciclico (`SetReadDeadline` a 250ms). Se scade senza ricevere nulla, il ciclo riparte silenziamente. Questa tecnica non-bloccante evita che la *goroutine* rimanga perennemente incastrata in una read di sistema nel caso in cui le venga chiesto lo shutdown (`<-t.done` o `<-ctx.Done()`).
- Quando riceve un pacchetto corretto (`n > 0`), alloca **una nuova copia in memoria** del payload (`copy(payload, buffer[:n])`). Questo passaggio previene pericolosissimi bug di *Data Race* nel caso in cui l'handler deleghi la processazione ad altre *goroutine* mentre il `readLoop` sta già riempiendo di nuovo il buffer originale con un pacchetto successivo. Infine, invoca la callback `t.handler` (passatagli dall'`Engine`).
