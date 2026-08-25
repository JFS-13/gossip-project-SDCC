# internal/transport/transport.go

Questo file definisce l'astrazione fondamentale per il livello di rete.
In un'architettura software pulita (Clean Architecture), il livello applicativo (Gossip) non deve sapere *come* i dati vengono inviati fisicamente sul cavo, ma solo che esiste un modo per farlo.

## L'Interfaccia `Transport` (Righe 8-13)
Il contratto che ogni layer di trasporto deve implementare per poter essere iniettato nell'`Engine`:
- `Start(ctx, handler)`: Inizializza il server (es. apre la porta in ascolto). L'`handler` è la callback (una funzione) che il layer di trasporto dovrà invocare non appena riceve un pacchetto valido. Nel nostro caso, questa callback sarà `engine.handleMessage`.
- `Send(ctx, address, payload)`: Invia un pacchetto grezzo (`[]byte`) all'indirizzo di destinazione.
- `Close()`: Chiude e libera le risorse di rete.

## La struct `NoopTransport` (Righe 15-20)
Implementa il pattern *Null Object*.
È uno "stub" (un'implementazione vuota e fittizia) utilissima per effettuare test unitari veloci senza dover realmente allocare socket di rete reali, evitando problemi di porte già occupate o lentezza del sistema operativo locale.
