# internal/observability/telemetry.go

Questo file raggruppa due concetti architetturali fondamentali per un sistema distribuito pronto per la produzione (production-ready): l'esposizione delle **Metriche** (via HTTP) e lo **Structured Logging**.
Mantenere queste funzionalità in un package dedicato (`observability`) rispetta il *Single Responsibility Principle* (SRP), sgravando l'`Engine` o il `main.go` da codice infrastrutturale.

## L'Interfaccia `MetricsProvider` (Righe 15-21)
Per evitare che il web server debba importare tutto il package `gossip` e conoscere intimamente l'Engine (creando accoppiamento forte), viene definita questa piccola interfaccia. Il server di telemetria sa solo che chiunque gli verrà iniettato sarà in grado di fornirgli la `Estimate`, il `Round` corrente e l'`Epoch`. L'`Engine` rispetta implicitamente questa interfaccia.

## Il Server HTTP (`TelemetryServer`)
È un server leggero asincrono che gira in parallelo al motore gossip UDP. Viene esposto su una porta separata (tipicamente `NodePort + 1000`, es. se il gossip è sull'8001, le metriche sono sull'9001, anche se nel nostro docker compose locale le porte HTTP sono esposte per comodità direttamente sulle 8000).

### Endpoint `/health` (Riga 64)
Restituisce un semplice `200 OK` in formato JSON, con l'ID del nodo e l'uptime. 
Negli orchestratori moderni (come Kubernetes o gli health check degli Application Load Balancer di AWS), questo endpoint è fondamentale per determinare se il container è bloccato o ancora responsivo.

### Endpoint `/metrics` (Riga 72)
Richiede al `provider` lo stato aggiornato (in modo thread-safe, grazie agli `RWMutex` interni all'Engine) e lo restituisce in formato JSON. 
Questo ci ha permesso di interrogare banalmente il cluster con un `curl` (es. `curl http://localhost:8001/metrics`) e verificare empiricamente la convergenza matematica dell'epidemia, il numero di nodi conosciuti (`known_nodes`) e l'`epoch`.

## `SetupLogger` (Riga 84)
Inizializza la libreria standard nativa di Go `log/slog` (introdotta di recente in Go 1.21).
La configuriamo forzando il `slog.NewJSONHandler`. 
Ciò significa che tutti i `log.Info` generati nel progetto non verranno stampati come testo libero, ma come perfetti documenti JSON. Questo è un requisito assoluto se i log devono essere ingeriti da sistemi come ElasticSearch, Datadog o AWS CloudWatch, dato che permette di filtrare, ricercare e indicizzare i log in base a tag (es. `node_id`, `round`).
