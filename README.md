# Gossip-Based Distributed Data Aggregation

Questo progetto, realizzato per il corso di Sistemi Distribuiti e Cloud Computing, implementa un sistema distribuito decentralizzato (peer-to-peer) capace di aggregare dati in tempo reale utilizzando un **protocollo epidemico (Gossip)** accoppiato a strutture dati **CRDT (Convergent Replicated Data Types)**. I CRDT sono speciali strutture dati progettate per essere replicate su più macchine; garantiscono che tutti i nodi convergano matematicamente allo stesso stato finale indipendentemente dall'ordine di arrivo dei messaggi o dalla presenza di eventuali duplicati di rete.

L'obiettivo è ottenere una stima globale convergente (es. media dei carichi, ricerca del massimo, somma globale) all'interno di un cluster di nodi, senza alcun Single Point of Failure (SPOF) o nodo coordinatore.

---

## Indice
1. [Panoramica e Architettura](#panoramica-e-architettura)
2. [Configurazione del Nodo](#configurazione-del-nodo)
3. [Tipi di Aggregazione (CRDT)](#tipi-di-aggregazione-crdt)
4. [Deploy del Cluster tramite Docker Compose](#deploy-del-cluster-tramite-docker-compose)
5. [Esecuzione Manuale Locale (Senza Docker)](#esecuzione-manuale-locale-senza-docker)
6. [Esecuzione e Struttura dei Test](#esecuzione-e-struttura-dei-test)
7. [Spegnimento Controllato e Fault Injection](#spegnimento-controllato-e-fault-injection)
8. [Osservabilità e Metriche](#osservabilità-e-metriche)
9. [Deploy su AWS Learner Lab](#deploy-su-aws-learner-lab)

---

## Panoramica e Architettura

Ogni nodo è un'istanza indipendente scritta in Go. Periodicamente, ogni nodo sceglie casualmente $K$ peer (parametro `fanout`) ed invia loro il proprio stato interno tramite protocollo **UDP**. 
L'architettura è divisa nei seguenti layer:

- **`Transport`**: Gestisce l'invio e la ricezione asincrona dei pacchetti UDP serializzati in JSON. Basandosi su un'interfaccia astratta, permette di iniettare dinamicamente implementazioni diverse: oltre al layer UDP reale, include lo stub NoopTransport per gli unit test ed un virtual switch in memoria (basato su Go Channels) per gli integration test.
- **`Topology`**: Gestisce l'appartenenza al cluster implementando un meccanismo di *Failure Detection* basato sul protocollo **SWIM** (Scalable Weakly-consistent Infection-style Process Group Membership). Si tratta di un protocollo in cui i nodi si monitorano a vicenda in modo decentralizzato, segnando lo stato dei peer come `Alive`, `Suspect` o `Dead` tramite dei timeout. Per evitare i conflitti ed i falsi positivi (es. un nodo che rientra dopo un crash), il protocollo associa ad ogni nodo un **Incarnation Number** (un orologio logico monotonico). Tramite *piggybacking*, ogni messaggio gossip trasporta anche la vista della topologia del mittente, permettendo la scoperta dinamica dei nuovi nodi.
- **`Message`**: Definisce i modelli dati scambiati sulla rete (il payload GossipMessage), tra cui la struttura dello stato CRDT condiviso (AggregationState). Qui risiede la definizione di Epoch e Version, ovvero degli orologi logici scalari utilizzati per imporre un ordine totale agli aggiornamenti e risolvere in modo deterministico i conflitti.
- **`Aggregation`**: Motore matematico idempotente. Implementa un approccio **Multi-CRDT** (Composite Aggregator): calcola simultaneamente le funzioni *Sum*, *Average*, *Min*, *Max* e *Top-K*. Garantisce che i risultati finali rimangano matematicamente corretti e convergenti anche in presenza di pacchetti UDP duplicati, smarriti o arrivati fuori ordine.
- **`Core`**: Cuore pulsante dell'istanza (event loop), innescato da un time ticker su una goroutine dedicata. Coordina il campionamento casuale del fanout per i round epidemici, orchestra il Merge asincrono (protetto da lock) degli stati CRDT, ed attua meccanismi di auto-guarigione (come il ping stocastico verso i seed originali) per sanare eventuali partizioni di rete o scenari di split-brain.
- **`Setup`**: Modulo dedicato al caricamento ed alla validazione stretta dei parametri operativi. La configurazione viene risolta tramite una catena di fallback: valori di default, parsing di un file YAML ed infine sovrascrittura tramite variabili d'ambiente (strategia per il deploy su Docker Compose ed AWS).
- **`Telemetry`**: Sottosistema responsabile del logging strutturato in formato JSON (tramite log/slog) e dell'esposizione asincrona di un server HTTP per l'osservabilità. Espone l'endpoint di liveness /health e l'endpoint /metrics, che permette di interrogare in tempo reale il nodo per conoscere il valore della stima aggregata (estimate), il numero di peer vivi visti dal nodo (known_nodes), il round di esecuzione corrente e l'epoch.

---

## Configurazione del Nodo

I nodi sono progettati per essere interamente configurabili all'avvio tramite file **YAML**. Il percorso del file si specifica con il flag `--config`. Nel repository sono presenti configurazioni di esempio all'interno della cartella `configs/` (es. `node1.yaml`, `node2.yaml`, ...).

Esempio di file `node1.yaml`:
```yaml
node_id: "node-1"
bind_address: "0.0.0.0"
advertise_addr: "node1"
node_port: 7001
seed_peers:
  - "node2:7002"
  - "node3:7003"
gossip_interval_ms: 1000
fanout: 2
membership_timeout_ms: 10000
aggregation_type: "topk"
initial_value: 10.0
top_k_size: 3
log_level: "info"
```

### Parametri Chiave
- `node_id`: Nome logico univoco del nodo (es. `node-1`). È fondamentale che ogni istanza del cluster abbia un ID distinto.
- `bind_address`: L'interfaccia di rete locale su cui il nodo si mette in ascolto per il traffico UDP (di solito `0.0.0.0` per ascoltare su tutte le interfacce).
- `advertise_address`: L'hostname o l'IP pubblico che questo nodo comunicherà al resto del cluster per farsi contattare. In Docker, coincide tipicamente con il nome del container/servizio DNS.
- `node_port`: La porta UDP utilizzata dal nodo sia per l'ascolto che per l'invio dei datagrammi gossip.
- `seed_peers`: Una lista iniziale di nodi noti (bootstrap). Un nodo appena avviato inizierà a contattare questi endpoint per presentarsi, venendo così scoperto dinamicamente dal resto della rete P2P.
- `gossip_interval_ms`: Frequenza (in millisecondi) con cui si ripete l'event loop epidemico.
- `fanout`: Numero di peer scelti casualmente dalla tabella di routing a cui inviare il proprio stato durante ogni round di gossip.
- `membership_timeout_ms`: Tempo massimo di silenzio (in millisecondi) superato il quale un peer viene declassato da `Alive` a `Suspect` (e, dopo un ulteriore periodo, a `Dead`) dal Failure Detector SWIM.
- `aggregation_type`: Specifica la funzione matematica CRDT "primaria". Sebbene il nodo calcoli *tutte* le aggregazioni in background (Multi-CRDT), questo campo determina quale valore verrà esposto come stima principale (`estimate`) nel server telemetrico (valori supportati: `sum`, `average`, `min`, `max`, `topk`).
- `initial_value`: Il contributo numerico iniziale che questo specifico nodo immette nell'aggregazione.
- `top_k_size`: Parametro utilizzato unicamente quando l'aggregatore scelto è `topk`. Stabilisce la dimensione assoluta $K$ della classifica da calcolare (es. i 3 valori più alti dell'intera rete).
- `log_level`: Livello di verbosità del logger strutturato JSON (`debug`, `info`, `warn`, `error`).

---

## Aggregazioni Multi-CRDT

Grazie all'architettura CompositeAggregator introdotta nel progetto, il cluster è in grado di calcolare **tutte le seguenti funzioni contemporaneamente**, sfruttando lo stesso payload di rete senza costi aggiuntivi. Modificando il campo `aggregation_type` nello YAML si sceglie solo quale funzione considerare come "primaria":

1. **`sum`**: Somma globale e distribuita di tutti i valori iniziali dei nodi attivi.
2. **`average`**: Media globale convergente.
3. **`min` / `max`**: Elezione distribuita del valore più basso o più alto nella rete.
4. **`topk`**: Trova i *K* valori assoluti più alti presenti nel sistema. Configurabile via `top_k_size` nello YAML.

*Nota operazionale: un nodo etichettato come `Dead` (che non risponde per il tempo di timeout) non viene rimosso fisicamente dalla memoria CRDT, ma il suo contributo viene filtrato a runtime, smettendo di pesare sul risultato matematico finché il nodo non torna `Alive`.*

---

## Deploy del Cluster tramite Docker Compose

Per eseguire l'intero cluster in un ambiente isolato è possibile utilizzare **Docker Compose**. Alla radice del progetto è presente un `docker-compose.yml` preconfigurato per avviare **8 nodi**.

### Prerequisiti Minimi
Prima di avviare il cluster, assicurarsi di avere a disposizione:
- **Docker Engine** in esecuzione;
- **Docker Compose** plugin (comando `docker compose` o `docker-compose`);
- **Go installato** (versione `1.26` o superiore) nel caso si voglia eseguire i test o avviare i nodi nativamente senza Docker;

Per verificare rapidamente che l'ambiente sia pronto si possono eseguire i comandi:
```bash
docker --version
docker-compose version
go version
```

### 1. Avvio del Cluster
```bash
docker-compose up -d
```
Questo comando:
- Compila il binario Go all'interno di container isolati.
- Crea una subnet privata DNS chiamata `gossip-net`.
- Lancia gli 8 servizi (`node1` fino a `node8`) iniettando ad ognuno il proprio file YAML (es. `configs/node1.yaml`).

### 2. Osservazione dei Log
```bash
docker-compose logs -f
```
È possibile visualizzare un log strutturato in formato JSON il cui output è simile al seguente:
```
gossip-node1  | {"time":"2026-09-08T14:45:22.033721402Z","level":"INFO","msg":"stima corrente","node_id":"node-1","aggregation":"topk","estimate":"80.0000","known_nodes":8,"round":34}
gossip-node1  | {"time":"2026-09-08T14:45:22.033907194Z","level":"INFO","msg":"tutte le aggregazioni","node_id":"node-1","sum":"360.0000","average":"45.0000","min":"10.0000","max":"80.0000","top_k":[60,70,80]}
```
Dettaglio del log:
- **`time`**: Timestamp dell'evento.
- **`level`**: Gravità dell'evento (INFO, DEBUG, WARN, ERROR).
- **`msg`**: Breve descrizione testuale dell'evento loggato per facilitarne la lettura (es. "stima corrente" o "top-k elementi").
- **`node_id`**: L'identificativo logico del nodo che sta scrivendo il log.
- **`aggregation`**: L'aggregazione matematica CRDT attualmente in esecuzione.
- **`estimate`**: Il risultato numerico convergente calcolato dal nodo in questo esatto momento.
- **`known_nodes`**: Il numero di peer all'interno del cluster attualmente riconosciuti come Alive dal Failure Detector.
- **`round`**: Il contatore dei cicli gossip (tick periodici) completati dal nodo dall'accensione.
- **`top_k`**: (Speciale) Array mostrato solo se l'aggregazione in uso è topk, che elenca i valori in classifica.

Osservando i log in tempo reale, si può osservare che i nodi partono con stime (estimate) isolate, ma dopo pochissimi round convergono tutti simultaneamente allo stesso esatto valore globale.

### 3. Spegnimento
```bash
docker-compose down
```

---

## Esecuzione Manuale Locale (Senza Docker)

Se la toolchain Go è installata (`>= 1.26`), è possibile eseguire le istanze manualmente aprendo più terminali.
**Nota bene:** Il comando `go run` avvia **sempre e solo una singola istanza alla volta**. Per simulare la rete, bisogna aprire più finestre (o tab) del terminale ed avviare un nodo in ciascuna, passandogli un file YAML dedicato per evitare conflitti di porta. Ad esempio:

**Terminale 1 (Avvia il Nodo 1):**
```bash
go run ./cmd/agent/main.go --config configs/node1.yaml
```

**Terminale 2 (Avvia il Nodo 2):**
```bash
go run ./cmd/agent/main.go --config configs/node2.yaml
```

**Terminale 3 (Avvia il Nodo 3):**
```bash
go run ./cmd/agent/main.go --config configs/node3.yaml
```

Avviati i nodi successivi al primo, questi utilizzeranno la direttiva `seed_peers` (configurata nei loro YAML) per contattare la porta locale del Nodo 1. In questo modo si scopriranno a vicenda creando dinamicamente la rete P2P ed inizieranno a scambiarsi gli stati.

---

## Esecuzione e Struttura dei Test

L'intera architettura è coperta da una forte suite di unit test ed integration test basata su un'interfaccia Transport *In-Memory*, la quale simula il traffico di rete in memoria senza allocare vere porte UDP.

### Comando generale per l'esecuzione
```bash
go test ./... -v
```
> **Nota per utenti Windows (AppLocker / Antivirus):**
> Se eseguendo `go test ./...` si riceve l'errore `An Application Control policy has blocked this file`, si può compilare ed eseguire il test manualmente nella root del progetto:
> ```powershell
> cd tests/integration
> go test -c -o ../../integration.test.exe
> cd ../..
> ./integration.test.exe
> ```

### Struttura dei Test
- **`tests/aggregation/...`**: Verifica puramente matematica. Assicura che le formule di unione (merge CRDT) ignorino eventuali messaggi duplicati, preferiscano sempre il payload con la versione più alta ed applichino correttamente le regole di idempotenza.
- **`tests/topology/...`**: Analizza il "Topology Manager" isolato, verificando l'esattezza delle transizioni di stato (`Alive -> Suspect -> Dead`) a seguito di timeout simulati.
- **`tests/integration/...`**: Rappresenta la suite **end-to-end**. Inizializza interi nodi connessi ad uno switch virtuale. Comprende test come:
  - `TestClusterConvergenza_Average` e `TestClusterConvergenza_Sum`: Avviano molteplici nodi e verificano che tutti raggiungano lo stesso aggregato convergente.
  - `TestRobustezza_PartizioneRete`: Simula la divisione della rete in due sottoreti isolate per testare i meccanismi di healing (Split-Brain).
  - `TestRobustezza_MessaggiDuplicati`: Iniettano pacchetti rindondanti per provare l'efficacia del CRDT contro le anomalie tipiche del protocollo UDP.

---

## Spegnimento Controllato e Fault Injection

Questa sezione illustra come collaudare manualmente o automaticamente la robustezza del codice introducendo guasti nel cluster su Docker. Per facilitare queste operazioni, nella cartella `scripts/` sono forniti degli strumenti dedicati:
1. **`fault_dashboard.ps1` (o `.sh`)**: Un pannello interattivo a riga di comando che elenca i container in esecuzione e permette di arrestarli (innescando un *Graceful Leave*) o riavviarli premendo semplicemente un tasto.
2. **`auto_crash_test.sh`**: Uno script di Chaos Engineering automatizzato che esegue veri e propri *Hard Crash* casuali (kill dei container) sui nodi e ne verifica i tempi di riconvergenza automatica tramite il protocollo SWIM.
3. **`demo_crash.sh`**: Una demo live e automatizzata progettata appositamente per mostrare lo stato del cluster, innescare un Hard Crash, attendere la convergenza del Failure Detector, e simulare il rejoin del nodo morto con un nuovo *Incarnation Number*.
4. **`demo_latency.sh`**: Una demo live visiva per simulare un ambiente di rete con una forte latenza. Applica ritardi crescenti (fino a 10 secondi) in modo sfalsato ad ogni nodo, mostrando in tempo reale come i CRDT riescano comunque a garantire la convergenza matematica globale.
5. **`plot_convergence.go`**: Uno strumento che interroga il cluster concorrentemente e disegna dei grafici per visualizzare visivamente la curva di convergenza delle metriche Multi-CRDT rispetto al tempo.

### Simulazione Latenza di Rete (Traffic Control)
Il sistema è in grado di dimostrare la stabilità e la convergenza matematica anche in scenari con forte latenza di rete. Sfruttando le capacità del kernel Linux (tramite il modulo `tc qdisc netem`), i container possono ritardare artificialmente l'uscita di tutti i loro pacchetti UDP.

Questo meccanismo è presente nello script **`entrypoint.sh`**, che viene eseguito automaticamente all'avvio di ogni container. Lo script intercetta le variabili d'ambiente fornite da Docker e, prima di avviare l'eseguibile Go, applica le regole di traffic control direttamente sull'interfaccia di rete (`eth0`) del container.

Nel `docker-compose.yml`, il ritardo (espresso in millisecondi) parte da `0` di default ed è applicabile dinamicamente **direttamente da riga di comando**.

1. **Ritardo Globale**: 
   Per ritardare l'uscita dei pacchetti di tutti i nodi contemporaneamente, usa la variabile `NETWORK_DELAY_MS`.
   - Su **Windows (PowerShell)**: `$env:NETWORK_DELAY_MS=300; docker-compose up -d`
   - Su **Linux/Mac/GitBash**: `NETWORK_DELAY_MS=300 docker-compose up -d`

2. **Ritardo Selettivo (per singolo nodo)**:
   Volendo simulare che solo un nodo specifico abbia problemi di connettività, si possono usare le variabili specifiche `NODE1_DELAY`, `NODE2_DELAY`, ecc., che sovrascriveranno il comportamento globale solo per quel nodo.
   - Su **Windows (PowerShell)**: `$env:NODE3_DELAY=1000; docker-compose up -d`
   - Su **Linux/Mac/GitBash**: `NODE3_DELAY=1000 docker-compose up -d`

### Spegnimento Controllato (Graceful Leave)
Se un nodo viene interrotto volontariamente (usando `docker stop`), simulando uno spegnimento volontario, l'applicazione intercetta il segnale del sistema operativo ed esegue un *graceful shutdown*. Invece di scomparire nel nulla, il nodo annuncia attivamente la sua uscita inviando un messaggio di "Leave" ai suoi peer. 
Questo permette al cluster di rimuovere il nodo dai calcoli in modo istantaneo, senza dover attendere l'intervento del Failure Detector.
Log atteso in uscita:
```
{"time":"2026-09-03T16:55:05.626Z","level":"INFO","msg":"shutdown in corso","node_id":"node-2"}
{"time":"2026-09-03T16:55:08.367Z","level":"INFO","msg":"Leave announcement inviato a 2 peer"}
```

### Test di Crash & Rejoin
Per collaudare la robustezza del protocollo contro i guasti improvvisi di alimentazione o rete, si può simulare un Hard Crash. Mentre il cluster è in esecuzione (`docker-compose up -d`), è possibile arrestare un'istanza forzatamente senza darle il tempo di inviare il Leave, usando ad esempio un comando del tipo:
```bash
docker kill gossip-node3
```

Comportamento atteso:
- I pacchetti destinati a `node3` iniziano a cadere nel vuoto.
- Entro la soglia di `membership_timeout_ms`, i nodi adiacenti, non ricevendo più l'heartbeat, declasseranno `node3` a `Suspect` e, conseguentemente, a `Dead`.
- Appena il nodo viene etichettato come morto, i nodi vivi ricalcolano l'aggregazione astraendo dinamicamente il contributo di `node3` dal totale globale.

Riavviando il nodo (`docker start gossip-node3`):
- `node3` si risveglia in modalità "stateless". Genera un nuovo `Incarnation Number` basato sull'Epoch time corrente (superiore a quello in cache negli altri nodi).
- Gli altri nodi ricevono il gossip, notano l'Incarnation superiore, rimuovono lo stato `Dead` dalla tabella e l'intera rete riconverge nuovamente includendo il nodo redivivo.

---

## Osservabilità e Metriche

L'Engine espone nativamente un server HTTP integrato, pensato per l'esposizione di **metriche**, ovvero un riepilogo in tempo reale dello stato di salute e dei parametri calcolati dall'applicazione (stima attuale, numero di peers attivi).

Se un nodo è configurato sulla porta UDP `7001`, il suo server HTTP verrà avviato per convenzione sulla porta `7001 + 1000 = 8001`. 
*Si fa così per evitare conflitti tra il traffico Gossip (UDP) ed il traffico di monitoraggio (TCP) sulla stessa istanza, ricavando deterministicamente la porta di servizio senza richiedere configurazioni separate.*

Endpoints disponibili:
- **`GET /health`**: Restituisce `{ "status": "ok" }` se l'istanza è responsiva.
- **`GET /metrics`**: Esporta in formato **JSON** lo stato di convergenza. Include la stima primaria (`estimate`), i tick completati (`round`), il numero di peer riconosciuti (`known_nodes`), e soprattutto un blocco `all_aggregations` contenente tutti i calcoli Multi-CRDT aggiornati in tempo reale (sum, average, min, max, top_k).

È possibile interrogare l'istanza localmente eseguendo un comando da terminale:
```bash
curl http://localhost:8001/metrics
curl http://localhost:8001/health
```
Grazie al port-mapping configurato nel docker-compose.yml, avviando il cluster in locale sul PC è possibile interrogare i nodi direttamente tramite un qualsiasi browser, incollando nella barra degli indirizzi:

- http://localhost:8001/metrics (per ispezionare il Nodo 1)
- http://localhost:8002/metrics (per ispezionare il Nodo 2)
- http://localhost:8003/metrics (per ispezionare il Nodo 3)
- ...e così via per gli altri nodi.

Per verificare lo stato di salute (Health Check):

- http://localhost:8001/health (Risponderà {"status":"ok"} se il nodo è attivo e funzionante)

Aggiornando la pagina del browser si può vedere l'avanzamento dei round ed il valore aggregato (estimate) cambiare in tempo reale, finché non raggiungerà la perfetta convergenza con il resto del cluster.

***

## Deploy su AWS Learner Lab

Questa sezione illustra come testare l'architettura cloud su Amazon Web Services, utilizzando l'ambiente **AWS Academy Learner Lab**. Vengono proposte due metodologie: una rapida su singola istanza ed una distribuita su 5 macchine fisiche distinte.

### Opzione A: Cluster Simulato (1 EC2, 8 Nodi containerizzati)
Questa soluzione è ideale per un test rapido. Si utilizza una singola istanza fisica per ospitare l'intero cluster containerizzato, in modo identico all'esecuzione locale sulla propria macchina.

1. **Avvio dell'Istanza**: Da AWS Academy (Canvas), avviare il Learner Lab (**Start Lab**) ed accedere alla console AWS. Creare una singola istanza EC2 di tipo `t3.micro` (o `t2.micro`, incluse nel piano gratuito) con immagine software (AMI) **Amazon Linux 2023** (o **Ubuntu**) e chiave `vockey`. 
2. **Security Group**: Nelle impostazioni di rete, creare un gruppo di sicurezza:
   - Lasciare la regola SSH esistente (protocollo `TCP` con intervallo di porte `22`).
   - Aggiungere regola del gruppo di sicurezza `TCP personalizzato` con intervallo di porte `8001 - 8008` (per interrogare gli endpoint delle metriche HTTP dal browser) e tipo di origine `ovunque` (`0.0.0.0/0`) che permettono a tutti gli indirizzi IP di accedere all'istanza.
   - Non c'è bisogno di aprire le porte UDP verso il mondo esterno siccome i nodi comunicheranno privatamente tra loro.
   - Lasciare le configurazioni di archiviazione esistenti.
3. **Connessione all'istanza**: È possibile connettersi all'istanza in due modi:
   - **Tramite Browser**: Dalla console AWS, selezionare l'istanza e cliccare su **Connetti** -> **Connessione a un'istanza EC2**.
   - **Tramite Client SSH**: 
     1. Dal portale AWS Academy, cliccare su **AWS Details** e scaricare il file della chiave SSH (`labsuser.pem` o `vockey.pem`).
     2. Aprire **Git Bash** nella cartella dove è stato salvato il file della chiave SSH.
     3. Restringere i permessi del file chiave (operazione da fare una sola volta):
        ```bash
        chmod 400 "labsuser.pem"
        ```
     4. Connettersi copiando l'indirizzo DNS Pubblico dell'istanza dalla console AWS:
        ```bash
        ssh -i "labsuser.pem" ec2-user@<INDIRIZZO_DNS_PUBBLICO>
        ```
        *(Nota: al riavvio di un'istanza cloud, l'Indirizzo IP pubblico cambia, per cui sarà necessario aggiornare il comando SSH con il nuovo IP).*

4. **Setup dell'Ambiente**: Nel terminale dell'istanza, installare Docker e Git:
   ```bash
   sudo dnf update -y
   sudo dnf install git docker -y
   sudo systemctl start docker
   sudo systemctl enable docker
   sudo usermod -aG docker ec2-user
   newgrp docker
   ```
5. **Installazione Docker Compose**:
    ```bash
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    ```
6. **Avvio del Cluster**: Clonare il repository ed eseguire il deployment tramite Docker Compose:
   ```bash
   git clone https://github.com/JFS-13/gossip-project-SDCC.git
   cd gossip-project-SDCC
   docker build -t gossip-agent:local .
   docker-compose up -d
   ```
   Questo avvierà istantaneamente tutti gli 8 nodi. Le metriche saranno visibili (ad esempio per il nodo 1) all'indirizzo `http://<IP-PUBBLICO-EC2>:8001/metrics`.
7. **Spegnimento**: Per non prosciugare il budget disponibile:
  1. Nel terminale eseguire `docker-compose down`, per cancellare i container liberando la memoria.
  2. Sulla dashboard di AWS EC2, selezionare l'istanza e fare **Stato dell'istanza -> Arresta istanza** (così non si perdono i dati e la prossima volta basterà riaccenderla). Cliccando su **Termina istanza** invece si cancella completamente l'istanza e bisognerà ricrearla al nuovo accesso.
  3. Tornare sulla scheda di Canvas (AWS Academy) e cliccare **End Lab**.
---

### Opzione B: Rete Puramente Distribuita (5 Macchine EC2 Separate)
Questo approccio rispecchia un ambiente di produzione P2P. Si allocano 5 istanze EC2 distinte (meno delle 8 locali per semplicità) che comunicano esclusivamente attraverso la rete fisica del VPC di Amazon.

#### 1. Creazione delle 5 Istanze
Dalla console AWS (sezione EC2 -> Istanze):
- Cliccare su **Avvia istanze**.
- **Numero di istanze**: Impostare a **`5`** (per crearle in blocco).
- **Nome**: `Gossip-Node` (è possibile rinominarle in seguito come `Node-1`, `Node-2`, ecc.).
- **AMI**: Scegliere **Amazon Linux 2023** o **Ubuntu**.
- **Tipo**: Lasciare `t3.micro` (o `t2.micro`) con Key pair `vockey`.
- **Security Group Unificato**: Oltre alla regola **TCP 22** (SSH) e **TCP 8001 - 8005** personalizzato, è imperativo aggiungere la regola **UDP personalizzato, porte `7001 - 7005`** in entrata, per permettere al traffico Gossip di attraversare le reti cloud. Impostare sempre tipo di origine `ovunque` (`0.0.0.0/0`) e lasciare le configurazioni di archiviazione esistenti.

#### 2. Preparazione (su tutte le 5 macchine)
Dalla console, connettersi alle istanze e aprire i 5 terminali. Su **ciascuno** di essi eseguire l'installazione:
```bash
sudo dnf update -y
sudo dnf install git docker -y
sudo systemctl start docker
sudo usermod -aG docker ec2-user
newgrp docker
```
poi clonare il codice ed effettuare la build dell'immagine base:
```bash
git clone https://github.com/JFS-13/gossip-project-SDCC.git
cd gossip-project-SDCC
sudo docker build -t gossip-agent:local .
```

#### 3. Iniezione degli IP e Avvio
In un cluster backend i nodi comunicano tramite la rete interna per motivi di sicurezza e performance, per questo si iniettano come variabili d'ambiente (`-e`) i rispettivi **Indirizzi IPv4 Privati** delle macchine (tipicamente nel formato `172.31.x.x`). Su AWS gli IP Privati **non cambiano mai** a seguito di uno Stop/Start dell'istanza (a differenza degli IP Pubblici), rendendo la configurazione del cluster permanente.

*(Appuntarsi gli IP Privati delle macchine dalla console AWS e sostituirli al posto di `<IP_PRIVATO_NODE_X>` nei comandi seguenti).*

**Sul terminale di Node-1:**
```bash
sudo docker run -d --name gossip-node1 \
  --cap-add=NET_ADMIN \
  -p 8001:8001 -p 7001:7001/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="<IP_PRIVATO_NODE_1>" \
  -e SEED_PEERS="<IP_PRIVATO_NODE_2>:7002,<IP_PRIVATO_NODE_3>:7003" \
  -e AGGREGATION_TYPE="topk" \
  gossip-agent:local --config /app/configs/node1.yaml
```

**Sul terminale di Node-2:**
```bash
sudo docker run -d --name gossip-node2 \
  --cap-add=NET_ADMIN \
  -p 8002:8002 -p 7002:7002/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="<IP_PRIVATO_NODE_2>" \
  -e SEED_PEERS="<IP_PRIVATO_NODE_1>:7001,<IP_PRIVATO_NODE_3>:7003" \
  -e AGGREGATION_TYPE="topk" \
  gossip-agent:local --config /app/configs/node2.yaml
```

*(Replicare in modo logico i comandi per i nodi 3, 4 e 5, avendo cura di modificare i nomi dei container `gossip-nodeX`, i port mapping `-p 800X:800X -p 700X:700X/udp`, la config `--config /app/configs/nodeX.yaml` ed incrociando correttamente la lista dei `SEED_PEERS`)*.

#### 4. Verifica della Convergenza e Spegnimento Sicuro
Visitando `http://<IP_PUBBLICO_NODE_X>:800X/metrics` sul proprio browser si potrà osservare che i nodi aggiornano la topologia (marcando `Alive` gli IP privati degli altri) e convergono alla stessa stima aggregata.

Alla fine, per non consumare il budget del Learner Lab, arrestare i container inviando il segnale di Graceful Leave:
```bash
sudo docker stop gossip-node1
```
Infine, dalla console AWS EC2, selezionare le 5 macchine, cliccare su **Stato dell'istanza** ed eseguire **Arresta istanza**.

> **⚠️ COMPORTAMENTO SUI RIAVVII (AWS Learner Lab):** 
> Con l'uso degli IP Privati se si arrestano le istanze per non consumare budget e le si riaccendono in un secondo momento, **il cluster riprenderà a comunicare istantaneamente** senza dover modificare alcuna configurazione.
> AWS assegnerà tuttavia dei **nuovi Indirizzi IP Pubblici**: questo non intacca minimamente il funzionamento interno del cluster, ma sarà semplicemente necessario ricordarsi di usare il *nuovo* IP Pubblico nella barra del browser del proprio PC per poter consultare le metriche (`http://<NUOVO_IP_PUBBLICO>:800X/metrics`).

### 5. Cambiare l'Aggregazione Primaria (Multi-CRDT)
Con l'architettura Multi-CRDT il cluster calcola **sempre e simultaneamente** tutte e 5 le aggregazioni matematiche (`average`, `sum`, `min`, `max`, `topk`), esponendole nell'oggetto JSON `all_aggregations` dell'API `/metrics`.

Tuttavia, è possibile definire quale di queste 5 debba essere considerata l'aggregazione "primaria" (quella esposta nel campo principale `estimate`). Per cambiare la metrica primaria:

- **Se si usa l'Opzione A**: Grazie all'utilizzo delle variabili d'ambiente nel file Compose, non c'è alcun bisogno di modificare manualmente i file YAML. Per cambiare il tipo di aggregazione primaria su tutti gli 8 nodi contemporaneamente, è sufficiente lanciare il cluster anteponendo la variabile desiderata al comando. Ad esempio, per passare ad `average`:
  ```bash
  AGGREGATION_TYPE="average" docker-compose up -d
  ```
- **Se si usa l'Opzione B**: È sufficiente modificare la variabile d'ambiente direttamente nel comando di avvio, sostituendo il flag `-e AGGREGATION_TYPE="topk"` con l'operazione desiderata. Poiché Docker non permette di creare due container con lo stesso nome, **prima di avviare il nuovo nodo** assicurarsi di distruggere il precedente eseguendo:
  ```bash
  sudo docker rm -f gossip-nodeX
  ```

    **Quindi, ad esempio, sul terminale di Node-1:**
    ```bash
    sudo docker rm -f gossip-node1 
    
    sudo docker run -d --name gossip-node1 \
      --cap-add=NET_ADMIN \
      -p 8001:8001 -p 7001:7001/udp \
      -v $(pwd)/configs:/app/configs:ro \
      -e ADVERTISE_ADDR="<IP_PRIVATO_NODE_1>" \
      -e SEED_PEERS="<IP_PRIVATO_NODE_2>:7002,<IP_PRIVATO_NODE_3>:7003" \
      -e AGGREGATION_TYPE="average" \
      gossip-agent:local \
      --config /app/configs/node1.yaml
    ```

*Attenzione: Affinché il cluster esponga correttamente un valore primario omogeneo, è buona norma che tutti i nodi della rete vengano riavviati con il medesimo `AGGREGATION_TYPE`.*

### 6. Prove di Collaudo in Cloud
Con il cluster è operativo sulle macchine AWS, è possibile eseguire le medesime prove di stress illustrate nelle sezioni precedenti per verificarne il comportamento su una rete geografica:
- **Lettura dei Log**: Per ispezionare il payload JSON dei messaggi Gossip di uno specifico nodo, eseguire `sudo docker logs -f gossip-node1` (vedi [Sezione 4](#2-osservazione-dei-log)).
- **Fault Tolerance (Crash & Rejoin)**: Provare ad uccidere uno o più processi (es. `sudo docker kill gossip-node3`), osservare l'aggregazione variare in tempo reale dal browser, e successivamente riavviarlo/i per testarne la riconvergenza automatica (vedi [Sezione 7](#test-di-crash--rejoin)).
