# Gossip-Based Distributed Data Aggregation

Questo progetto, realizzato per il corso di Sistemi Distribuiti e Cloud Computing, implementa un sistema distribuito decentralizzato (peer-to-peer) capace di aggregare dati in tempo reale utilizzando un **protocollo epidemico (Gossip)** accoppiato a strutture dati **CRDT (Convergent Replicated Data Types)**. I CRDT sono speciali strutture dati progettate per essere replicate su più macchine; garantiscono che tutti i nodi convergano matematicamente allo stesso stato finale indipendentemente dall'ordine di arrivo dei messaggi o dalla presenza di eventuali duplicati di rete.

L'obiettivo è ottenere una stima globale convergente (es. media dei carichi, ricerca del massimo, somma globale) all'interno di un cluster di nodi, senza alcun Single Point of Failure (SPOF) o nodo coordinatore.

---

## Indice
1. [Panoramica e Architettura](#panoramica-e-architettura)
2. [Configurazione del Nodo](#configurazione-del-nodo)
3. [Tipi di Aggregazione (CRDT)](#tipi-di-aggregazione-crdt)
4. [Quickstart End-to-End (Docker Compose)](#quickstart-end-to-end-docker-compose)
5. [Esecuzione Manuale Locale](#esecuzione-manuale-locale)
6. [Esecuzione e Struttura dei Test](#esecuzione-e-struttura-di-test)
7. [Fault Injection e Split-Brain](#fault-injection-e-split-brain)
8. [Osservabilità e Metriche](#osservabilità-e-metriche)
9. [Deploy su AWS Learner Lab](#deploy-su-aws-learner-lab)

---

## Panoramica e Architettura

Ogni nodo è un'istanza indipendente scritta in Go. Periodicamente, ogni nodo sceglie casualmente $K$ peer (parametro `fanout`) ed invia loro il proprio stato interno tramite protocollo **UDP**. 
L'architettura è divisa nei seguenti layer:

- **Transport**: Gestisce l'invio e la ricezione asincrona dei pacchetti UDP serializzati in JSON. Basandosi su un'interfaccia astratta, permette di iniettare dinamicamente implementazioni diverse: oltre al layer UDP reale, include lo stub NoopTransport per gli unit test ed un virtual switch in memoria (basato su Go Channels) per gli integration test.
- **Topology**: Gestisce l'appartenenza al cluster implementando un meccanismo di *Failure Detection* basato sul protocollo **SWIM** (Scalable Weakly-consistent Infection-style Process Group Membership). Si tratta di un protocollo in cui i nodi si monitorano a vicenda in modo decentralizzato, segnando lo stato dei peer come `Alive`, `Suspect` o `Dead` tramite dei timeout. Tramite *piggybacking*, ogni messaggio gossip trasporta anche la vista della topologia del mittente, permettendo la scoperta dinamica dei nuovi nodi.
- **Message**: Definisce i modelli dati scambiati sulla rete (il payload GossipMessage), tra cui la struttura dello stato CRDT condiviso (AggregationState). Qui risiede la definizione di Epoch e Version, ovvero degli orologi logici scalari utilizzati per imporre un ordine totale agli aggiornamenti e risolvere in modo deterministico i conflitti.
- **Aggregation**: Motore matematico idempotente. Garantisce che il risultato dell'aggregazione finale rimanga matematicamente corretto e convergente anche se i pacchetti UDP arrivano duplicati o fuori ordine.
- **Core**: Cuore pulsante dell'istanza (event loop), innescato da un time ticker su una goroutine dedicata. Coordina il campionamento casuale del fanout per i round epidemici, orchestra il Merge asincrono (protetto da lock) degli stati CRDT, ed attua meccanismi di auto-guarigione (come il ping stocastico verso i seed originali) per sanare eventuali partizioni di rete o scenari di split-brain.
- **Setup**: Modulo dedicato al caricamento ed alla validazione stretta dei parametri operativi. La configurazione viene risolta tramite una catena di fallback: valori di default, parsing di un file YAML ed infine sovrascrittura tramite variabili d'ambiente (strategia per il deploy su Docker Compose ed AWS).
- **Telemetry**: Sottosistema responsabile del logging strutturato in formato JSON (tramite log/slog) e dell'esposizione asincrona di un server HTTP per l'osservabilità. Espone l'endpoint di liveness /health e l'endpoint /metrics, che permette di interrogare in tempo reale il nodo per conoscere il valore della stima aggregata (estimate), il numero di peer vivi visti dal nodo (known_nodes), il round di esecuzione corrente e l'epoch.

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
- `advertise_address`: - `advertise_addr`: L'hostname o l'IP pubblico che questo nodo comunicherà al resto del cluster per farsi contattare. In Docker, coincide tipicamente con il nome del container/servizio DNS.
- `node_port`: La porta UDP utilizzata dal nodo sia per l'ascolto che per l'invio dei datagrammi gossip.
- `seed_peers`: Una lista iniziale di nodi noti (bootstrap). Un nodo appena avviato inizierà a contattare questi endpoint per presentarsi, venendo così scoperto dinamicamente dal resto della rete P2P.
- `gossip_interval_ms`: Frequenza (in millisecondi) con cui si ripete l'event loop epidemico.
- `fanout`: Numero di peer scelti casualmente dalla tabella di routing a cui inviare il proprio stato durante ogni round di gossip.
- `membership_timeout_ms`: Tempo massimo di silenzio (in millisecondi) superato il quale un peer viene declassato da `Alive` a `Suspect` (e, dopo un ulteriore periodo, a `Dead`) dal Failure Detector SWIM.
- `aggregation_type`: Specifica la funzione matematica CRDT da istanziare per il calcolo globale (valori supportati: `sum`, `average`, `min`, `max`, `topk`).
- `initial_value`: Il contributo numerico iniziale che questo specifico nodo immette nell'aggregazione.
- `top_k_size`: Parametro utilizzato unicamente quando l'aggregatore scelto è `topk`. Stabilisce la dimensione assoluta $K$ della classifica da calcolare (es. i 3 valori più alti dell'intera rete).
- `log_level`: Livello di verbosità del logger strutturato JSON (`debug`, `info`, `warn`, `error`).

---

## Tipi di Aggregazione (CRDT)

Modificando il campo `aggregation` nel file YAML, si altera il comportamento dell'intero cluster. Sono supportate le seguenti funzioni matematiche:

1. **`sum`**: Somma globale e distribuita di tutti i valori iniziali dei nodi attivi.
2. **`average`**: Media globale convergente.
3. **`min` / `max`**: Elezione distribuita del valore più basso o più alto nella rete.
4. **`topk`**: Trova i *K* valori assoluti più alti presenti nel sistema. Configurabile via `topk_size` nello YAML.

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
docker-compose up -d --build
```
Questo comando:
- Compila il binario Go all'interno di container isolati.
- Crea una subnet privata DNS chiamata `gossip-net`.
- Lancia gli 8 servizi (`node1` fino a `node8`) iniettando ad ognuno il proprio file YAML (es. `configs/node1.yaml`).

### 2. Osservazione dei Log
```bash
docker-compose logs -f
```
È possibile visualizzare un log strutturato simile al seguente:
```
node1 | time=... level=INFO msg="stima corrente" estimate=30.0000 known_nodes=8 round=15
```
I nodi partono con valori iniziali diversi e raggiungono il consenso distribuito rapidamente, mostrando tutti lo stesso valore `estimate`.

### 3. Spegnimento
```bash
docker-compose down
```

---

## Esecuzione Manuale Locale

Se la toolchain Go è installata (`>= 1.26`), è possibile eseguire le istanze manualmente aprendo più terminali.

Nel Terminale 1 (Nodo A):
```bash
go run ./cmd/agent/main.go --config configs/node1.yaml
```

Nel Terminale 2 (Nodo B):
```bash
go run ./cmd/agent/main.go --config configs/node2.yaml
```

Il nodo B, utilizzando la direttiva `seed_peers` verso la porta locale del nodo A, lo scoprirà e i due inizieranno a unire i propri stati tramite protocollo gossip.

---

## Esecuzione e Struttura dei Test

L'intera architettura è coperta da una forte suite di unit test e integration test basata su un'interfaccia Transport *In-Memory*, la quale simula il traffico di rete in memoria senza allocare vere porte UDP.

### Comando generale per l'esecuzione
```bash
go test ./... -v
```

### Struttura dei Test
- **`tests/aggregation/...`**: Verifica puramente matematica. Assicura che le formule di unione (merge CRDT) ignorino eventuali messaggi duplicati, preferiscano sempre il payload con la versione più alta e applichino correttamente le regole di idempotenza.
- **`tests/topology/...`**: Analizza il "Topology Manager" isolato, verificando l'esattezza delle transizioni di stato (`Alive -> Suspect -> Dead`) a seguito di timeout simulati.
- **`tests/integration/...`**: Rappresenta la suite **end-to-end**. Inizializza interi nodi connessi a uno switch virtuale. Comprende test complessi come:
  - `TestClusterConvergenza_Average` e `TestClusterConvergenza_Sum`: Avviano molteplici nodi e verificano che tutti raggiungano lo stesso aggregato convergente.
  - `TestRobustezza_PartizioneRete`: Simula la divisione della rete in due sottoreti isolate per testare i meccanismi di healing (Split-Brain).
  - `TestRobustezza_MessaggiDuplicati`: Iniettano pacchetti rindondanti per provare l'efficacia del CRDT contro le anomalie tipiche del protocollo UDP.

---

## Fault Injection

Questa sezione illustra come collaudare manualmente la robustezza del codice introducendo guasti nel cluster su Docker. 
Per facilitare queste operazioni, nella cartella `scripts/` sono forniti degli strumenti dedicati:

1. **`fault_dashboard.ps1` (o `.sh`)**: Un pannello interattivo a riga di comando che elenca i container in esecuzione e permette di spegnerli (`Stop`) o riavviarli (`Start`) premendo semplicemente un tasto.
2. **`auto_crash_test.sh`**: Uno script di Chaos Engineering automatizzato che fa cadere casualmente i nodi e ne verifica i tempi di riconvergenza, garantendo che lo stato sopravviva in maniera automatizzata.

### Test Manuale di Crash & Rejoin
Mentre il cluster è in esecuzione (`docker-compose up -d`), è possibile arrestare un'istanza forzatamente usando lo script o il comando:
```bash
docker stop gossip-node3
```
Comportamento atteso:
- I pacchetti destinati a `node3` iniziano a cadere.
- Entro la soglia di `membership_timeout_ms`, i nodi adiacenti declasseranno `node3` a `Suspect` e, conseguentemente, a `Dead`.
- Appena il nodo viene etichettato come morto, i nodi vivi ricalcolano l'aggregazione astraendo il contributo di `node3` dal totale globale.

Riavviando il nodo (`docker start gossip-node3`):
- `node3` si risveglia in modalità "stateless". Genera un nuovo `Incarnation Number` basato sull'Epoch time corrente (superiore a quello in cache negli altri nodi).
- Gli altri nodi ricevono il gossip, confrontano l'Incarnation, rimuovono lo stato `Dead` dalla tabella e l'intera rete riconverge nuovamente.

---

## Osservabilità e Metriche

L'Engine espone nativamente un server HTTP integrato, pensato per l'esposizione di **metriche**, ovvero un riepilogo in tempo reale dello stato di salute e dei parametri calcolati dall'applicazione (stima attuale, numero di peers attivi).

Se un nodo è configurato sulla porta UDP `7001`, il suo server HTTP verrà avviato per convenzione sulla porta `7001 + 1000 = 8001`. 
*Si fa così per evitare conflitti tra il traffico Gossip (UDP) e il traffico di monitoraggio (TCP) sulla stessa istanza, ricavando deterministicamente la porta di servizio senza richiedere configurazioni separate.*

Endpoints disponibili:
- **`GET /health`**: Restituisce `{ "status": "ok" }` se l'istanza è responsiva.
- **`GET /metrics`**: Esporta in formato **JSON** il valore corrente calcolato dal CRDT, i tick completati e il numero di peer riconosciuti.

È possibile interrogare l'istanza localmente eseguendo:
```bash
curl http://localhost:8001/metrics
```

---

## Deploy su AWS Learner Lab

Questa sezione spiega passo-passo come distribuire il progetto in ambiente cloud su un'istanza **Amazon EC2**, in modo da testare il cluster (ridimensionato per il collaudo a **5 macchine/nodi containerizzati**).

### 1. Prerequisiti su AWS
- **Istanza EC2**: Lanciare un'istanza di tipo `t3.micro (o t2.micro)` (idonea al Free Tier del Learner Lab) utilizzando l'OS **Amazon Linux 2023** (o Ubuntu).
- **Security Group**: Affinché sia possibile interrogare i nodi dall'esterno, è necessario aprire le seguenti porte nelle regole di *Inbound* (In entrata):
  - `TCP 22`: Per l'accesso SSH.
  - `TCP 8001-8005`: Per permettere l'accesso agli endpoint `/metrics` dei 5 nodi containerizzati.
  - *(Le porte UDP 7001-7005 non devono essere esposte al pubblico, in quanto i nodi comunicheranno tra loro internamente alla subnet virtuale creata da Docker).*

### 2. Installazione di Docker sull'Istanza
Connettersi all'istanza via SSH usando la chiave `.pem` fornita dal Learner Lab:
```bash
ssh -i "vockey.pem" ec2-user@<IP-PUBBLICO-EC2>
```
Su Amazon Linux 2023, installare Docker e avviare il servizio:
```bash
sudo yum update -y
sudo yum install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
```
*(Nota: Disconnettersi e riconnettersi all'SSH per applicare i permessi del gruppo docker).*

### 3. Trasferimento dei File
Copiare l'intera cartella del progetto dal proprio computer locale all'istanza EC2 usando `scp`:
```bash
scp -i "vockey.pem" -r ./gossip-project ec2-user@<IP-PUBBLICO-EC2>:/home/ec2-user/
```

### 4. Avvio del Cluster
Entrare nella cartella trasferita ed eseguire il build e lo start del cluster Docker Compose in background. Nel collaudo AWS, limiteremo l'avvio a **5 macchine** per evitare sovraccarichi sulla t2.micro:
```bash
cd gossip-project
docker compose up -d --build node1 node2 node3 node4 node5
```
Verificare che i 5 container siano in esecuzione:
```bash
docker compose ps
```

### 5. Verifica della Convergenza (Osservabilità)
Con il cluster attivo su EC2, è possibile verificarne la convergenza dal proprio computer locale o dal browser, semplicemente interrogando l'IP pubblico dell'istanza sulle porte TCP aperte precedentemente:
```bash
curl http://<IP-PUBBLICO-EC2>:8001/metrics
curl http://<IP-PUBBLICO-EC2>:8005/metrics
```
Entrambi i nodi dovranno rispondere con un JSON che mostrerà la medesima `estimate` e la conoscenza condivisa dell'intera topologia (`"known_nodes": 5`).

### 6. Shutdown e Pulizia
Per fermare l'esperimento, eseguire un graceful shutdown liberando le risorse (fondamentale per non sprecare il budget accademico del Learner Lab):
```bash
docker compose down
```
