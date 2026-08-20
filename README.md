# Gossip-Based Distributed Data Aggregation

Questo progetto implementa un sistema distribuito decentralizzato (peer-to-peer) capace di aggregare dati in tempo reale utilizzando un **protocollo epidemico (Gossip)** accoppiato a strutture dati **CRDT (Convergent Replicated Data Types)**. I CRDT sono speciali strutture dati progettate per essere replicate su più macchine; garantiscono che tutti i nodi convergano matematicamente allo stesso stato finale indipendentemente dall'ordine di arrivo dei messaggi o dalla presenza di eventuali duplicati di rete.

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

Ogni nodo è un'istanza indipendente scritta in Go. Periodicamente, ogni nodo sceglie a caso $K$ peer (parametro `fanout`) e invia loro il proprio stato interno su protocollo **UDP**. 
L'architettura si divide in layer ben definiti:

- **Transport**: Gestisce l'invio e la ricezione non bloccante dei pacchetti UDP serializzati in JSON.
- **Topology**: Gestisce l'appartenenza al cluster implementando un meccanismo di *Failure Detection* basato sul protocollo **SWIM** (Scalable Weakly-consistent Infection-style Process Group Membership). Si tratta di un protocollo in cui i nodi si monitorano a vicenda in modo decentralizzato, segnando lo stato dei peer come `Alive`, `Suspect` o `Dead` tramite dei timeout. Tramite *piggybacking*, ogni messaggio gossip trasporta anche la vista della topologia del mittente, garantendo la scoperta dinamica dei nuovi nodi.
- **Aggregation (CRDT)**: Motore matematico idempotente. Garantisce che il risultato dell'aggregazione finale rimanga matematicamente corretto e convergente anche se i pacchetti UDP arrivano duplicati o fuori ordine.
- **Core Engine**: Coordina i tick periodici, unisce gli stati ricevuti e gestisce le transizioni (come il ping stocastico ai seed originali per prevenire partizioni di rete permanenti).
- **Setup**: Modulo dedicato al caricamento e alla validazione stretta dei parametri di configurazione passati tramite file YAML.
- **Telemetry**: Sottosistema responsabile del logging strutturato (tramite `slog`) e dell'esposizione del server HTTP per le metriche.

---

## Configurazione del Nodo

Il nodo è progettato per essere interamente configurabile all'avvio tramite file **YAML**. Il percorso del file si specifica con il flag `--config`. Nel repository sono presenti configurazioni di esempio all'interno della cartella `configs/` (es. `node1.yaml`, `node2.yaml`).

Esempio di file `node1.yaml`:
```yaml
node_id: "node-1"
bind_address: "0.0.0.0"
node_port: 7001
advertise_address: "node1:7001"
gossip_interval_ms: 1000
fanout: 2
aggregation: "average"
initial_value: 10.0
seed_peers:
  - "node2:7002"
  - "node3:7003"
membership_timeout_ms: 5000
cleanup_timeout_ms: 15000
```

### Parametri Chiave
- `node_id`: Nome logico univoco del nodo (es. `node-1`). È fondamentale che ogni nodo abbia un ID distinto.
- `advertise_address`: L'endpoint reale (`host:porta`) che gli altri nodi devono usare per contattare questa istanza. In Docker, coincide con il nome del servizio DNS.
- `fanout`: Numero di peer contattati a ogni round di gossip.
- `gossip_interval_ms`: Frequenza del ciclo di gossip in millisecondi.
- `aggregation` e `initial_value`: Tipo di calcolo da effettuare e il valore locale iniziale che questo nodo immette nel sistema.
- `seed_peers`: Una lista iniziale di nodi noti (bootstrap). Un nuovo nodo inizierà a "gossippare" verso questi endpoint, venendo a sua volta scoperto dagli altri.

---

## Tipi di Aggregazione (CRDT)

Modificando il campo `aggregation` nel file YAML, si altera il comportamento dell'intero cluster. Sono supportate le seguenti funzioni matematiche:

1. **`sum`**: Somma globale e distribuita di tutti i valori iniziali dei nodi attivi.
2. **`average`**: Media globale convergente.
3. **`min` / `max`**: Elezione distribuita del valore più basso o più alto nella rete.
4. **`topk`**: Trova i *K* valori assoluti più alti presenti nel sistema. Configurabile via `topk_size` nello YAML.

*Nota operazionale: un nodo etichettato come `Dead` (che non risponde per il tempo di timeout) non viene rimosso fisicamente dalla memoria CRDT, ma il suo contributo viene filtrato a runtime, smettendo di pesare sul risultato matematico finché il nodo non torna `Alive`.*

---

## Quickstart End-to-End (Docker Compose)

Per eseguire l'intero cluster in un ambiente isolato è possibile utilizzare **Docker Compose**. Alla radice del progetto è presente un `docker-compose.yml` preconfigurato per avviare **8 nodi**.

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

Se la toolchain Go è installata (`>= 1.22`), è possibile eseguire le istanze manualmente aprendo più terminali.

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
