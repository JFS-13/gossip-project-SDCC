# Guida Definitiva: Deploy del Cluster Gossip su AWS EC2

Questo documento è il tuo "manuale di istruzioni" passo-passo. Contiene esattamente tutti i click e i comandi da terminale che abbiamo usato per creare il cluster distribuito oggi. Seguendolo potrai ricreare l'ambiente in 5 minuti netti.

---

## 1. Avvio dell'Ambiente e Creazione Server
1. Vai su Canvas e apri **AWS Academy Learner Lab**.
2. Clicca su **Start Lab** e attendi che il pallino accanto ad "AWS" diventi VERDE. Clicca sulla scritta AWS per aprire la Console.
3. Cerca **EC2** nella barra di ricerca in alto e clicca su "Istanze".
4. Clicca sul bottone arancione **"Avvia istanze"**.

### Impostazioni per la creazione:
- **Nome**: Scrivi `Gossip-Node` (verrà assegnato a tutte, le rinomineremo dopo).
- **Numero di istanze**: Imposta **`3`** (così AWS crea tre computer identici in un colpo solo).
- **Immagine software (AMI)**: Scegli **Amazon Linux 2023**.
- **Tipo di istanza**: Lascia `t3.micro` (inclusa nel piano gratuito).
- **Coppia di chiavi (Key pair)**: Seleziona **`vockey`**.
- **Impostazioni di rete (Firewall / Security Group)**:
    - Scegli "Crea gruppo di sicurezza".
    - Lascia la regola SSH esistente.
    - Clicca *Aggiungi regola*: Tipo **UDP personalizzato**, Intervallo porte **`7001 - 7003`**, Origine **Ovunque (0.0.0.0/0)**. (Permette al Gossip di comunicare).
    - Clicca *Aggiungi regola*: Tipo **TCP personalizzato**, Intervallo porte **`8001 - 8003`**, Origine **Ovunque (0.0.0.0/0)**. (Permette di vedere le metriche dal browser).
- **Configura archiviazione**: Lascia 8 GiB.
- Clicca su **Avvia istanza**.

---

## 2. Rinomina e Raccolta IP
1. Torna all'elenco delle Istanze EC2 (ora ne vedrai 3 "In esecuzione").
2. Passa il mouse sul nome di ciascuna e clicca la matita per rinominarle in **Node-1**, **Node-2**, e **Node-3** per non confonderti.
3. Seleziona un nodo alla volta e appuntati su un blocco note il suo **Indirizzo IPv4 Pubblico**.

---

## 3. Preparazione delle Macchine (da ripetere su tutti e 3 i nodi)
Apri 3 schede del browser affiancate. In ognuna:
1. Seleziona il nodo dalla console AWS e clicca **Connetti** in alto.
2. Usa la scheda **EC2 Instance Connect** e premi Connetti per aprire il terminale nero.
3. Nel terminale, incolla questo blocco di comandi per installare gli strumenti necessari:

```bash
sudo dnf update -y
sudo dnf install git docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
newgrp docker
```

---

## 4. Download del Codice e Build dell'Immagine (da ripetere su tutti e 3 i nodi)
Sempre all'interno dei terminali, scarica il tuo progetto ed entra nella cartella:

```bash
git clone https://github.com/JFS-13/gossip-project-SDCC.git
cd gossip-project-SDCC
sudo docker build -t gossip-agent:local .
```
*(Questo passaggio costruisce materialmente il software a partire dal tuo `Dockerfile`)*.

---

## 5. Lancio del Cluster (Il momento magico)
Ora che il software è pronto sulle 3 macchine, non serve modificare file a mano. Lanciamo i container passando i tuoi IP Pubblici tramite variabili d'ambiente.
*(Assicurati di **sostituire** la parola `IP_PUBBLICO_NODE_...` con i veri IP numerici che ti sei appuntato al Passo 2).*

**Nel terminale di Node-1:**
```bash
sudo docker rm -f gossip-node1 

sudo docker run -d --name gossip-node1 \
  -p 8001:8001 -p 7001:7001/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="13.222.186.155" \
  -e SEED_PEERS="107.22.156.120:7002,3.87.87.65:7003" \
  -e AGGREGATION_TYPE="average" \
  gossip-agent:local \
  --config /app/configs/node1.yaml
```

**Nel terminale di Node-2:**
```bash
sudo docker rm -f gossip-node2 

sudo docker run -d --name gossip-node2 \
  -p 8002:8002 -p 7002:7002/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="107.22.156.120" \
  -e SEED_PEERS="13.222.186.155:7001,3.87.87.65:7003" \
  -e AGGREGATION_TYPE="average" \
  gossip-agent:local \
  --config /app/configs/node2.yaml
```

**Nel terminale di Node-3:**
```bash
sudo docker rm -f gossip-node3 

sudo docker run -d --name gossip-node3 \
  -p 8003:8003 -p 7003:7003/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="3.87.87.65" \
  -e SEED_PEERS="13.222.186.155:7001,107.22.156.120:7002" \
  -e AGGREGATION_TYPE="average" \
  gossip-agent:local \
  --config /app/configs/node3.yaml
```

**Nel terminale di Node-4:**
```bash
sudo docker rm -f gossip-node4 

sudo docker run -d --name gossip-node4 \
  -p 8004:8004 -p 7004:7004/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="54.91.99.160" \
  -e SEED_PEERS="13.222.186.155:7001,3.87.87.65:7003" \
  -e AGGREGATION_TYPE="average" \
  gossip-agent:local \
  --config /app/configs/node4.yaml
```

**Nel terminale di Node-5:**
```bash
sudo docker rm -f gossip-node5

sudo docker run -d --name gossip-node5 \
  -p 8005:8005 -p 7005:7005/udp \
  -v $(pwd)/configs:/app/configs:ro \
  -e ADVERTISE_ADDR="3.93.164.11" \
  -e SEED_PEERS="107.22.156.120:7002,54.91.99.160:7004" \
  -e AGGREGATION_TYPE="average" \
  gossip-agent:local \
  --config /app/configs/node5.yaml
```

---

## 6. Verifica del Funzionamento
Per dimostrare che il sistema distribuito è vivo e ha raggiunto il consenso:
1. Apri una nuova scheda del tuo browser su Windows/Mac.
2. Vai all'indirizzo `http://<IP_PUBBLICO_NODE_1>:8001/metrics`.
3. Dovrai leggere `"known_nodes":3` (il cluster si è trovato) e un `"estimate"` corrispondente al calcolo dell'aggregazione configurata.
4. Per leggere i log completi e vedere i numeri grezzi (es. l'intero array del Top-K), torna nel terminale di AWS e digita:
   `sudo docker logs gossip-node1`

---

## 7. Spegnimento Sicuro
Per non sprecare budget o tempo utile del Lab:
1. Dalla Console AWS, seleziona tutte e 3 le istanze.
2. Clicca su **Stato dell'istanza** in alto a destra e seleziona **Arresta istanza** (NON "Termina"). Non spuntare nulla nel popup, conferma semplicemente cliccando Arresta.
3. Torna su Canvas e premi **End Lab**.
