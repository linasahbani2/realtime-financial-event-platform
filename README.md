# Real-Time Financial Intelligence & Event Processing Platform (RT-FIEP)

Plateforme de traitement d'événements financiers en temps réel, avec détection
d'anomalies. Projet pédagogique en cours de construction.

## Architecture (MVP actuel)

producer.py --> Kafka (financial.transactions)
|
v
Flink (job.py)
- detection montant anormal (regle 1)
- detection velocite anormale (regle 2)
|
v
Kafka (financial.alerts)
|
+-------------+-------------+
v v
alerts_consumer.py consumer.py
--> table "alerts" --> table "transactions"
(PostgreSQL) (PostgreSQL)


## Services (Docker)

| Service     | Role                                  | Port local |
|-------------|----------------------------------------|------------|
| kafka       | broker de messagerie                   | 9092       |
| kafka-ui    | interface web pour Kafka               | 8090       |
| postgres    | base de donnees                        | 5432       |
| jobmanager  | coordinateur Flink                     | 8091       |
| taskmanager | executant Flink                        | -          |

## Detection d'anomalies

- **AMOUNT_ANOMALY** : montant superieur a 3x la moyenne des 5 dernieres
  transactions du client.
- **VELOCITY_ANOMALY** : 8 transactions ou plus du meme client en moins de
  10 secondes.

## Prerequis

- Docker et Docker Compose
- Python 3.10 (requis specifiquement pour `flink-jobs`, a cause de PyFlink)
- Java 11 (requis par Flink)

## Lancement

1. Demarrer les services :
```bash
   cd docker
   docker compose up -d
```

2. Creer un fichier `.env` a la racine du projet (voir `.env.example`).

3. Dans 3 terminaux separes, avec le `venv` de chaque dossier active :
```bash
   # Terminal 1
   cd flink-jobs && python job.py

   # Terminal 2
   cd consumer-service && python3 alerts_consumer.py
   cd consumer-service && python3 consumer.py   # dans un 4e terminal, optionnel

   # Terminal 3
   cd event-generator && python3 producer.py
```

## Etat d'avancement

- [x] Simulateur de transactions (producer)
- [x] Kafka (topics, partitions)
- [x] Flink : detection montant anormal + velocite anormale, avec etat par client
- [x] Stockage PostgreSQL (transactions + alertes)
- [ ] requirements.txt par service
- [ ] Scenarios d'anomalies supplementaires
- [ ] Machine Learning
- [ ] Observabilite (Prometheus / Grafana)