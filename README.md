# Bank App — Architecture Microservices

Projet réalisé dans le cadre du cours **SoA et Microservices**  
Enseignant : Dr. Salah Gontara | A.U. : 2025-26 | Classe : 4Info

---

## Description

Application bancaire simplifiée basée sur une architecture **microservices** en Node.js.  
Elle permet de gérer des comptes bancaires, effectuer des transactions (dépôt, retrait, virement)  
et consulter l'historique des opérations en temps réel via Kafka.

Le projet est entièrement **conteneurisé avec Docker Compose** — une seule commande suffit pour tout lancer.

---

## Architecture

```
Client (Postman)
    | REST / GraphQL (HTTP/1.1 + JSON)
API Gateway (port 3000)
    | gRPC (HTTP/2 + Protobuf)
    |-- MS1 Comptes      (port 50051) -> SQLite3 (données persistantes)
    |-- MS2 Transactions (port 50052) -> SQLite3 (données persistantes) + Kafka Producer
    |-- MS3 Historique   (port 50053) -> RxDB (NoSQL) + Kafka Consumer

Kafka Broker (port 9092)
    |-- Topic : transaction.done
```

---

## Technologies utilisées

| Composant | Technologie |
|---|---|
| Langage | Node.js |
| API Gateway | Express.js + Apollo Server |
| Communication interne | gRPC + Protobuf (HTTP/2) |
| Communication asynchrone | Apache Kafka (KRaft mode) |
| Base de données SQL | SQLite3 |
| Base de données NoSQL | RxDB |
| Conteneurisation | Docker + Docker Compose |
| Test API | Postman |

---

## Structure du projet

```
bank-app/
|-- proto/
|   |-- account.proto          <- contrat gRPC du service Comptes
|   |-- transaction.proto      <- contrat gRPC du service Transactions
|   |-- history.proto          <- contrat gRPC du service Historique
|-- ms1-comptes/
|   |-- Dockerfile
|   |-- .dockerignore
|   |-- package.json
|   |-- index.js               <- gRPC server + SQLite3
|-- ms2-transactions/
|   |-- Dockerfile
|   |-- .dockerignore
|   |-- package.json
|   |-- index.js               <- gRPC server + SQLite3 + Kafka producer
|-- ms3-historique/
|   |-- Dockerfile
|   |-- package.json
|   |-- index.js               <- gRPC server + RxDB + Kafka consumer
|-- api-gateway/
|   |-- Dockerfile
|   |-- package.json
|   |-- schema.gql             <- schema GraphQL
|   |-- index.js               <- Express + Apollo + gRPC clients
|-- docker-compose.yml         <- orchestration de tous les services
|-- README.md
```

---

## Installation

### Prerequis
- Docker Desktop installe et demarre
- Git
- Postman (pour tester)

### Cloner le projet
```bash
git clone https://github.com/TON_USERNAME/bank-app.git
cd bank-app
```

---

## Execution avec Docker (recommande)

### Demarrer tous les services en une seule commande
```bash
docker compose up --build
```

Docker va automatiquement :
- Demarrer Kafka en mode KRaft
- Construire et demarrer les 3 microservices
- Demarrer l'API Gateway
- Creer le topic Kafka transaction.done
- Persister les donnees SQLite3 entre les redemarrages

### Arreter les services
```bash
docker compose down
```

Les donnees SQLite3 sont persistantes grace aux volumes Docker.

---

## Execution sans Docker (developpement)

### 1. Demarrer Kafka
```bash
cd C:\kafka_2.13-4.2.0
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 2. Creer le topic Kafka
```bash
.\bin\windows\kafka-topics.bat --create --topic transaction.done --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Installer les dependances
```bash
cd ms1-comptes && npm install && cd ..
cd ms2-transactions && npm install && cd ..
cd ms3-historique && npm install && cd ..
cd api-gateway && npm install && cd ..
```

### 4. Demarrer dans l'ordre
```bash
cd ms1-comptes && node index.js      # Terminal 1
cd ms2-transactions && node index.js # Terminal 2
cd ms3-historique && node index.js   # Terminal 3
cd api-gateway && node index.js      # Terminal 4
```

---

## Endpoints REST

### Comptes
| Methode | Endpoint | Description | Body |
|---|---|---|---|
| GET | /accounts | Lister tous les comptes | - |
| GET | /accounts/:id | Consulter un compte | - |
| POST | /accounts | Creer un compte | { owner, balance, currency } |

### Transactions
| Methode | Endpoint | Description | Body |
|---|---|---|---|
| POST | /transactions/deposit | Depot | { accountId, amount } |
| POST | /transactions/withdraw | Retrait | { accountId, amount } |
| POST | /transactions/transfer | Virement | { fromAccountId, toAccountId, amount } |

### Historique
| Methode | Endpoint | Description |
|---|---|---|
| GET | /history/:accountId | Historique complet d'un compte |

---

## Schema GraphQL

### Queries
```graphql
account(id: String!): Account
accounts: [Account]
history(accountId: String!): [HistoryEntry]
```

### Mutations
```graphql
createAccount(owner: String!, balance: Float!, currency: String!): Account
deposit(accountId: String!, amount: Float!): Transaction
withdraw(accountId: String!, amount: Float!): Transaction
transfer(fromAccountId: String!, toAccountId: String!, amount: Float!): Transaction
```

---

## Topics Kafka

| Topic | Producteur | Consommateur | Contenu |
|---|---|---|---|
| transaction.done | MS2 | MS3 | id, type, accountId, amount, newBalance, timestamp |

### Scenario metier
1. Client fait une transaction via REST ou GraphQL
2. API Gateway appelle MS2 via gRPC
3. MS2 met a jour le solde dans MS1 via gRPC
4. MS2 publie sur Kafka transaction.done
5. MS3 consomme et enregistre dans RxDB
6. Client consulte l'historique via REST ou GraphQL

---

## Bases de donnees

| Microservice | Type | Emplacement | Collection |
|---|---|---|---|
| MS1 Comptes | SQLite3 (SQL) | /app/data/accounts.db | accounts |
| MS2 Transactions | SQLite3 (SQL) | /app/data/transactions.db | transactions |
| MS3 Historique | RxDB (NoSQL) | En memoire | history |

---

## Docker

### Services
| Container | Port |
|---|---|
| kafka | 9092 |
| ms1-comptes | 50051 |
| ms2-transactions | 50052 |
| ms3-historique | 50053 |
| api-gateway | 3000 |

### Commandes utiles
```bash
docker compose up --build        # Demarrer
docker compose down              # Arreter (donnees conservees)
docker compose down -v           # Arreter + supprimer donnees
docker compose logs ms1-comptes  # Voir logs d'un service
docker compose ps                # Voir containers actifs
```

---

## Auteur

- Nom : Oumaima Mdaini
- Classe : 4émeGL-Groupe1
- GitHub : https://github.com/OumaimaMd