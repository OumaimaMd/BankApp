# 🏦 Bank App — Architecture Microservices

Projet réalisé dans le cadre du cours **SoA et Microservices**  
Enseignant : Dr. Salah Gontara | A.U. : 2025-26

---

## 📋 Description

Application bancaire simplifiée basée sur une architecture microservices en Node.js.  
Elle permet de gérer des comptes bancaires, effectuer des transactions (dépôt, retrait, virement)  
et consulter l'historique des opérations en temps réel via Kafka.

---

## 🏗️ Architecture
Client (Postman)
↓ REST / GraphQL
API Gateway (port 3000)
↓ gRPC
├── MS1 · Comptes      (port 50051) → SQLite3
├── MS2 · Transactions (port 50052) → SQLite3 + Kafka Producer
└── MS3 · Historique   (port 50053) → RxDB    + Kafka Consumer
Kafka Broker (port 9092)
Topic : transaction.done
---

## 🛠️ Technologies utilisées

| Composant | Technologie |
|---|---|
| Langage | Node.js |
| API Gateway | Express.js + Apollo Server |
| Communication interne | gRPC + Protobuf |
| Communication async | Apache Kafka (KRaft) |
| Base de données SQL | SQLite3 |
| Base de données NoSQL | RxDB |
| Test API | Postman |

---

## 📁 Structure du projet
bank-app/
├── proto/
│   ├── account.proto
│   ├── transaction.proto
│   └── history.proto
├── ms1-comptes/
│   └── index.js          ← gRPC server + SQLite3
├── ms2-transactions/
│   └── index.js          ← gRPC server + SQLite3 + Kafka producer
├── ms3-historique/
│   └── index.js          ← gRPC server + RxDB + Kafka consumer
└── api-gateway/
├── index.js           ← Express + Apollo + gRPC clients
└── schema.gql         ← Schéma GraphQL
---

## ⚙️ Installation

### Prérequis
- Node.js v20+
- Apache Kafka 4.2 (KRaft mode)
- Git

### Cloner le projet
```bash
git clone https://github.com/TON_USERNAME/bank-app.git
cd bank-app
```

### Installer les dépendances
```bash
cd ms1-comptes && npm install && cd ..
cd ms2-transactions && npm install && cd ..
cd ms3-historique && npm install && cd ..
cd api-gateway && npm install && cd ..
```

---

## 🚀 Exécution

### 1. Démarrer Kafka
```bash
cd C:\kafka_2.13-4.2.0
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 2. Créer le topic Kafka
```bash
.\bin\windows\kafka-topics.bat --create --topic transaction.done --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Démarrer les microservices (dans l'ordre)
```bash
# Terminal 1
cd ms1-comptes && node index.js

# Terminal 2
cd ms2-transactions && node index.js

# Terminal 3
cd ms3-historique && node index.js

# Terminal 4
cd api-gateway && node index.js
```

---

## 📡 Endpoints REST

### Comptes
| Méthode | Endpoint | Description |
|---|---|---|
| GET | /accounts | Lister tous les comptes |
| GET | /accounts/:id | Consulter un compte |
| POST | /accounts | Créer un compte |

### Transactions
| Méthode | Endpoint | Description |
|---|---|---|
| POST | /transactions/deposit | Dépôt |
| POST | /transactions/withdraw | Retrait |
| POST | /transactions/transfer | Virement |

### Historique
| Méthode | Endpoint | Description |
|---|---|---|
| GET | /history/:accountId | Historique d'un compte |

---

## 🔷 Schéma GraphQL

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

## 📨 Topics Kafka

| Topic | Producteur | Consommateur | Contenu |
|---|---|---|---|
| transaction.done | MS2 Transactions | MS3 Historique | id, type, accountId, amount, newBalance, timestamp |

### Scénario métier
Quand une transaction est effectuée (dépôt, retrait, virement) :
1. MS2 exécute la transaction
2. MS2 publie un événement sur `transaction.done`
3. MS3 consomme l'événement et enregistre l'opération dans l'historique

---

## 🗄️ Bases de données

| Microservice | Type | Fichier | Tables/Collections |
|---|---|---|---|
| MS1 Comptes | SQLite3 | accounts.db | accounts |
| MS2 Transactions | SQLite3 | transactions.db | transactions |
| MS3 Historique | RxDB (NoSQL) | mémoire | history |

---

## 👤 Auteur

- **Nom :** Oumaima Mdaini
- **Classe :** 4émeGL-Groupe1
- **GitHub :** https://github.com/OumaimaMd