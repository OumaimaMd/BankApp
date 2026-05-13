const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { Kafka } = require('kafkajs');

const PROTO_PATH = process.env.PROTO_PATH || path.join(__dirname, '../proto');
//Charger les protos 
const packageDef = protoLoader.loadSync(
  path.join(__dirname, '../proto/transaction.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const transactionProto = grpc.loadPackageDefinition(packageDef).transaction;

const accountPackageDef = protoLoader.loadSync(
  path.join(__dirname, '../proto/account.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const accountProto = grpc.loadPackageDefinition(accountPackageDef).account;

//Base de données SQLite3
const db = new sqlite3.Database('/app/data/transactions.db', (err) => {
  if (err) console.error(err);
  else console.log('SQLite3 connecté');
});

db.run(`CREATE TABLE IF NOT EXISTS transactions (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL,
  accountId   TEXT NOT NULL,
  amount      REAL NOT NULL,
  toAccountId TEXT,
  newBalance  REAL,
  timestamp   TEXT NOT NULL
)`);

//  Client gRPC vers MS1 
const accountClient = new accountProto.AccountService(
  `${process.env.MS1_HOST || 'localhost'}:50051`,
  grpc.credentials.createInsecure()
);

function grpcCall(client, method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

//  Kafka Producer 
const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] });
const producer = kafka.producer();

async function startKafka() {
  await producer.connect();
  console.log('Kafka producer connecté');
}
startKafka();

function generateId() {
  return 'txn-' + Date.now();
}

async function publishTransaction(data) {
  await producer.send({
    topic: 'transaction.done',
    messages: [{ key: data.accountId, value: JSON.stringify(data) }],
  });
  console.log('Kafka publié:', data);
}

//  Implémentation gRPC 
const service = {

  Deposit: async (call, callback) => {
    try {
      const { accountId, amount } = call.request;

      // 1. Récupérer le compte depuis MS1
      const accountRes = await grpcCall(accountClient, 'GetAccount', { id: accountId });
      const account = accountRes.account;
      const newBalance = account.balance + amount;

      // 2. Mettre à jour le solde dans MS1
      await grpcCall(accountClient, 'UpdateBalance', { id: accountId, balance: newBalance });

      // 3. Sauvegarder la transaction
      const id = generateId();
      const timestamp = new Date().toISOString();
      db.run(
        `INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [id, 'deposit', accountId, amount, null, newBalance, timestamp]
      );

      // 4. Publier sur Kafka
      await publishTransaction({ id, type: 'deposit', accountId, amount, newBalance, timestamp });

      callback(null, { newBalance, transactionId: id });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  Withdraw: async (call, callback) => {
    try {
      const { accountId, amount } = call.request;

      const accountRes = await grpcCall(accountClient, 'GetAccount', { id: accountId });
      const account = accountRes.account;

      if (account.balance < amount) {
        return callback({ code: grpc.status.FAILED_PRECONDITION, message: 'Solde insuffisant' });
      }

      const newBalance = account.balance - amount;
      await grpcCall(accountClient, 'UpdateBalance', { id: accountId, balance: newBalance });

      const id = generateId();
      const timestamp = new Date().toISOString();
      db.run(
        `INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [id, 'withdrawal', accountId, amount, null, newBalance, timestamp]
      );

      await publishTransaction({ id, type: 'withdrawal', accountId, amount, newBalance, timestamp });

      callback(null, { newBalance, transactionId: id });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },

  Transfer: async (call, callback) => {
    try {
      const { fromAccountId, toAccountId, amount } = call.request;

      // Vérifier le compte source
      const fromRes = await grpcCall(accountClient, 'GetAccount', { id: fromAccountId });
      const fromAccount = fromRes.account;

      if (fromAccount.balance < amount) {
        return callback({ code: grpc.status.FAILED_PRECONDITION, message: 'Solde insuffisant' });
      }

      // Mettre à jour les deux comptes
      const newFromBalance = fromAccount.balance - amount;
      await grpcCall(accountClient, 'UpdateBalance', { id: fromAccountId, balance: newFromBalance });

      const toRes = await grpcCall(accountClient, 'GetAccount', { id: toAccountId });
      const newToBalance = toRes.account.balance + amount;
      await grpcCall(accountClient, 'UpdateBalance', { id: toAccountId, balance: newToBalance });

      const id = generateId();
      const timestamp = new Date().toISOString();
      db.run(
        `INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [id, 'transfer', fromAccountId, amount, toAccountId, newFromBalance, timestamp]
      );

      await publishTransaction({
        id, type: 'transfer',
        accountId: fromAccountId,
        toAccountId, amount,
        newBalance: newFromBalance,
        timestamp
      });

      callback(null, { newBalance: newFromBalance, transactionId: id });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, message: err.message });
    }
  },
};

//  Démarrer le serveur gRPC 
const server = new grpc.Server();
server.addService(transactionProto.TransactionService.service, service);
server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) { console.error(err); return; }
  console.log(`MS2 Transactions démarré sur le port ${port}`);
});