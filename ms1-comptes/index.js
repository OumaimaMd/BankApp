const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const PROTO_PATH = process.env.PROTO_PATH || path.join(__dirname, '../proto');


//Charger le proto
const packageDef = protoLoader.loadSync(
  path.join(__dirname, '../proto/account.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const accountProto = grpc.loadPackageDefinition(packageDef).account;

//Base de données SQLite3
const db = new sqlite3.Database('/app/data/accounts.db', (err) => {
  if (err) console.error(err);
  else console.log('✅ SQLite3 connecté');
});

db.run(`CREATE TABLE IF NOT EXISTS accounts (
  id       TEXT PRIMARY KEY,
  owner    TEXT NOT NULL,
  balance  REAL NOT NULL,
  currency TEXT NOT NULL
)`);

//Fonctions utilitaires
function generateId() {
  return 'acc-' + Date.now();
}

//Implémentation gRPC
const service = {

  CreateAccount: (call, callback) => {
    const { owner, balance, currency } = call.request;
    const id = generateId();
    db.run(
      `INSERT INTO accounts VALUES (?, ?, ?, ?)`,
      [id, owner, balance, currency],
      (err) => {
        if (err) return callback(err);
        callback(null, { account: { id, owner, balance, currency } });
      }
    );
  },

  GetAccount: (call, callback) => {
    const { id } = call.request;
    db.get(`SELECT * FROM accounts WHERE id = ?`, [id], (err, row) => {
      if (err) return callback(err);
      if (!row) return callback({ code: grpc.status.NOT_FOUND, message: 'Compte introuvable' });
      callback(null, { account: row });
    });
  },

  GetAllAccounts: (call, callback) => {
    db.all(`SELECT * FROM accounts`, [], (err, rows) => {
      if (err) return callback(err);
      callback(null, { accounts: rows });
    });
  },
  UpdateBalance: (call, callback) => {
  const { id, balance } = call.request;
  db.run(
    `UPDATE accounts SET balance = ? WHERE id = ?`,
    [balance, id],
    (err) => {
      if (err) return callback(err);
      db.get(`SELECT * FROM accounts WHERE id = ?`, [id], (err, row) => {
        if (err) return callback(err);
        callback(null, { account: row });
      });
    }
  );
},

};

//Démarrer le serveur gRPC
const server = new grpc.Server();
server.addService(accountProto.AccountService.service, service);
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) { console.error(err); return; }
  console.log(`🏦 MS1 Comptes démarré sur le port ${port}`);
});