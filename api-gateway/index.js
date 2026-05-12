const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@as-integrations/express4');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const fs = require('fs');

const app = express();

// ── Middlewares GLOBAUX en tout premier ───────────
app.use(cors());
app.use(express.json());

// ── Charger les protos ────────────────────────────
const accountProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '../proto/account.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true })
).account;

const transactionProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '../proto/transaction.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true })
).transaction;

const historyProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '../proto/history.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true })
).history;

// ── Clients gRPC ──────────────────────────────────
const accountClient = new accountProto.AccountService(
  'localhost:50051', grpc.credentials.createInsecure()
);
const transactionClient = new transactionProto.TransactionService(
  'localhost:50052', grpc.credentials.createInsecure()
);
const historyClient = new historyProto.HistoryService(
  'localhost:50053', grpc.credentials.createInsecure()
);

// ── Helper gRPC ───────────────────────────────────
function grpcCall(client, method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

// ── Route accueil ─────────────────────────────────
app.get('/', (req, res) => {
  res.json({
    message: '🏦 Bank App API Gateway',
    rest: {
      accounts: 'GET  /accounts',
      account:  'GET  /accounts/:id',
      create:   'POST /accounts',
      deposit:  'POST /transactions/deposit',
      withdraw: 'POST /transactions/withdraw',
      transfer: 'POST /transactions/transfer',
      history:  'GET  /history/:accountId',
    },
    graphql: 'POST /graphql'
  });
});

// ── Routes REST Comptes ───────────────────────────
app.get('/accounts', async (req, res) => {
  try {
    const result = await grpcCall(accountClient, 'GetAllAccounts', {});
    res.json(result.accounts);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/accounts/:id', async (req, res) => {
  try {
    const result = await grpcCall(accountClient, 'GetAccount', { id: req.params.id });
    res.json(result.account);
  } catch (err) { res.status(404).json({ error: 'Compte introuvable' }); }
});

app.post('/accounts', async (req, res) => {
  try {
    const result = await grpcCall(accountClient, 'CreateAccount', req.body);
    res.status(201).json(result.account);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Routes REST Transactions ──────────────────────
app.post('/transactions/deposit', async (req, res) => {
  try {
    const result = await grpcCall(transactionClient, 'Deposit', req.body);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/transactions/withdraw', async (req, res) => {
  try {
    const result = await grpcCall(transactionClient, 'Withdraw', req.body);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/transactions/transfer', async (req, res) => {
  try {
    const result = await grpcCall(transactionClient, 'Transfer', req.body);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Route REST Historique ─────────────────────────
app.get('/history/:accountId', async (req, res) => {
  try {
    const result = await grpcCall(historyClient, 'GetHistory', { accountId: req.params.accountId });
    res.json(result.entries);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── GraphQL avec Apollo ───────────────────────────
const typeDefs = fs.readFileSync(path.join(__dirname, 'schema.gql'), 'utf8');

const resolvers = {
  Query: {
    account: async (_, { id }) => {
      const res = await grpcCall(accountClient, 'GetAccount', { id });
      return res.account;
    },
    accounts: async () => {
      const res = await grpcCall(accountClient, 'GetAllAccounts', {});
      return res.accounts;
    },
    history: async (_, { accountId }) => {
      const res = await grpcCall(historyClient, 'GetHistory', { accountId });
      return res.entries;
    },
  },
  Mutation: {
    createAccount: async (_, args) => {
      const res = await grpcCall(accountClient, 'CreateAccount', args);
      return res.account;
    },
    deposit: async (_, args) => {
      return await grpcCall(transactionClient, 'Deposit', args);
    },
    withdraw: async (_, args) => {
      return await grpcCall(transactionClient, 'Withdraw', args);
    },
    transfer: async (_, args) => {
      return await grpcCall(transactionClient, 'Transfer', args);
    },
  },
};

// ── Démarrer Apollo + Express ─────────────────────
async function main() {
  const server = new ApolloServer({ typeDefs, resolvers });
  await server.start();

  app.use('/graphql', expressMiddleware(server));

  app.listen(3000, () => {
    console.log('🚀 API Gateway sur http://localhost:3000');
    console.log('📡 REST    → http://localhost:3000/accounts');
    console.log('🔷 GraphQL → http://localhost:3000/graphql');
  });
}

main().catch(console.error);