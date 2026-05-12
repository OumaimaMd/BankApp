const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const { createRxDatabase } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');

// ── Charger le proto ──────────────────────────────
const packageDef = protoLoader.loadSync(
  path.join(__dirname, '../proto/history.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const historyProto = grpc.loadPackageDefinition(packageDef).history;

// ── Schéma RxDB ───────────────────────────────────
const historySchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id:         { type: 'string', maxLength: 100 },
    accountId:  { type: 'string', maxLength: 100 },
    type:       { type: 'string', maxLength: 50  },
    amount:     { type: 'number' },
    newBalance: { type: 'number' },
    timestamp:  { type: 'string', maxLength: 100 },
  },
  required: ['id', 'accountId', 'type', 'amount', 'timestamp'],
};

// ── Initialiser RxDB ──────────────────────────────
let historyCollection;

async function initDB() {
  const db = await createRxDatabase({
    name: 'historydb',
    storage: getRxStorageMemory(),
  });
  await db.addCollections({ history: { schema: historySchema } });
  historyCollection = db.history;
  console.log('✅ RxDB connecté');
}

// ── Kafka Consumer ────────────────────────────────
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'history-group' });

async function startKafkaConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'transaction.done', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      console.log('📥 Kafka reçu:', data);

      await historyCollection.insert({
        id:         data.id,
        accountId:  data.accountId,
        type:       data.type,
        amount:     data.amount,
        newBalance: data.newBalance || 0,
        timestamp:  data.timestamp,
      });

      console.log('💾 Historique sauvegardé:', data.id);
    },
  });

  console.log('✅ Kafka consumer connecté — écoute transaction.done');
}

//  Implémentation gRPC 
const service = {

  GetHistory: async (call, callback) => {
    const { accountId } = call.request;
    try {
      const docs = await historyCollection.find({
        selector: { accountId }
      }).exec();
      const entries = docs.map(d => d.toJSON());
      callback(null, { entries });
    } catch (err) {
      callback(err);
    }
  },

};

// ── Démarrer tout ─────────────────────────────────
async function main() {
  await initDB();
  await startKafkaConsumer();

  const server = new grpc.Server();
  server.addService(historyProto.HistoryService.service, service);
  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`📜 MS3 Historique démarré sur le port ${port}`);
  });
}

main().catch(console.error);