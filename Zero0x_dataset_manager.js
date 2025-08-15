const { MongoClient, ObjectId } = require('mongodb');
const EventEmitter = require('events');
const { Validator } = require('jsonschema');
const crypto = require('crypto');
const { promisify } = require('util');
const fs = require('fs/promises');
const path = require('path');
const zlib = require('zlib');
const gunzip = promisify(zlib.gunzip);
const gzip = promisify(zlib.gzip);

// Zero0x Configuration Constants
const ZERO0X_CONFIG = {
  mongoUri: process.env.ZERO0X_MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  datasetNameRegex: /^[a-zA-Z0-9-]{1,128}$/,
  retentionDefaultDays: 30,
  maxFieldsPerDataset: 500,
  importFormats: ['ndjson', 'json-array', 'csv'],
  vacuumCooldownMs: 24 * 60 * 60 * 1000, // 1 day
  specialFields: {
    time: '_time',
    sysTime: '_sysTime'
  }
};

// Advanced Schema Validator for Zero0x Datasets
const datasetSchemaValidator = new Validator();
datasetSchemaValidator.addSchema({
  type: 'object',
  properties: {
    name: { type: 'string', pattern: ZERO0X_CONFIG.datasetNameRegex.source },
    retentionDays: { type: 'number', minimum: 1 },
    fields: { type: 'array', items: { type: 'string' } }
  },
  required: ['name']
}, '/DatasetSchema');

// Zero0x Dataset Event Emitter
class Zero0xDatasetEvents extends EventEmitter {}
const datasetEvents = new Zero0xDatasetEvents();

// Zero0xDatasetManager Class - Core engine for dataset operations
class Zero0xDatasetManager {
  constructor() {
    this.client = new MongoClient(ZERO0X_CONFIG.mongoUri, { useUnifiedTopology: true });
    this.db = null;
    this.connected = false;
    this.vacuumTimestamps = new Map(); // Track last vacuum time per dataset
  }

  // Asynchronous connection initialization with retry logic
  async initializeConnection(retries = 3, delayMs = 1000) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await this.client.connect();
        this.db = this.client.db();
        this.connected = true;
        datasetEvents.emit('connection:established');
        return;
      } catch (err) {
        if (attempt === retries) throw new Error(`Zero0x connection failed after ${retries} attempts: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
      }
    }
  }

  // Create a new dataset with schema enforcement and indexing
  async createDataset(datasetConfig) {
    if (!this.connected) await this.initializeConnection();
    const validation = datasetSchemaValidator.validate(datasetConfig, '/DatasetSchema');
    if (!validation.valid) throw new Error(`Invalid dataset config: ${validation.errors.map(e => e.message).join(', ')}`);

    const { name, retentionDays = ZERO0X_CONFIG.retentionDefaultDays } = datasetConfig;
    if (await this.db.listCollections({ name }).hasNext()) throw new Error(`Dataset ${name} already exists`);

    const collection = await this.db.createCollection(name, {
      validator: { $jsonSchema: { bsonType: 'object', required: [ZERO0X_CONFIG.specialFields.time] } },
      validationLevel: 'strict'
    });

    // Create TTL index for retention
    await collection.createIndex({ [ZERO0X_CONFIG.specialFields.time]: 1 }, {
      expireAfterSeconds: retentionDays * 86400,
      partialFilterExpression: { [ZERO0X_CONFIG.specialFields.time]: { $exists: true } }
    });

    // Auto-add special fields
    datasetEvents.emit('dataset:created', { name, retentionDays });
    return { name, status: 'created', retentionDays };
  }

  // Import data with format detection, compression handling, and batch insertion
  async importData(datasetName, filePath, options = {}) {
    if (!this.connected) await this.initializeConnection();
    if (!ZERO0X_CONFIG.datasetNameRegex.test(datasetName)) throw new Error(`Invalid dataset name: ${datasetName}`);

    const collection = this.db.collection(datasetName);
    const stats = await fs.stat(filePath);
    if (stats.size === 0) throw new Error('Empty import file');

    let buffer = await fs.readFile(filePath);
    if (path.extname(filePath) === '.gz') buffer = await gunzip(buffer);

    const format = this.detectImportFormat(buffer.toString('utf8').slice(0, 1024));
    if (!ZERO0X_CONFIG.importFormats.includes(format)) throw new Error(`Unsupported format: ${format}`);

    const events = await this.parseImportData(buffer.toString('utf8'), format, options.timestampField);
    const enrichedEvents = events.map(event => ({
      ...event,
      [ZERO0X_CONFIG.specialFields.time]: event[ZERO0X_CONFIG.specialFields.time] || new Date(),
      [ZERO0X_CONFIG.specialFields.sysTime]: new Date()
    }));

    // Batch insert with ordered false for performance
    const result = await collection.insertMany(enrichedEvents, { ordered: false });
    datasetEvents.emit('data:imported', { datasetName, count: result.insertedCount });

    return { insertedCount: result.insertedCount, datasetName };
  }

  // Detect import format using heuristic analysis
  detectImportFormat(sample) {
    if (sample.startsWith('[')) return 'json-array';
    if (sample.includes('\n') && !sample.includes(',')) return 'ndjson';
    return 'csv'; // Assume CSV otherwise
  }

  // Parse data based on format with error resilience
  async parseImportData(dataStr, format, customTimestampField) {
    try {
      switch (format) {
        case 'json-array':
          return JSON.parse(dataStr).map(event => {
            if (customTimestampField && event[customTimestampField]) {
              event[ZERO0X_CONFIG.specialFields.time] = new Date(event[customTimestampField]);
            }
            return event;
          });
        case 'ndjson':
          return dataStr.trim().split('\n').map(line => JSON.parse(line));
        case 'csv':
          const lines = dataStr.split('\n');
          const headers = lines[0].split(',');
          return lines.slice(1).map(line => {
            const values = line.split(',');
            return headers.reduce((obj, header, idx) => {
              obj[header.trim()] = values[idx]?.trim();
              return obj;
            }, {});
          });
        default:
          throw new Error('Unknown format');
      }
    } catch (err) {
      throw new Error(`Parsing failed: ${err.message}`);
    }
  }

  // Trim dataset: Delete events before a specified date using bulk operations
  async trimDataset(datasetName, beforeDate) {
    if (!this.connected) await this.initializeConnection();
    const collection = this.db.collection(datasetName);

    const deleteResult = await collection.deleteMany({
      [ZERO0X_CONFIG.specialFields.time]: { $lt: beforeDate }
    });

    datasetEvents.emit('dataset:trimmed', { datasetName, deletedCount: deleteResult.deletedCount });
    return { deletedCount: deleteResult.deletedCount };
  }

  // Vacuum fields: Rebuild schema by aggregating current fields within retention
  async vacuumFields(datasetName) {
    if (!this.connected) await this.initializeConnection();
    const lastVacuum = this.vacuumTimestamps.get(datasetName) || 0;
    if (Date.now() - lastVacuum < ZERO0X_CONFIG.vacuumCooldownMs) {
      throw new Error(`Vacuum cooldown active for ${datasetName}`);
    }

    const collection = this.db.collection(datasetName);
    const retentionDate = new Date(Date.now() - ZERO0X_CONFIG.retentionDefaultDays * 86400 * 1000);

    // Advanced aggregation to extract unique fields
    const pipeline = [
      { $match: { [ZERO0X_CONFIG.specialFields.time]: { $gte: retentionDate } } },
      { $project: { fields: { $objectToArray: "$$ROOT" } } },
      { $unwind: "$fields" },
      { $group: { _id: "$fields.k", count: { $sum: 1 } } },
      { $match: { _id: { $nin: [ZERO0X_CONFIG.specialFields.time, ZERO0X_CONFIG.specialFields.sysTime] } } }
    ];

    const currentFields = await collection.aggregate(pipeline).toArray();
    if (currentFields.length > ZERO0X_CONFIG.maxFieldsPerDataset) {
      throw new Error(`Field count exceeds limit after vacuum: ${currentFields.length}`);
    }

    // Update validator schema dynamically
    await this.db.command({
      collMod: datasetName,
      validator: {
        $jsonSchema: {
          bsonType: 'object',
          required: [ZERO0X_CONFIG.specialFields.time],
          properties: currentFields.reduce((props, field) => {
            props[field._id] = { bsonType: ['string', 'number', 'bool', 'date', 'object', 'array'] };
            return props;
          }, {})
        }
      }
    });

    this.vacuumTimestamps.set(datasetName, Date.now());
    datasetEvents.emit('fields:vacuumed', { datasetName, fieldCount: currentFields.length });
    return { fieldCount: currentFields.length };
  }

  // Share dataset: Generate cryptographically secure sharing token
  async shareDataset(datasetName, targetOrgId) {
    if (!this.connected) await this.initializeConnection();
    const token = crypto.randomBytes(32).toString('hex');
    const shareDoc = {
      datasetName,
      targetOrgId: new ObjectId(targetOrgId),
      token,
      createdAt: new Date()
    };

    await this.db.collection('zero0x_shares').insertOne(shareDoc);
    datasetEvents.emit('dataset:shared', { datasetName, token });
    return { sharingUrl: `https://zero0x.app/share/dataset/${token}` };
  }

  // Revoke share: Remove access with cascading effects
  async revokeShare(datasetName, targetOrgId) {
    if (!this.connected) await this.initializeConnection();
    const result = await this.db.collection('zero0x_shares').deleteMany({
      datasetName,
      targetOrgId: new ObjectId(targetOrgId)
    });

    datasetEvents.emit('share:revoked', { datasetName, deletedCount: result.deletedCount });
    return { revoked: result.deletedCount > 0 };
  }

  // Update retention period with data pruning
  async updateRetention(datasetName, newDays) {
    if (!this.connected) await this.initializeConnection();
    if (newDays <= 0) throw new Error('Retention must be positive');

    const collection = this.db.collection(datasetName);
    await collection.dropIndex(`${ZERO0X_CONFIG.specialFields.time}_1`); // Drop old TTL
    await collection.createIndex({ [ZERO0X_CONFIG.specialFields.time]: 1 }, {
      expireAfterSeconds: newDays * 86400
    });

    // Prune immediately if shortened
    const pruneDate = new Date(Date.now() - newDays * 86400 * 1000);
    await this.trimDataset(datasetName, pruneDate);

    datasetEvents.emit('retention:updated', { datasetName, newDays });
    return { newRetentionDays: newDays };
  }

  // Delete dataset with confirmation and cleanup
  async deleteDataset(datasetName, confirmationName) {
    if (!this.connected) await this.initializeConnection();
    if (datasetName !== confirmationName) throw new Error('Confirmation mismatch');

    await this.db.collection(datasetName).drop();
    await this.db.collection('zero0x_shares').deleteMany({ datasetName });
    this.vacuumTimestamps.delete(datasetName);

    datasetEvents.emit('dataset:deleted', { datasetName });
    return { status: 'deleted' };
  }

  // Graceful shutdown
  async shutdown() {
    if (this.connected) {
      await this.client.close();
      this.connected = false;
      datasetEvents.emit('connection:closed');
    }
  }
}

// Export manager and events for modular use
module.exports = { Zero0xDatasetManager, datasetEvents };

// Example advanced usage with event listening
if (require.main === module) {
  (async () => {
    const manager = new Zero0xDatasetManager();
    datasetEvents.on('dataset:created', data => console.log('Created:', data));
    try {
      await manager.initializeConnection();
      const dataset = await manager.createDataset({ name: 'trade_logs', retentionDays: 90 });
      console.log('Dataset created:', dataset);
      // Simulate import
      // await manager.importData('trade_logs', './sample.ndjson.gz');
      await manager.vacuumFields('trade_logs');
      await manager.shutdown();
    } catch (err) {
      console.error('Zero0x Error:', err);
      await manager.shutdown();
    }
  })();
}
