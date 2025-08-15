const { MongoClient, ObjectId } = require('mongodb');
const crypto = require('crypto');
const { Validator } = require('jsonschema');
const EventEmitter = require('events');
const { RateLimiter } = require('limiter');
const winston = require('winston');

// Zero0x Configuration Constants
const ZERO0X_CONFIG = {
  mongoUri: process.env.ZERO0X_MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  datasetNameRegex: /^[a-zA-Z0-9-]{1,128}$/,
  maxFieldsPerDataset: 500,
  retentionDefaultDays: 30,
  vacuumCooldownMs: 24 * 60 * 60 * 1000, // 1 day
  specialFields: {
    time: '_time',
    sysTime: '_sysTime'
  },
  roles: {
    admin: 'admin',
    trader: 'trader',
    analyst: 'analyst'
  },
  environments: ['production', 'staging', 'development'],
  signalTypes: ['trades', 'logs', 'errors', 'analytics'],
  services: ['trading', 'wallet', 'market_data', 'compliance']
};

// Schema Validator for Dataset Policies
const policySchemaValidator = new Validator();
policySchemaValidator.addSchema({
  type: 'object',
  properties: {
    datasetName: { type: 'string', pattern: ZERO0X_CONFIG.datasetNameRegex.source },
    environment: { type: 'string', enum: ZERO0X_CONFIG.environments },
    signalType: { type: 'string', enum: ZERO0X_CONFIG.signalTypes },
    service: { type: 'string', enum: ZERO0X_CONFIG.services },
    retentionDays: { type: 'number', minimum: 1 },
    allowedRoles: { type: 'array', items: { type: 'string', enum: Object.values(ZERO0X_CONFIG.roles) } }
  },
  required: ['datasetName', 'environment', 'signalType', 'service']
}, '/PolicySchema');

// Zero0x Policy Event Emitter
class Zero0xPolicyEvents extends EventEmitter {}
const policyEvents = new Zero0xPolicyEvents();

// Configure Winston Logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ssZ' }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/zero0x_policy_audit.jsonl', level: 'info' }),
    new winston.transports.File({ filename: 'logs/zero0x_policy_errors.jsonl', level: 'error' })
  ]
});

// Zero0xDatasetPolicyEnforcer Class
class Zero0xDatasetPolicyEnforcer {
  constructor() {
    this.mongoClient = new MongoClient(ZERO0X_CONFIG.mongoUri, { useUnifiedTopology: true });
    this.db = null;
    this.connected = false;
    this.rateLimiter = new RateLimiter({ tokensPerInterval: 10, interval: 'second' });
    this.vacuumTimestamps = new Map();
  }

  // Initialize MongoDB connection with retry logic
  async initialize() {
    try {
      await this.mongoClient.connect();
      this.db = this.mongoClient.db();
      this.connected = true;
      await this.ensurePolicyCollection();
      logger.info({
        message: 'Dataset policy enforcer initialized',
        [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
        dataset: 'policy_logs'
      });
      policyEvents.emit('policy:initialized');
    } catch (error) {
      logger.error({
        message: 'Initialization failed',
        error: error.message,
        [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
        dataset: ZERO0X_CONFIG.datasetNames.errors
      });
      throw error;
    }
  }

  // Ensure policy collection exists
  async ensurePolicyCollection() {
    const collections = await this.db.listCollections({ name: 'zero0x_policies' }).toArray();
    if (collections.length === 0) {
      await this.db.createCollection('zero0x_policies', {
        validator: { $jsonSchema: { bsonType: 'object', required: ['datasetName', 'environment', 'signalType', 'service'] } }
      });
    }
  }

  // Create dataset with policy enforcement
  async createDataset(policyConfig, userId) {
    if (!this.connected) await this.initialize();
    await this.rateLimiter.removeTokens(1);

    const validation = policySchemaValidator.validate(policyConfig, '/PolicySchema');
    if (!validation.valid) throw new Error(`Invalid policy config: ${validation.errors.map(e => e.message).join(', ')}`);

    const { datasetName, environment, signalType, service, retentionDays = ZERO0X_CONFIG.retentionDefaultDays, allowedRoles = [ZERO0X_CONFIG.roles.admin] } = policyConfig;

    // Prevent kitchen sink datasets
    if (!environment || !signalType || !service) {
      throw new Error('Dataset must specify environment, signalType, and service to avoid kitchen sink');
    }

    // Check for existing dataset
    if (await this.db.listCollections({ name: datasetName }).hasNext()) {
      throw new Error(`Dataset ${datasetName} already exists`);
    }

    // Create dataset with TTL index
    const collection = await this.db.createCollection(datasetName, {
      validator: {
        $jsonSchema: {
          bsonType: 'object',
          required: [ZERO0X_CONFIG.specialFields.time],
          properties: { [ZERO0X_CONFIG.specialFields.time]: { bsonType: 'date' } }
        }
      }
    });

    await collection.createIndex({ [ZERO0X_CONFIG.specialFields.time]: 1 }, {
      expireAfterSeconds: retentionDays * 86400
    });

    // Store policy
    await this.db.collection('zero0x_policies').insertOne({
      datasetName,
      environment,
      signalType,
      service,
      allowedRoles,
      createdBy: new ObjectId(userId),
      [ZERO0X_CONFIG.specialFields.time]: new Date(),
      [ZERO0X_CONFIG.specialFields.sysTime]: new Date()
    });

    logger.info({
      message: `Dataset ${datasetName} created with policy`,
      policy: { environment, signalType, service },
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('dataset:created', { datasetName, environment, signalType, service });

    return { datasetName, status: 'created', retentionDays };
  }

  // Import data with policy validation
  async importData(datasetName, data, userId, format = 'ndjson') {
    if (!this.connected) await this.initialize();
    await this.rateLimiter.removeTokens(1);

    const policy = await this.db.collection('zero0x_policies').findOne({ datasetName });
    if (!policy) throw new Error(`No policy found for dataset ${datasetName}`);
    if (!await this.hasAccess(userId, datasetName, 'write')) throw new Error('User lacks write access');

    const events = this.parseData(data, format);
    const enrichedEvents = events.map(event => ({
      ...event,
      [ZERO0X_CONFIG.specialFields.time]: event[ZERO0X_CONFIG.specialFields.time] || new Date(),
      [ZERO0X_CONFIG.specialFields.sysTime]: new Date(),
      environment: policy.environment,
      signalType: policy.signalType,
      service: policy.service
    }));

    const collection = this.db.collection(datasetName);
    const result = await collection.insertMany(enrichedEvents, { ordered: false });

    logger.info({
      message: `Imported ${result.insertedCount} events to ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('data:imported', { datasetName, count: result.insertedCount });

    return { insertedCount: result.insertedCount };
  }

  // Parse data based on format
  parseData(data, format) {
    try {
      switch (format) {
        case 'ndjson':
          return data.trim().split('\n').map(line => JSON.parse(line));
        case 'json-array':
          return JSON.parse(data);
        case 'csv':
          const lines = data.split('\n');
          const headers = lines[0].split(',');
          return lines.slice(1).map(line => {
            const values = line.split(',');
            return headers.reduce((obj, header, idx) => {
              obj[header.trim()] = values[idx]?.trim();
              return obj;
            }, {});
          });
        default:
          throw new Error(`Unsupported format: ${format}`);
      }
    } catch (err) {
      throw new Error(`Data parsing failed: ${err.message}`);
    }
  }

  // Trim dataset with policy check
  async trimDataset(datasetName, beforeDate, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const collection = this.db.collection(datasetName);
    const result = await collection.deleteMany({
      [ZERO0X_CONFIG.specialFields.time]: { $lt: beforeDate }
    });

    logger.info({
      message: `Trimmed ${result.deletedCount} events from ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('dataset:trimmed', { datasetName, deletedCount: result.deletedCount });

    return { deletedCount: result.deletedCount };
  }

  // Vacuum fields with policy enforcement
  async vacuumFields(datasetName, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const lastVacuum = this.vacuumTimestamps.get(datasetName) || 0;
    if (Date.now() - lastVacuum < ZERO0X_CONFIG.vacuumCooldownMs) {
      throw new Error(`Vacuum cooldown active for ${datasetName}`);
    }

    const collection = this.db.collection(datasetName);
    const retentionDate = new Date(Date.now() - ZERO0X_CONFIG.retentionDefaultDays * 86400 * 1000);

    const pipeline = [
      { $match: { [ZERO0X_CONFIG.specialFields.time]: { $gte: retentionDate } } },
      { $project: { fields: { $objectToArray: "$$ROOT" } } },
      { $unwind: "$fields" },
      { $group: { _id: "$fields.k", count: { $sum: 1 } } },
      { $match: { _id: { $nin: [ZERO0X_CONFIG.specialFields.time, ZERO0X_CONFIG.specialFields.sysTime] } } }
    ];

    const currentFields = await collection.aggregate(pipeline).toArray();
    if (currentFields.length > ZERO0X_CONFIG.maxFieldsPerDataset) {
      throw new Error(`Field count exceeds limit: ${currentFields.length}`);
    }

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
    logger.info({
      message: `Vacuumed ${currentFields.length} fields for ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('fields:vacuumed', { datasetName, fieldCount: currentFields.length });

    return { fieldCount: currentFields.length };
  }

  // Share dataset with cryptographic token
  async shareDataset(datasetName, targetOrgId, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const token = crypto.randomBytes(32).toString('hex');
    await this.db.collection('zero0x_shares').insertOne({
      datasetName,
      targetOrgId: new ObjectId(targetOrgId),
      token,
      sharedBy: new ObjectId(userId),
      [ZERO0X_CONFIG.specialFields.time]: new Date()
    });

    logger.info({
      message: `Shared dataset ${datasetName} with org ${targetOrgId}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('dataset:shared', { datasetName, token });

    return { sharingUrl: `https://zero0x.app/share/dataset/${token}` };
  }

  // Delete sharing link
  async deleteSharingLink(datasetName, token, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const result = await this.db.collection('zero0x_shares').deleteOne({ datasetName, token });

    logger.info({
      message: `Deleted sharing link for ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('share:deleted', { datasetName, deleted: result.deletedCount > 0 });

    return { deleted: result.deletedCount > 0 };
  }

  // Remove access to shared dataset
  async removeSharedAccess(datasetName, targetOrgId, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const result = await this.db.collection('zero0x_shares').deleteMany({
      datasetName,
      targetOrgId: new ObjectId(targetOrgId)
    });

    logger.info({
      message: `Removed access to ${datasetName} for org ${targetOrgId}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('access:removed', { datasetName, deletedCount: result.deletedCount });

    return { removed: result.deletedCount > 0 };
  }

  // Remove shared dataset from receiving organization
  async removeSharedDataset(datasetName, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');

    const result = await this.db.collection('zero0x_shares').deleteMany({ datasetName });

    logger.info({
      message: `Removed shared dataset ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('dataset:shared_removed', { datasetName, deletedCount: result.deletedCount });

    return { removed: result.deletedCount > 0 };
  }

  // Specify data retention period
  async specifyRetentionPeriod(datasetName, newDays, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');
    if (newDays <= 0) throw new Error('Retention period must be positive');

    const collection = this.db.collection(datasetName);
    await collection.dropIndex(`${ZERO0X_CONFIG.specialFields.time}_1`);
    await collection.createIndex({ [ZERO0X_CONFIG.specialFields.time]: 1 }, {
      expireAfterSeconds: newDays * 86400
    });

    const pruneDate = new Date(Date.now() - newDays * 86400 * 1000);
    await this.trimDataset(datasetName, pruneDate, userId);

    await this.db.collection('zero0x_policies').updateOne(
      { datasetName },
      { $set: { retentionDays: newDays, [ZERO0X_CONFIG.specialFields.sysTime]: new Date() } }
    );

    logger.info({
      message: `Updated retention for ${datasetName} to ${newDays} days`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('retention:updated', { datasetName, newDays });

    return { newRetentionDays: newDays };
  }

  // Delete dataset with confirmation
  async deleteDataset(datasetName, confirmationName, userId) {
    if (!this.connected) await this.initialize();
    if (!await this.hasAccess(userId, datasetName, 'admin')) throw new Error('User lacks admin access');
    if (datasetName !== confirmationName) throw new Error('Confirmation mismatch');

    await this.db.collection(datasetName).drop();
    await this.db.collection('zero0x_policies').deleteOne({ datasetName });
    await this.db.collection('zero0x_shares').deleteMany({ datasetName });
    this.vacuumTimestamps.delete(datasetName);

    logger.info({
      message: `Deleted dataset ${datasetName}`,
      [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
      dataset: 'policy_logs'
    });
    policyEvents.emit('dataset:deleted', { datasetName });

    return { status: 'deleted' };
  }

  // Check user access based on role
  async hasAccess(userId, datasetName, requiredPermission) {
    const user = await this.db.collection('zero0x_users').findOne({ _id: new ObjectId(userId) });
    if (!user) return false;

    const policy = await this.db.collection('zero0x_policies').findOne({ datasetName });
    if (!policy) return false;

    const userRoles = user.roles || [];
    const hasRole = policy.allowedRoles.some(role => userRoles.includes(role));
    if (requiredPermission === 'admin' && !userRoles.includes(ZERO0X_CONFIG.roles.admin)) return false;
    if (requiredPermission === 'write' && !hasRole) return false;

    return true;
  }

  // Shutdown gracefully
  async shutdown() {
    if (this.connected) {
      await this.mongoClient.close();
      this.connected = false;
      logger.info({
        message: 'Dataset policy enforcer shutdown',
        [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
        dataset: 'policy_logs'
      });
      policyEvents.emit('policy:shutdown');
    }
  }
}

// Export enforcer and events
module.exports = { Zero0xDatasetPolicyEnforcer, policyEvents };

// Example usage with event listeners
if (require.main === module) {
  (async () => {
    const enforcer = new Zero0xDatasetPolicyEnforcer();
    policyEvents.on('dataset:created', data => console.log('Dataset Created:', data));
    policyEvents.on('error', err => console.error('Error:', err));

    try {
      await enforcer.initialize();
      const policyConfig = {
        datasetName: 'solana_trades_prod',
        environment: 'production',
        signalType: 'trades',
        service: 'trading',
        retentionDays: 90,
        allowedRoles: [ZERO0X_CONFIG.roles.trader, ZERO0X_CONFIG.roles.admin]
      };
      await enforcer.createDataset(policyConfig, 'user123');
      await enforcer.vacuumFields('solana_trades_prod', 'user123');
      await enforcer.shareDataset('solana_trades_prod', 'org456', 'user123');
    } catch (err) {
      console.error('Policy Enforcer Error:', err);
    } finally {
      await enforcer.shutdown();
    }
  })();
}
