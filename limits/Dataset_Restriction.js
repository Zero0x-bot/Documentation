const { MongoClient } = require('mongodb');
const { Validator } = require('jsonschema');
const EventEmitter = require('events');

// Dataset Restriction Configuration
const RESTRICTION_CONFIG = {
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  maxDatasets: 50, // Per org
  maxFieldsPerDataset: 200,
  systemWideMaxIngestionPerDayGB: 1000,
  requiredFields: ['_time', 'level'] // Timestamp and log level
};

// Restriction Events Emitter
class RestrictionEvents extends EventEmitter {}
const restrictionEvents = new RestrictionEvents();

// DatasetRestrictions Class
class DatasetRestrictions {
  constructor() {
    this.client = new MongoClient(RESTRICTION_CONFIG.mongoUri);
    this.db = null;
    this.connected = false;
    this.schemaValidator = new Validator();
  }

  // Initialize connection and schema
  async initialize() {
    await this.client.connect();
    this.db = this.client.db();
    this.connected = true;

    this.schemaValidator.addSchema({
      type: 'object',
      required: RESTRICTION_CONFIG.requiredFields,
      properties: {
        _time: { type: 'string', format: 'date-time' },
        level: { enum: ['info', 'warn', 'error', 'debug'] }
      }
    }, '/DatasetSchema');

    restrictionEvents.emit('restrictions:initialized');
  }

  // Enforce dataset creation restrictions
  async createDataset(orgID, datasetName, schema) {
    if (!this.connected) await this.initialize();

    const coll = this.db.collection('datasets');
    const count = await coll.countDocuments({ org_id: orgID });
    if (count >= RESTRICTION_CONFIG.maxDatasets) {
      restrictionEvents.emit('dataset_limit:exceeded', { orgID });
      throw new Error(`Max datasets limit reached for org ${orgID}`);
    }

    const fields = Object.keys(schema);
    if (fields.length > RESTRICTION_CONFIG.maxFieldsPerDataset) {
      restrictionEvents.emit('fields_limit:exceeded', { datasetName, fields: fields.length });
      throw new Error(`Max fields per dataset exceeded for ${datasetName}`);
    }

    await coll.insertOne({ org_id: orgID, name: datasetName, schema, _sysTime: new Date() });
    restrictionEvents.emit('dataset:created', { orgID, datasetName });

    return { status: 'created' };
  }

  // Validate system-wide limits and required fields on ingestion
  async validateIngestion(orgID, datasetName, events) {
    if (!this.connected) await this.initialize();

    const coll = this.db.collection('ingestion_logs');
    const today = new Date().setHours(0, 0, 0, 0);
    const dailyDoc = await coll.findOne({ org_id: orgID, date: today });

    const sizeGB = JSON.stringify(events).length / (1024 * 1024 * 1024); // Approximate size
    const newDailyGB = (dailyDoc ? dailyDoc.daily_gb : 0) + sizeGB;
    if (newDailyGB > RESTRICTION_CONFIG.systemWideMaxIngestionPerDayGB) {
      restrictionEvents.emit('system_ingestion_limit:exceeded', { orgID, newDailyGB });
      throw new Error('System-wide daily ingestion limit exceeded');
    }

    for (const event of events) {
      const validation = this.schemaValidator.validate(event, '/DatasetSchema');
      if (!validation.valid) {
        restrictionEvents.emit('required_fields:missing', { errors: validation.errors });
        throw new Error(`Required fields missing: ${validation.errors.map(e => e.message).join(', ')}`);
      }
    }

    await coll.updateOne(
      { org_id: orgID, date: today },
      { $inc: { daily_gb: sizeGB } },
      { upsert: true }
    );

    restrictionEvents.emit('ingestion:validated', { orgID, datasetName, events: events.length });
    return { validated: true };
  }

  // Shutdown
  async shutdown() {
    await this.client.close();
    this.connected = false;
    restrictionEvents.emit('restrictions:shutdown');
  }
}

module.exports = DatasetRestrictions;

// Example usage
if (require.main === module) {
  (async () => {
    const restrictions = new DatasetRestrictions();
    restrictionEvents.on('dataset_limit:exceeded', data => console.error('Dataset Limit:', data));

    try {
      await restrictions.initialize();
      await restrictions.createDataset('org123', 'new_dataset', { _time: 'date', level: 'string', extra: 'number' });
      await restrictions.validateIngestion('org123', 'new_dataset', [
        { _time: new Date().toISOString(), level: 'info', extra: 42 }
      ]);
    } catch (err) {
      console.error('Restrictions Error:', err);
    } finally {
      await restrictions.shutdown();
    }
  })();
}
