const { MongoClient } = require('mongodb');
const EventEmitter = require('events');
const { promisify } = require('util');
const setTimeoutPromise = promisify(setTimeout);

// Ingestion Limit Configuration
const INGESTION_CONFIG = {
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  maxEventsPerMinute: 10000,
  requiredTimestampFormat: 'ISO8601',
  requiredLogLevels: ['debug', 'info', 'warn', 'error'],
  validationRetryDelay: 500 // ms
};

// Ingestion Events Emitter
class IngestionEvents extends EventEmitter {}
const ingestionEvents = new IngestionEvents();

// IngestionLimitValidator Class
class IngestionLimitValidator {
  constructor() {
    this.client = new MongoClient(INGESTION_CONFIG.mongoUri);
    this.db = null;
    this.connected = false;
  }

  // Initialize connection
  async initialize() {
    await this.client.connect();
    this.db = this.client.db();
    this.connected = true;

    ingestionEvents.emit('validator:initialized');
  }

  // Validate ingested data limits and requirements
  async validateData(events) {
    if (!this.connected) await this.initialize();

    if (events.length > INGESTION_CONFIG.maxEventsPerMinute) {
      ingestionEvents.emit('ingestion_limit:exceeded', { count: events.length });
      throw new Error(`Ingestion limit exceeded: ${events.length} > ${INGESTION_CONFIG.maxEventsPerMinute}`);
    }

    for (const event of events) {
      // Validate timestamp
      if (!event._time || !this.isValidISODate(event._time)) {
        ingestionEvents.emit('timestamp:invalid', { event });
        throw new Error('Invalid or missing timestamp field');
      }

      // Validate log level
      if (!event.level || !INGESTION_CONFIG.requiredLogLevels.includes(event.level)) {
        ingestionEvents.emit('log_level:invalid', { event });
        throw new Error('Invalid or missing log level field');
      }
    }

    // Simulate async validation with retry
    try {
      await setTimeoutPromise(INGESTION_CONFIG.validationRetryDelay);
    } catch (err) {
      throw new Error('Validation timeout');
    }

    ingestionEvents.emit('data:validated', { count: events.length });
    return { valid: true };
  }

  // Check if string is valid ISO date
  isValidISODate(dateStr) {
    const date = new Date(dateStr);
    return date.toISOString() === dateStr;
  }

  // Log validated ingestion
  async logIngestion(events) {
    const coll = this.db.collection('ingestion_logs');
    await coll.insertMany(events.map(event => ({
      ...event,
      validated_at: new Date()
    })));

    ingestionEvents.emit('ingestion:logged', { count: events.length });
  }

  // Shutdown
  async shutdown() {
    await this.client.close();
    this.connected = false;
    ingestionEvents.emit('validator:shutdown');
  }
}

module.exports = IngestionLimitValidator;

// Example usage
if (require.main === module) {
  (async () => {
    const validator = new IngestionLimitValidator();
    ingestionEvents.on('ingestion_limit:exceeded', data => console.error('Limit Exceeded:', data));

    try {
      await validator.initialize();
      const events = [
        { _time: new Date().toISOString(), level: 'info', data: 'trade executed' }
      ];
      await validator.validateData(events);
      await validator.logIngestion(events);
    } catch (err) {
      console.error('Validator Error:', err);
    } finally {
      await validator.shutdown();
    }
  })();
}
