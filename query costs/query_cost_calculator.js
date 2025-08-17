const { MongoClient } = require('mongodb');
const EventEmitter = require('events');

// Query Cost Configuration
const QUERY_CONFIG = {
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  costPerGBHour: 0.05, // USD per GB-hour
  memoryPerQueryMB: 512 // Default memory allocation
};

// Query Events Emitter
class QueryEvents extends EventEmitter {}
const queryEvents = new QueryEvents();

// QueryCostCalculator Class
class QueryCostCalculator {
  constructor() {
    this.client = new MongoClient(QUERY_CONFIG.mongoUri);
    this.db = null;
    this.connected = false;
  }

  async initialize() {
    await this.client.connect();
    this.db = this.client.db();
    this.connected = true;
    await this.db.collection('query_costs').createIndex({ org_id: 1, _time: -1 });
    queryEvents.emit('calculator:initialized');
  }

  // Calculate GB-hours for a query (duration in ms * memory in GB)
  async calculateQueryCost(orgID, queryText, durationMs, memoryMB = QUERY_CONFIG.memoryPerQueryMB) {
    if (!this.connected) await this.initialize();
    const memoryGB = memoryMB / 1024;
    const hours = durationMs / (1000 * 60 * 60);
    const gbHours = memoryGB * hours;
    const costUSD = gbHours * QUERY_CONFIG.costPerGBHour;

    await this.db.collection('query_costs').insertOne({
      org_id: orgID,
      query_text: queryText,
      duration_ms: durationMs,
      memory_mb: memoryMB,
      gb_hours: gbHours,
      cost_usd: costUSD,
      _time: new Date()
    });

    queryEvents.emit('cost:calculated', { orgID, gbHours, costUSD });
    return { gbHours, costUSD };
  }

  // Retrieve total GB-hours for an organization
  async getTotalGBHours(orgID, startDate, endDate) {
    if (!this.connected) await this.initialize();
    const result = await this.db.collection('query_costs').aggregate([
      { $match: { org_id: orgID, _time: { $gte: startDate, $lte: endDate } } },
      { $group: { _id: null, totalGBHours: { $sum: '$gb_hours' } } }
    ]).toArray();
    return result.length ? result[0].totalGBHours : 0;
  }

  async shutdown() {
    await this.client.close();
    this.connected = false;
    queryEvents.emit('calculator:shutdown');
  }
}

module.exports = QueryCostCalculator;

// Example usage
if (require.main === module) {
  (async () => {
    const calculator = new QueryCostCalculator();
    queryEvents.on('cost:calculated', data => console.log('Cost Calculated:', data));
    try {
      await calculator.initialize();
      const cost = await calculator.calculateQueryCost('org123', 'SELECT * FROM trades', 2000);
      console.log('Query Cost:', cost);
      const total = await calculator.getTotalGBHours('org123', new Date('2025-08-01'), new Date());
      console.log('Total GB-Hours:', total);
    } catch (err) {
      console.error('Error:', err);
    } finally {
      await calculator.shutdown();
    }
  })();
}
