const { MongoClient, ObjectId } = require('mongodb');
const Redis = require('ioredis');
const WebSocket = require('ws');
const { Validator } = require('jsonschema');
const EventEmitter = require('events');
const { RateLimiter } = require('limiter');

// Zero0x Configuration Constants
const ZERO0X_CONFIG = {
  mongoUri: process.env.ZERO0X_MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  redisUri: process.env.ZERO0X_REDIS_URI || 'redis://localhost:6379',
  wsPort: 8080,
  datasetNames: {
    trades: 'trades_dataset',
    analytics: 'analytics_dataset',
    errors: 'errors_dataset'
  },
  cacheTTL: 300, // 5 minutes in seconds
  maxAggregationWindowDays: 30,
  specialFields: {
    time: '_time',
    sysTime: '_sysTime'
  }
};

// Schema Validator for Analytics Output
const analyticsSchemaValidator = new Validator();
analyticsSchemaValidator.addSchema({
  type: 'object',
  properties: {
    metric: { type: 'string', enum: ['volume', 'pnl', 'success_rate', 'arbitrage'] },
    value: { type: ['number', 'object'] },
    timestamp: { type: 'string', format: 'date-time' },
    dataset: { type: 'string' }
  },
  required: ['metric', 'value', 'timestamp']
}, '/AnalyticsSchema');

// Zero0x Analytics Event Emitter
class Zero0xAnalyticsEvents extends EventEmitter {}
const analyticsEvents = new Zero0xAnalyticsEvents();

// Zero0xTradeAnalyticsEngine Class
class Zero0xTradeAnalyticsEngine {
  constructor() {
    this.mongoClient = new MongoClient(ZERO0X_CONFIG.mongoUri, { useUnifiedTopology: true });
    this.redisClient = new Redis(ZERO0X_CONFIG.redisUri);
    this.wss = new WebSocket.Server({ port: ZERO0X_CONFIG.wsPort });
    this.db = null;
    this.connected = false;
    this.rateLimiter = new RateLimiter({ tokensPerInterval: 5, interval: 'second' });
  }

  // Initialize MongoDB and Redis connections
  async initialize() {
    try {
      await this.mongoClient.connect();
      this.db = this.mongoClient.db();
      await this.redisClient.ping();
      this.connected = true;

      // Setup WebSocket broadcast
      this.wss.on('connection', ws => {
        ws.on('message', async message => {
          const { metric } = JSON.parse(message);
          await this.broadcastMetric(metric, ws);
        });
      });

      analyticsEvents.emit('engine:initialized');
    } catch (error) {
      analyticsEvents.emit('error', {
        message: 'Initialization failed',
        error: error.message,
        [ZERO0X_CONFIG.specialFields.time]: new Date().toISOString(),
        dataset: ZERO0X_CONFIG.datasetNames.errors
      });
      throw error;
    }
  }

  // Compute trade volume by chain (Solana/Ethereum)
  async computeTradeVolume(chain, startDate, endDate) {
    if (!this.connected) await this.initialize();
    await this.rateLimiter.removeTokens(1);

    const pipeline = [
      {
        $match: {
          [ZERO0X_CONFIG.specialFields.time]: { $gte: startDate, $lte: endDate },
          chain: chain.toLowerCase()
        }
      },
      {
        $group: {
          _id: null,
          totalVolume: { $sum: '$amount' },
          tradeCount: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          metric: 'volume',
          value: { total: '$totalVolume', count: '$tradeCount' },
          timestamp: new Date().toISOString()
        }
      }
    ];

    const result = await this.db.collection(ZERO0X_CONFIG.datasetNames.trades)
      .aggregate(pipeline)
      .toArray();

    const metricData = result[0] || { metric: 'volume', value: { total: 0, count: 0 }, timestamp: new Date().toISOString() };
    await this.cacheMetric('volume', chain, metricData);
    await this.storeMetric(metricData);
    analyticsEvents.emit('metric:computed', metricData);

    return metricData;
  }

  // Calculate profit and loss (PnL) for a time window
  async calculatePnL(startDate, endDate) {
    if (!this.connected) await this.initialize();
    await this.rateLimiter.removeTokens(1);

    const pipeline = [
      {
        $match: {
          [ZERO0X_CONFIG.specialFields.time]: { $gte: startDate, $lte: endDate },
          type: 'arbitrage'
        }
      },
      {
        $group: {
          _id: null,
          totalProfit: { $sum: { $subtract: ['$sellPrice', '$buyPrice'] } },
          tradeCount: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          metric: 'pnl',
          value: {
            totalProfit: '$totalProfit',
            avgProfitPerTrade: { $divide: ['$totalProfit', { $max: ['$tradeCount', 1] }] }
          },
          timestamp: new Date().toISOString()
        }
      }
    ];

    const result = await this.db.collection(ZERO0X_CONFIG.datasetNames.trades)
      .aggregate(pipeline)
      .toArray();

    const metricData = result[0] || { metric: 'pnl', value: { totalProfit: 0, avgProfitPerTrade: 0 }, timestamp: new Date().toISOString() };
    await this.cacheMetric('pnl', 'all', metricData);
    await this.storeMetric(metricData);
    analyticsEvents.emit('metric:computed', metricData);

    return metricData;
  }

  // Compute trade success rate
  async computeSuccessRate(startDate, endDate) {
    if (!this.connected) await this.initialize();
    await this.rateLimiter.removeTokens(1);

    const pipeline = [
      {
        $match: {
          [ZERO0X_CONFIG.specialFields.time]: { $gte: startDate, $lte: endDate }
        }
      },
      {
        $group: {
          _id: null,
          successfulTrades: { $sum: { $cond: [{ $eq: ['$status', 'success'] }, 1, 0] } },
          totalTrades: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          metric: 'success_rate',
          value: {
            rate: { $divide: ['$successfulTrades', { $max: ['$totalTrades', 1] }] },
            successful: '$successfulTrades',
            total: '$totalTrades'
          },
          timestamp: new Date().toISOString()
        }
      }
    ];

    const result = await this.db.collection(ZERO0X_CONFIG.datasetNames.trades)
      .aggregate(pipeline)
      .toArray();

    const metricData = result[0] || { metric: 'success_rate', value: { rate: 0, successful: 0, total: 0 }, timestamp: new Date().toISOString() };
    await this.cacheMetric('success_rate', 'all', metricData);
    await this.storeMetric(metricData);
    analyticsEvents.emit('metric:computed', metricData);

    return metricData;
  }

  // Cache metric in Redis for quick access
  async cacheMetric(metric, scope, data) {
    const key = `zero0x:analytics:${metric}:${scope}`;
    const validation = analyticsSchemaValidator.validate(data, '/AnalyticsSchema');
    if (!validation.valid) throw new Error(`Invalid metric data: ${validation.errors.map(e => e.message).join(', ')}`);

    await this.redisClient.setex(key, ZERO0X_CONFIG.cacheTTL, JSON.stringify(data));
  }

  // Store metric in analytics dataset
  async storeMetric(metricData) {
    metricData[ZERO0X_CONFIG.specialFields.time] = new Date(metricData.timestamp);
    metricData[ZERO0X_CONFIG.specialFields.sysTime] = new Date();
    metricData.dataset = ZERO0X_CONFIG.datasetNames.analytics;

    await this.db.collection(ZERO0X_CONFIG.datasetNames.analytics).insertOne(metricData);
  }

  // Broadcast metric to WebSocket clients
  async broadcastMetric(metric, ws) {
    const cacheKey = `zero0x:analytics:${metric}:*`;
    const keys = await this.redisClient.keys(cacheKey);
    const metrics = await Promise.all(keys.map(async key => JSON.parse(await this.redisClient.get(key))));

    ws.send(JSON.stringify({
      event: 'metric_update',
      metric,
      data: metrics
    }));
  }

  // Trim analytics dataset to enforce retention
  async trimAnalyticsDataset(beforeDate) {
    if (!this.connected) await this.initialize();
    const result = await this.db.collection(ZERO0X_CONFIG.datasetNames.analytics).deleteMany({
      [ZERO0X_CONFIG.specialFields.time]: { $lt: beforeDate }
    });

    analyticsEvents.emit('analytics:trimmed', { deletedCount: result.deletedCount });
    return { deletedCount: result.deletedCount };
  }

  // Shutdown engine gracefully
  async shutdown() {
    if (this.connected) {
      await this.mongoClient.close();
      await this.redisClient.quit();
      this.wss.close();
      this.connected = false;
      analyticsEvents.emit('engine:shutdown');
    }
  }
}

// Export engine and events
module.exports = { Zero0xTradeAnalyticsEngine, analyticsEvents };

// Example usage with event listeners
if (require.main === module) {
  (async () => {
    const engine = new Zero0xTradeAnalyticsEngine();
    analyticsEvents.on('metric:computed', data => console.log('Metric:', data));
    analyticsEvents.on('error', err => console.error('Error:', err));

    try {
      await engine.initialize();
      const startDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const endDate = new Date();
      await engine.computeTradeVolume('solana', startDate, endDate);
      await engine.calculatePnL(startDate, endDate);
      await engine.computeSuccessRate(startDate, endDate);
      await engine.trimAnalyticsDataset(startDate);
    } catch (err) {
      console.error('Analytics Engine Error:', err);
    } finally {
      await engine.shutdown();
    }
  })();
}
