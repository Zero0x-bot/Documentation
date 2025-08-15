const { MongoClient, Binary } = require('mongodb');
const jwt = require('jsonwebtoken');
const { createHash } = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { Mutex } = require('async-mutex');

// Audit Trail Configuration
const AUDIT_CONFIG = {
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/zero0x_db',
  jwtSecret: process.env.JWT_SECRET || 'audit_secret',
  auditCollection: 'audit_trail',
  immutableFields: ['action', 'userId', 'timestamp', 'dataHash']
};

// Compliance Audit Trail with JWT and Mutex Locking
class ComplianceAuditTrail {
  constructor() {
    this.client = new MongoClient(AUDIT_CONFIG.mongoUri);
    this.db = null;
    this.mutex = new Mutex();
    this.connected = false;
  }

  // Initialize with capped collection for immutability
  async initialize() {
    await this.client.connect();
    this.db = this.client.db();
    await this.db.createCollection(AUDIT_CONFIG.auditCollection, { capped: true, size: 100000000, max: 100000 }); // 100MB capped
    this.connected = true;
  }

  // Log action with JWT signing and hash chaining
  async logAction(action, userId, data) {
    if (!this.connected) await this.initialize();

    const release = await this.mutex.acquire();
    try {
      const prevAudit = await this.db.collection(AUDIT_CONFIG.auditCollection).findOne({}, { sort: { timestamp: -1 } });
      const prevHash = prevAudit ? prevAudit.dataHash : 'genesis';

      const auditEntry = {
        action,
        userId,
        timestamp: new Date(),
        data,
        prevHash,
        dataHash: createHash('sha256').update(JSON.stringify(data) + prevHash).digest('hex')
      };

      const token = jwt.sign(auditEntry, AUDIT_CONFIG.jwtSecret, { expiresIn: '1y' });
      auditEntry.jwt = token;

      const result = await this.db.collection(AUDIT_CONFIG.auditCollection).insertOne(auditEntry);
      return { auditId: result.insertedId, hash: auditEntry.dataHash };
    } finally {
      release();
    }
  }

  // Verify audit trail integrity with chain validation
  async verifyTrail(startId, endId) {
    if (!this.connected) await this.initialize();

    const cursor = this.db.collection(AUDIT_CONFIG.auditCollection).find({
      _id: { $gte: new Binary(uuidv4().replace(/-/g, ''), 3), $lte: new Binary(uuidv4().replace(/-/g, ''), 3) } // UUID binary for IDs
    }).sort({ timestamp: 1 });

    const entries = await cursor.toArray();
    let isValid = true;
    let prevHash = 'genesis';

    for (const entry of entries) {
      if (entry.prevHash !== prevHash) {
        isValid = false;
        break;
      }
      const recalculatedHash = createHash('sha256').update(JSON.stringify(entry.data) + entry.prevHash).digest('hex');
      if (recalculatedHash !== entry.dataHash) {
        isValid = false;
        break;
      }
      try {
        jwt.verify(entry.jwt, AUDIT_CONFIG.jwtSecret);
      } catch (err) {
        isValid = false;
        break;
      }
      prevHash = entry.dataHash;
    }

    return { isValid, entryCount: entries.length };
  }

  // Prune old audits while maintaining chain (complex: skip capped logic)
  async pruneAudits(beforeDate) {
    if (!this.connected) await this.initialize();

    const oldEntries = await this.db.collection(AUDIT_CONFIG.auditCollection).find({ timestamp: { $lt: beforeDate } }).toArray();
    if (oldEntries.length === 0) return { pruned: 0 };

    // Re-chain remaining entries
    await this.db.collection(AUDIT_CONFIG.auditCollection).deleteMany({ timestamp: { $lt: beforeDate } });
    const remaining = await this.db.collection(AUDIT_CONFIG.auditCollection).findOne({}, { sort: { timestamp: 1 } });
    if (remaining) {
      await this.db.collection(AUDIT_CONFIG.auditCollection).updateOne(
        { _id: remaining._id },
        { $set: { prevHash: 'genesis_pruned' } }
      );
    }

    return { pruned: oldEntries.length };
  }

  // Shutdown
  async shutdown() {
    await this.client.close();
    this.connected = false;
  }
}

module.exports = ComplianceAuditTrail;

// Example usage with verification
if (require.main === module) {
  (async () => {
    const auditTrail = new ComplianceAuditTrail();
    try {
      await auditTrail.initialize();
      const { auditId: id1 } = await auditTrail.logAction('trade_execute', 'user1', { chain: 'solana', amount: 100 });
      const { auditId: id2 } = await auditTrail.logAction('trade_confirm', 'user1', { chain: 'ethereum', amount: 50 });
      const verification = await auditTrail.verifyTrail(id1, id2);
      console.log('Trail Valid:', verification.isValid);

      const beforeDate = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
      await auditTrail.pruneAudits(beforeDate);
    } catch (err) {
      console.error('Audit Trail Error:', err);
    } finally {
      await auditTrail.shutdown();
    }
  })();
}
