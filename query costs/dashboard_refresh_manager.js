const EventEmitter = require('events');
const { setTimeout } = require('timers/promises');

// Dashboard Configuration
const DASHBOARD_CONFIG = {
  minRefreshIntervalMs: 60 * 1000, // 1 minute
  maxConcurrentRefreshes: 5
};

// Dashboard Events Emitter
class DashboardEvents extends EventEmitter {}
const dashboardEvents = new DashboardEvents();

// DashboardRefreshManager Class
class DashboardRefreshManager {
  constructor() {
    this.schedules = new Map(); // dashboardID -> intervalMs
    this.activeRefreshes = 0;
  }

  // Schedule dashboard refresh
  scheduleRefresh(dashboardID, queryText, intervalMs) {
    if (intervalMs < DASHBOARD_CONFIG.minRefreshIntervalMs) {
      intervalMs = DASHBOARD_CONFIG.minRefreshIntervalMs;
    }

    this.schedules.set(dashboardID, { queryText, intervalMs });
    this.runRefreshLoop(dashboardID);
    dashboardEvents.emit('refresh:scheduled', { dashboardID, intervalMs });
  }

  // Run refresh loop
  async runRefreshLoop(dashboardID) {
    const schedule = this.schedules.get(dashboardID);
    if (!schedule) return;

    while (this.schedules.has(dashboardID)) {
      if (this.activeRefreshes < DASHBOARD_CONFIG.maxConcurrentRefreshes) {
        this.activeRefreshes++;
        try {
          // Simulate query execution
          await setTimeout(1000);
          dashboardEvents.emit('refresh:executed', { dashboardID, query: schedule.queryText });
        } catch (err) {
          dashboardEvents.emit('refresh:error', { dashboardID, error: err.message });
        } finally {
          this.activeRefreshes--;
        }
      }
      await setTimeout(schedule.intervalMs);
    }
  }

  // Stop refresh
  stopRefresh(dashboardID) {
    this.schedules.delete(dashboardID);
    dashboardEvents.emit('refresh:stopped', { dashboardID });
  }
}

module.exports = DashboardRefreshManager;

// Example usage
if (require.main === module) {
  const manager = new DashboardRefreshManager();
  dashboardEvents.on('refresh:executed', data => console.log('Refresh:', data));
  manager.scheduleRefresh('dash1', 'SELECT * FROM trades', 120000);
  setTimeout(() => manager.stopRefresh('dash1'), 300000);
}
