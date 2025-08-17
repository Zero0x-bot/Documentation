const EventEmitter = require('events');

// Optimizer Configuration
const OPTIMIZER_CONFIG = {
  maxFilterCombinations: 100,
  preferredOperators: ['==', '>=', '<='] // Efficient operators
};

// Optimizer Events Emitter
class OptimizerEvents extends EventEmitter {}
const optimizerEvents = new OptimizerEvents();

// QueryOptimizer Class
class QueryOptimizer {
  constructor() {
    this.filters = [];
  }

  // Optimize query by reordering filters and operators
  optimizeQuery(queryText, dataset) {
    const filters = this.parseFilters(queryText);
    const optimizedFilters = this.reorderFieldSpecificFilters(filters);
    const optimizedQuery = this.rewriteQuery(optimizedFilters, dataset);

    optimizerEvents.emit('query:optimized', { original: queryText, optimized: optimizedQuery });
    return optimizedQuery;
  }

  // Parse filters from query text
  parseFilters(queryText) {
    const filterRegex = /(\w+\.\w+\s*(==|>=|<=|!=)\s*['"]?[^'"]+['"]?)/g;
    const filters = [];
    let match;
    while ((match = filterRegex.exec(queryText)) !== null) {
      filters.push(match[1]);
    }
    return filters;
  }

  // Reorder field-specific filters (index-friendly first)
  reorderFieldSpecificFilters(filters) {
    return filters.sort((a, b) => {
      const aIsFieldSpecific = a.includes('.');
      const bIsFieldSpecific = b.includes('.');
      if (aIsFieldSpecific && !bIsFieldSpecific) return -1;
      if (!aIsFieldSpecific && bIsFieldSpecific) return 1;
      const aOp = a.match(/(==|>=|<=|!=)/)[1];
      const bOp = b.match(/(==|>=|<=|!=)/)[1];
      return OPTIMIZER_CONFIG.preferredOperators.indexOf(aOp) - OPTIMIZER_CONFIG.preferredOperators.indexOf(bOp);
    });
  }

  // Rewrite query with optimized filters
  rewriteQuery(filters, dataset) {
    return `['${dataset}'] | where ${filters.join(' and ')}`;
  }
}

module.exports = QueryOptimizer;

// Example usage
if (require.main === module) {
  const optimizer = new QueryOptimizer();
  optimizerEvents.on('query:optimized', data => console.log('Optimized:', data));
  const query = "['trades_dataset'] | where trade.type=='arbitrage' and chain_id>=1";
  const optimized = optimizer.optimizeQuery(query, 'trades_dataset');
  console.log('Optimized Query:', optimized);
}
