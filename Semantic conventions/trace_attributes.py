import datetime
import json
import threading
from pymongo import MongoClient
import logging
from opentelemetry import trace

# Trace Attributes Configuration
TRACE_CONFIG = {
    'mongo_uri': 'mongodb://localhost:27017/zero0x_db',
    'supported_versions': ['1.25', '1.32'],
    'default_version': '1.32',
    'semconv_schema_url': 'https://opentelemetry.io/schemas/1.32'
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('trace_attributes')

class TraceAttributesHandler:
    def __init__(self):
        self.client = MongoClient(TRACE_CONFIG['mongo_uri'])
        self.db = self.client['zero0x_db']
        self.collection = self.db['traces']
        self.tracer = trace.get_tracer('zero0x.traces')
        self.lock = threading.Lock()
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.collection.create_index([('attributes.trade_id', 1), ('_time', -1)], sparse=True)
        logger.info("Indexes ensured for trace collection")

    def process_trace(self, trace_data, semconv_version):
        with self.tracer.start_as_current_span("process_trace") as span:
            if semconv_version not in TRACE_CONFIG['supported_versions']:
                semconv_version = TRACE_CONFIG['default_version']
                logger.warning(f"Unsupported version, using default: {semconv_version}")
                span.set_attribute("version_fallback", True)

            trace_data['attributes']['semconv_version'] = semconv_version
            trace_data['_time'] = datetime.datetime.utcnow()
            trace_data['_sysTime'] = datetime.datetime.utcnow()

            with self.lock:
                result = self.collection.insert_one(trace_data)
                logger.info(f"Trace inserted: {result.inserted_id}")

            span.set_attribute("trace_id", str(result.inserted_id))
            span.set_attribute("semconv_version", semconv_version)

        return result.inserted_id

    def retrieve_traces(self, query, version):
        with self.tracer.start_as_current_span("retrieve_traces") as span:
            cursor = self.collection.find(query)
            traces = list(cursor)
            logger.info(f"Retrieved {len(traces)} traces for version {version}")
            span.set_attribute("result_count", len(traces))
            return traces

    def shutdown(self):
        self.client.close()
        logger.info("Trace handler shutdown")

if __name__ == "__main__":
    handler = TraceAttributesHandler()
    trace_data = {
        "attributes": {
            "trade_type": "arbitrage",
            "chain_id": "solana"
        }
    }
    trace_id = handler.process_trace(trace_data, '1.32')
    results = handler.retrieve_traces({"attributes.semconv_version": '1.32'}, '1.32')
    print(f"Processed trace ID: {trace_id}, Retrieved: {len(results)}")
    handler.shutdown()
