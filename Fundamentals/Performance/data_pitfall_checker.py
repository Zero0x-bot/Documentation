import json
import logging
from pymongo import MongoClient
import threading
from concurrent.futures import ThreadPoolExecutor

# Pitfall Checker Configuration
PITFALL_CONFIG = {
    'mongo_uri': 'mongodb://localhost:27017/zero0x_db',
    'max_fields': 100,
    'time_gap_threshold': 3600,  # seconds
    'mixed_type_threshold': 0.1  # 10% mismatch
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('data_pitfall_checker')

class DataPitfallChecker:
    def __init__(self):
        self.client = MongoClient(PITFALL_CONFIG['mongo_uri'])
        self.db = self.client['zero0x_db']
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.lock = threading.Lock()

    def check_mixed_data(self, dataset_name):
        coll = self.db[dataset_name]
        sample = coll.aggregate([{'$sample': {'size': 100}}])
        types = {}
        for doc in sample:
            for key, value in doc.items():
                t = type(value).__name__
                types.setdefault(key, set()).add(t)
        issues = [k for k, v in types.items() if len(v) > 1]
        if issues:
            logger.warning(f"Mixed types in fields: {issues}")
        return issues

    def check_excessive_backfilling(self, dataset_name):
        coll = self.db[dataset_name]
        pipeline = [
            {'$project': {'gap': {'$subtract': ['$_sysTime', '$_time']}}},
            {'$match': {'gap': {'$gt': PITFALL_CONFIG['time_gap_threshold'] * 1000}}}
        ]
        gaps = list(coll.aggregate(pipeline))
        if gaps:
            logger.warning(f"Large time gaps found: {len(gaps)} instances")
        return len(gaps)

    def check_large_fields(self, dataset_name):
        coll = self.db[dataset_name]
        pipeline = [
            {'$project': {'fields': {'$objectToArray': '$$ROOT'}}},
            {'$unwind': '$fields'},
            {'$group': {'_id': '$fields.k'}}
        ]
        fields = list(coll.aggregate(pipeline))
        if len(fields) > PITFALL_CONFIG['max_fields']:
            logger.warning(f"Excessive fields: {len(fields)} > {PITFALL_CONFIG['max_fields']}")
            return len(fields)
        return 0

    def run_checks(self, dataset_name):
        futures = [
            self.executor.submit(self.check_mixed_data, dataset_name),
            self.executor.submit(self.check_excessive_backfilling, dataset_name),
            self.executor.submit(self.check_large_fields, dataset_name)
        ]
        results = {}
        for future in futures:
            try:
                results.update({future.result().__name__: future.result()})
            except Exception as e:
                logger.error(f"Check failed: {e}")
        return results

    def shutdown(self):
        self.executor.shutdown()
        self.client.close()
        logger.info("Checker shutdown")

if __name__ == "__main__":
    checker = DataPitfallChecker()
    results = checker.run_checks('trades_dataset')
    print("Pitfall Check Results:", json.dumps(results, indent=2))
    checker.shutdown()
