#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MONGO_URI "mongodb://localhost:27017"
#define DB_NAME "zero0x_db"
#define COLLECTION_NAME "traces"
#define MAX_REGIONS 2

typedef struct {
    const char *region_id;
    const char *endpoint;
    int max_retries;
} RegionConfig;

typedef struct {
    mongoc_client_t *client;
    mongoc_collection_t *collection;
    RegionConfig regions[MAX_REGIONS];
    FILE *log_file;
} TraceDispatcher;

void log_message(TraceDispatcher *dispatcher, const char *level, const char *msg) {
    time_t now = time(NULL);
    fprintf(dispatcher->log_file, "[%s] %s: %s\n", ctime(&now), level, msg);
    fflush(dispatcher->log_file);
}

TraceDispatcher* init_dispatcher() {
    TraceDispatcher *dispatcher = (TraceDispatcher*)malloc(sizeof(TraceDispatcher));
    if (!dispatcher) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }

    dispatcher->log_file = fopen("dispatcher.log", "a");
    if (!dispatcher->log_file) {
        free(dispatcher);
        return NULL;
    }

    mongoc_init();
    dispatcher->client = mongoc_client_new(MONGO_URI);
    if (!dispatcher->client) {
        log_message(dispatcher, "ERROR", "MongoDB client init failed");
        fclose(dispatcher->log_file);
        free(dispatcher);
        mongoc_cleanup();
        return NULL;
    }

    dispatcher->collection = mongoc_client_get_collection(dispatcher->client, DB_NAME, COLLECTION_NAME);
    dispatcher->regions[0] = (RegionConfig){"US", "us.zero0x.trade", 3};
    dispatcher->regions[1] = (RegionConfig){"EU", "eu.zero0x.trade", 3};

    bson_t *index = BCON_NEW("attributes.trade_id", BCON_INT32(1), "_time", BCON_INT32(-1));
    bson_t *options = BCON_NEW("sparse", BCON_BOOL(true));
    bson_error_t error;
    mongoc_collection_create_index(dispatcher->collection, index, options, &error);
    bson_destroy(index);
    bson_destroy(options);

    log_message(dispatcher, "INFO", "Dispatcher initialized");
    return dispatcher;
}

int dispatch_trace(TraceDispatcher *dispatcher, const char *region_id, const char *trace_json) {
    bson_error_t error;
    bson_t *doc = bson_new_from_json((const uint8_t *)trace_json, -1, &error);
    if (!doc) {
        log_message(dispatcher, "ERROR", error.message);
        return -1;
    }

    bson_t *attributes = bson_new();
    BCON_APPEND(attributes, "region_id", BCON_UTF8(region_id), "semconv_version", BCON_UTF8("1.32"));
    BCON_APPEND(doc, "attributes", BCON_DOCUMENT(attributes), "_time", BCON_DATE_TIME(time(NULL) * 1000));

    int region_idx = -1;
    for (int i = 0; i < MAX_REGIONS; i++) {
        if (strcmp(dispatcher->regions[i].region_id, region_id) == 0) {
            region_idx = i;
            break;
        }
    }
    if (region_idx == -1) {
        log_message(dispatcher, "ERROR", "Invalid region ID");
        bson_destroy(doc);
        return -1;
    }

    int retries = 0;
    while (retries < dispatcher->regions[region_idx].max_retries) {
        if (mongoc_collection_insert_one(dispatcher->collection, doc, NULL, NULL, &error)) {
            char msg[256];
            snprintf(msg, sizeof(msg), "Trace dispatched to %s", region_id);
            log_message(dispatcher, "INFO", msg);
            bson_destroy(doc);
            return 0;
        }
        retries++;
        log_message(dispatcher, "WARN", error.message);
    }

    log_message(dispatcher, "ERROR", "Max retries reached");
    bson_destroy(doc);
    return -1;
}

void cleanup_dispatcher(TraceDispatcher *dispatcher) {
    if (dispatcher) {
        mongoc_collection_destroy(dispatcher->collection);
        mongoc_client_destroy(dispatcher->client);
        mongoc_cleanup();
        fclose(dispatcher->log_file);
        free(dispatcher);
    }
}

int main() {
    TraceDispatcher *dispatcher = init_dispatcher();
    if (!dispatcher) return 1;

    const char *trace = "{\"attributes\":{\"trade_type\":\"arbitrage\",\"trade_id\":\"123\"}}";
    if (dispatch_trace(dispatcher, "US", trace) != 0) {
        printf("Failed to dispatch trace to US\n");
    }

    cleanup_dispatcher(dispatcher);
    return 0;
}
