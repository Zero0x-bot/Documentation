#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <json-c/json.h>

#define MONGO_URI "mongodb://localhost:27017"
#define DB_NAME "zero0x_db"
#define COLLECTION_NAME "trace_schema"
#define OUTPUT_FILE "trace_schema_doc.json"

typedef struct {
    mongoc_client_t *client;
    mongoc_collection_t *collection;
    FILE *log_file;
} TraceSchemaDoc;

void log_message(TraceSchemaDoc *doc, const char *level, const char *msg) {
    time_t now = time(NULL);
    fprintf(doc->log_file, "[%s] %s: %s\n", ctime(&now), level, msg);
    fflush(doc->log_file);
}

TraceSchemaDoc* init_trace_schema_doc() {
    TraceSchemaDoc *doc = (TraceSchemaDoc*)malloc(sizeof(TraceSchemaDoc));
    if (!doc) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }

    doc->log_file = fopen("trace_schema_doc.log", "a");
    if (!doc->log_file) {
        free(doc);
        return NULL;
    }

    mongoc_init();
    doc->client = mongoc_client_new(MONGO_URI);
    if (!doc->client) {
        log_message(doc, "ERROR", "MongoDB client init failed");
        fclose(doc->log_file);
        free(doc);
        mongoc_cleanup();
        return NULL;
    }

    doc->collection = mongoc_client_get_collection(doc->client, DB_NAME, COLLECTION_NAME);
    bson_t *index = BCON_NEW("schema_version", BCON_INT32(1));
    bson_t *options = BCON_NEW("unique", BCON_BOOL(true));
    bson_error_t error;
    mongoc_collection_create_index(doc->collection, index, options, &error);
    bson_destroy(index);
    bson_destroy(options);

    log_message(doc, "INFO", "Trace schema doc initialized");
    return doc;
}

int generate_schema_doc(TraceSchemaDoc *doc, const char *version) {
    struct json_object *schema = json_object_new_object();
    struct json_object *regions = json_object_new_array();
    json_object_array_add(regions, json_object_new_string("US: api.zero0x.trade"));
    json_object_array_add(regions, json_object_new_string("EU: api.eu.zero0x.trade"));
    json_object_object_add(schema, "regions", regions);

    struct json_object *attributes = json_object_new_object();
    json_object_object_add(attributes, "trade_id", json_object_new_string("string"));
    json_object_object_add(attributes, "trade_type", json_object_new_string("string"));
    json_object_object_add(attributes, "level", json_object_new_string("enum: info, warn, error"));
    json_object_object_add(schema, "attributes", attributes);
    json_object_object_add(schema, "version", json_object_new_string(version));
    json_object_object_add(schema, "_time", json_object_new_string("ISO8601"));

    bson_t *bson_doc = bson_new_from_json((const uint8_t *)json_object_to_json_string(schema), -1, NULL);
    if (!bson_doc) {
        log_message(doc, "ERROR", "Failed to convert JSON to BSON");
        json_object_put(schema);
        return -1;
    }

    bson_error_t error;
    if (!mongoc_collection_insert_one(doc->collection, bson_doc, NULL, NULL, &error)) {
        log_message(doc, "ERROR", error.message);
        bson_destroy(bson_doc);
        json_object_put(schema);
        return -1;
    }

    FILE *output = fopen(OUTPUT_FILE, "w");
    if (!output) {
        log_message(doc, "ERROR", "Failed to open output file");
        bson_destroy(bson_doc);
        json_object_put(schema);
        return -1;
    }
    fprintf(output, "%s\n", json_object_to_json_string_ext(schema, JSON_C_TO_STRING_PRETTY));
    fclose(output);

    log_message(doc, "INFO", "Schema documentation generated");
    bson_destroy(bson_doc);
    json_object_put(schema);
    return 0;
}

void cleanup_trace_schema_doc(TraceSchemaDoc *doc) {
    if (doc) {
        mongoc_collection_destroy(doc->collection);
        mongoc_client_destroy(doc->client);
        mongoc_cleanup();
        fclose(doc->log_file);
        free(doc);
        log_message(doc, "INFO", "Trace schema doc cleanup");
    }
}

int main() {
    TraceSchemaDoc *doc = init_trace_schema_doc();
    if (!doc) return 1;

    if (generate_schema_doc(doc, "1.32") != 0) {
        printf("Failed to generate schema documentation\n");
    }

    cleanup_trace_schema_doc(doc);
    return 0;
}
