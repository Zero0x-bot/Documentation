#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <json-c/json.h>

#define MONGO_URI "mongodb://localhost:27017"
#define DB_NAME "zero0x_db"
#define COLLECTION_NAME "query_logs"
#define OUTPUT_FILE "query_usage_doc.json"

typedef struct {
    mongoc_client_t *client;
    mongoc_collection_t *collection;
    FILE *log_file;
} QueryUsageDoc;

void log_message(QueryUsageDoc *doc, const char *level, const char *msg) {
    time_t now = time(NULL);
    fprintf(doc->log_file, "[%s] %s: %s\n", ctime(&now), level, msg);
    fflush(doc->log_file);
}

QueryUsageDoc* init_query_usage_doc() {
    QueryUsageDoc *doc = (QueryUsageDoc*)malloc(sizeof(QueryUsageDoc));
    if (!doc) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }

    doc->log_file = fopen("query_usage_doc.log", "a");
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
    log_message(doc, "INFO", "Query usage doc initialized");
    return doc;
}

int generate_usage_doc(QueryUsageDoc *doc, const char *org_id) {
    bson_t *query = BCON_NEW("org_id", BCON_UTF8(org_id));
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(doc->collection, query, NULL, NULL);
    bson_error_t error;

    struct json_object *doc_json = json_object_new_object();
    struct json_object *queries = json_object_new_array();
    double total_gb_hours = 0.0;

    const bson_t *bson_doc;
    while (mongoc_cursor_next(cursor, &bson_doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, bson_doc, "duration_ms") && bson_iter_init_find(&iter, bson_doc, "memory_mb")) {
            double duration_ms = bson_iter_double(&iter);
            double memory_mb = bson_iter_double(&iter);
            double gb_hours = (memory_mb / 1024.0) * (duration_ms / (1000.0 * 3600.0));
            total_gb_hours += gb_hours;

            struct json_object *query_entry = json_object_new_object();
            json_object_object_add(query_entry, "query_text", json_object_new_string(bson_iter_find(&iter, "query_text") ? bson_iter_utf8(&iter, NULL) : ""));
            json_object_object_add(query_entry, "gb_hours", json_object_new_double(gb_hours));
            json_object_array_add(queries, query_entry);
        }
    }

    json_object_object_add(doc_json, "org_id", json_object_new_string(org_id));
    json_object_object_add(doc_json, "total_gb_hours", json_object_new_double(total_gb_hours));
    json_object_object_add(doc_json, "optimization_tip", json_object_new_string("Use field-specific filters first to reduce GB-hours"));
    json_object_object_add(doc_json, "queries", queries);

    FILE *output = fopen(OUTPUT_FILE, "w");
    if (!output) {
        log_message(doc, "ERROR", "Failed to open output file");
        json_object_put(doc_json);
        mongoc_cursor_destroy(cursor);
        bson_destroy(query);
        return -1;
    }
    fprintf(output, "%s\n", json_object_to_json_string_ext(doc_json, JSON_C_TO_STRING_PRETTY));
    fclose(output);

    log_message(doc, "INFO", "Query usage documentation generated");
    json_object_put(doc_json);
    mongoc_cursor_destroy(cursor);
    bson_destroy(query);
    return 0;
}

void cleanup_query_usage_doc(QueryUsageDoc *doc) {
    if (doc) {
        mongoc_collection_destroy(doc->collection);
        mongoc_client_destroy(doc->client);
        mongoc_cleanup();
        fclose(doc->log_file);
        free(doc);
        log_message(doc, "INFO", "Query usage doc cleanup");
    }
}

int main() {
    QueryUsageDoc *doc = init_query_usage_doc();
    if (!doc) return 1;

    if (generate_usage_doc(doc, "org123") != 0) {
        printf("Failed to generate query usage documentation\n");
    }

    cleanup_query_usage_doc(doc);
    return 0;
}
