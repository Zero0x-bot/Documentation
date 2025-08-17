#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <json-c/json.h>

#define MONGO_URI "mongodb://localhost:27017"
#define DB_NAME "zero0x_db"
#define COLLECTION_NAME "requirements"
#define OUTPUT_FILE "system_requirements_doc.json"

typedef struct {
    mongoc_client_t *client;
    mongoc_collection_t *collection;
    FILE *log_file;
} RequirementsDoc;

void log_message(RequirementsDoc *doc, const char *level, const char *msg) {
    time_t now = time(NULL);
    fprintf(doc->log_file, "[%s] %s: %s\n", ctime(&now), level, msg);
    fflush(doc->log_file);
}

RequirementsDoc* init_requirements_doc() {
    RequirementsDoc *doc = (RequirementsDoc*)malloc(sizeof(RequirementsDoc));
    if (!doc) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }

    doc->log_file = fopen("requirements_doc.log", "a");
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
    log_message(doc, "INFO", "Requirements doc initialized");
    return doc;
}

int generate_requirements_doc(RequirementsDoc *doc) {
    struct json_object *req_doc = json_object_new_object();
    struct json_object *requirements = json_object_new_array();

    struct json_object *req1 = json_object_new_object();
    json_object_object_add(req1, "name", json_object_new_string("Data Format"));
    json_object_object_add(req1, "description", json_object_new_string("Must include attributes.trade_id (string)"));
    json_object_array_add(requirements, req1);

    struct json_object *req2 = json_object_new_object();
    json_object_object_add(req2, "name", json_object_new_string("Timestamp"));
    json_object_object_add(req2, "description", json_object_new_string("Must include _time field in ISO8601 format"));
    json_object_array_add(requirements, req2);

    struct json_object *req3 = json_object_new_object();
    json_object_object_add(req3, "name", json_object_new_string("Log Level"));
    json_object_object_add(req3, "description", json_object_new_string("Must include attributes.level (enum: info, warn, error)"));
    json_object_array_add(requirements, req3);

    json_object_object_add(req_doc, "requirements", requirements);
    json_object_object_add(req_doc, "last_updated", json_object_new_string(ctime(&(time_t){time(NULL)})));

    bson_t *bson_doc = bson_new_from_json((const uint8_t *)json_object_to_json_string(req_doc), -1, NULL);
    if (!bson_doc) {
        log_message(doc, "ERROR", "Failed to convert JSON to BSON");
        json_object_put(req_doc);
        return -1;
    }

    bson_error_t error;
    if (!mongoc_collection_insert_one(doc->collection, bson_doc, NULL, NULL, &error)) {
        log_message(doc, "ERROR", error.message);
        bson_destroy(bson_doc);
        json_object_put(req_doc);
        return -1;
    }

    FILE *output = fopen(OUTPUT_FILE, "w");
    if (!output) {
        log_message(doc, "ERROR", "Failed to open output file");
        bson_destroy(bson_doc);
        json_object_put(req_doc);
        return -1;
    }
    fprintf(output, "%s\n", json_object_to_json_string_ext(req_doc, JSON_C_TO_STRING_PRETTY));
    fclose(output);

    log_message(doc, "INFO", "System requirements documentation generated");
    bson_destroy(bson_doc);
    json_object_put(req_doc);
    return 0;
}

void cleanup_requirements_doc(RequirementsDoc *doc) {
    if (doc) {
        mongoc_collection_destroy(doc->collection);
        mongoc_client_destroy(doc->client);
        mongoc_cleanup();
        fclose(doc->log_file);
        free(doc);
        log_message(doc, "INFO", "Requirements doc cleanup");
    }
}

int main() {
    RequirementsDoc *doc = init_requirements_doc();
    if (!doc) return 1;

    if (generate_requirements_doc(doc) != 0) {
        printf("Failed to generate requirements documentation\n");
    }

    cleanup_requirements_doc(doc);
    return 0;
}
