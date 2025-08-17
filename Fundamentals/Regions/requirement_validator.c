#include <mongoc/mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MONGO_URI "mongodb://localhost:27017"
#define DB_NAME "zero0x_db"
#define COLLECTION_NAME "traces"

typedef struct {
    mongoc_client_t *client;
    mongoc_collection_t *collection;
    FILE *log_file;
} RequirementValidator;

void log_message(RequirementValidator *validator, const char *level, const char *msg) {
    time_t now = time(NULL);
    fprintf(validator->log_file, "[%s] %s: %s\n", ctime(&now), level, msg);
    fflush(validator->log_file);
}

RequirementValidator* init_validator() {
    RequirementValidator *validator = (RequirementValidator*)malloc(sizeof(RequirementValidator));
    if (!validator) {
        fprintf(stderr, "Memory allocation failed\n");
        return NULL;
    }

    validator->log_file = fopen("validator.log", "a");
    if (!validator->log_file) {
        free(validator);
        return NULL;
    }

    mongoc_init();
    validator->client = mongoc_client_new(MONGO_URI);
    if (!validator->client) {
        log_message(validator, "ERROR", "MongoDB client init failed");
        fclose(validator->log_file);
        free(validator);
        mongoc_cleanup();
        return NULL;
    }

    validator->collection = mongoc_client_get_collection(validator->client, DB_NAME, COLLECTION_NAME);
    log_message(validator, "INFO", "Validator initialized");
    return validator;
}

int validate_requirements(RequirementValidator *validator, const char *trace_json) {
    bson_error_t error;
    bson_t *doc = bson_new_from_json((const uint8_t *)trace_json, -1, &error);
    if (!doc) {
        log_message(validator, "ERROR", error.message);
        return -1;
    }

    // Requirement 1: Data format (must have attributes.trade_id)
    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, doc, "attributes.trade_id")) {
        log_message(validator, "ERROR", "Missing trade_id");
        bson_destroy(doc);
        return -1;
    }

    // Requirement 2: Timestamp (_time must be present)
    if (!bson_iter_init_find(&iter, doc, "_time")) {
        log_message(validator, "ERROR", "Missing _time field");
        bson_destroy(doc);
        return -1;
    }

    // Requirement 3: Log level (attributes.level must be valid)
    if (bson_iter_init_find(&iter, doc, "attributes.level")) {
        const char *level = bson_iter_utf8(&iter, NULL);
        if (strcmp(level, "info") != 0 && strcmp(level, "error") != 0 && strcmp(level, "warn") != 0) {
            log_message(validator, "ERROR", "Invalid log level");
            bson_destroy(doc);
            return -1;
        }
    } else {
        log_message(validator, "ERROR", "Missing log level");
        bson_destroy(doc);
        return -1;
    }

    if (!mongoc_collection_insert_one(validator->collection, doc, NULL, NULL, &error)) {
        log_message(validator, "ERROR", error.message);
        bson_destroy(doc);
        return -1;
    }

    log_message(validator, "INFO", "Trace validated and stored");
    bson_destroy(doc);
    return 0;
}

void cleanup_validator(RequirementValidator *validator) {
    if (validator) {
        mongoc_collection_destroy(validator->collection);
        mongoc_client_destroy(validator->client);
        mongoc_cleanup();
        fclose(validator->log_file);
        free(validator);
    }
}

int main() {
    RequirementValidator *validator = init_validator();
    if (!validator) return 1;

    const char *trace = "{\"attributes\":{\"trade_id\":\"123\",\"level\":\"info\",\"trade_type\":\"arbitrage\"},\"_time\":1697059200000}";
    if (validate_requirements(validator, trace) != 0) {
        printf("Validation failed\n");
    }

    cleanup_validator(validator);
    return 0;
}
