package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// SemconvConfig defines configuration for semantic conventions upgrades.
type SemconvConfig struct {
	MongoURI       string
	OTelSchemaURL  string
	SupportedVersions []string
	DefaultVersion   string
	Timeout          time.Duration
	MaxWorkers       int
}

// SemconvVersionUpgrader manages OTel semantic conventions upgrades.
type SemconvVersionUpgrader struct {
	client     *mongo.Client
	db         *mongo.Database
	logger     *zap.Logger
	tracer     trace.Tracer
	config     SemconvConfig
	schemaCache sync.Map // Cache for OTel schema diffs
}

// NewSemconvVersionUpgrader initializes the upgrader.
func NewSemconvVersionUpgrader(config SemconvConfig) (*SemconvVersionUpgrader, error) {
	logger, err := zap.NewProduction(zap.Fields(zap.String("component", "semconv_version_upgrader")))
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		logger.Error("MongoDB connect failed", zap.Error(err))
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	tracer := trace.NewNoopTracerProvider().Tracer("zero0x/semconv")
	return &SemconvVersionUpgrader{
		client: client,
		db:     client.Database("zero0x_db"),
		logger: logger,
		tracer: tracer,
		config: config,
	}, nil
}

// Initialize ensures necessary collections and indexes.
func (u *SemconvVersionUpgrader) Initialize(ctx context.Context) error {
	_, err := u.db.Collection("schema_changes").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"version": 1},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		u.logger.Error("Index creation failed", zap.Error(err))
		return fmt.Errorf("create index: %w", err)
	}

	u.logger.Info("Semconv upgrader initialized", zap.Strings("versions", u.config.SupportedVersions))
	return nil
}

// DetermineVersionChanges fetches and caches schema changes between versions.
func (u *SemconvVersionUpgrader) DetermineVersionChanges(ctx context.Context, fromVersion, toVersion string) (map[string]string, error) {
	cacheKey := fmt.Sprintf("%s:%s", fromVersion, toVersion)
	if cached, ok := u.schemaCache.Load(cacheKey); ok {
		return cached.(map[string]string), nil
	}

	// Fetch schema files from OTel (simulated for realism)
	url := fmt.Sprintf("%s/%s/trace.yaml", u.config.OTelSchemaURL, toVersion)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create schema request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch schema: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}

	// Simulate parsing schema changes (real-world would parse YAML)
	changes := map[string]string{
		"attributes.custom.trade_type": "attributes.trade.type",
		"attributes.custom.chain_id":   "attributes.chain.id",
	}

	u.schemaCache.Store(cacheKey, changes)
	u.db.Collection("schema_changes").InsertOne(ctx, bson.M{
		"fromVersion": fromVersion,
		"toVersion":   toVersion,
		"changes":     changes,
		"_time":       time.Now(),
	})

	u.logger.Info("Schema changes cached", zap.String("from", fromVersion), zap.String("to", toVersion))
	return changes, nil
}

// UpgradeTraces applies semantic conventions upgrades concurrently.
func (u *SemconvVersionUpgrader) UpgradeTraces(ctx context.Context, fromVersion, toVersion string) (int, error) {
	_, span := u.tracer.Start(ctx, "UpgradeTraces")
	defer span.End()

	changes, err := u.DetermineVersionChanges(ctx, fromVersion, toVersion)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("determine changes: %w", err)
	}

	coll := u.db.Collection("traces")
	cursor, err := coll.Find(ctx, bson.M{"attributes.semconv_version": fromVersion})
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("find traces: %w", err)
	}
	defer cursor.Close(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(u.config.MaxWorkers)
	count := 0

	for cursor.Next(ctx) {
		var traceData map[string]interface{}
		if err := cursor.Decode(&traceData); err != nil {
			return count, fmt.Errorf("decode trace: %w", err)
		}

		g.Go(func() error {
			remapped := u.applySchemaChanges(traceData, changes)
			remapped["attributes.semconv_version"] = toVersion
			_, err := coll.UpdateOne(ctx, bson.M{"_id": traceData["_id"]}, bson.M{"$set": remapped})
			if err != nil {
				return fmt.Errorf("update trace: %w", err)
			}
			return nil
		})
		count++
	}

	if err := g.Wait(); err != nil {
		span.RecordError(err)
		u.logger.Error("Upgrade failed", zap.Error(err))
		return count, fmt.Errorf("upgrade: %w", err)
	}

	u.logger.Info("Traces upgraded", zap.String("from", fromVersion), zap.String("to", toVersion), zap.Int("count", count))
	span.AddEvent("TracesUpgraded", trace.WithAttributes(
		attribute.Int("count", count),
	))
	return count, nil
}

// applySchemaChanges remaps attributes based on schema changes.
func (u *SemconvVersionUpgrader) applySchemaChanges(data map[string]interface{}, changes map[string]string) map[string]interface{} {
	remapped := make(map[string]interface{})
	for k, v := range data {
		if newKey, ok := changes[k]; ok {
			remapped[newKey] = v
		} else {
			remapped[k] = v
		}
	}
	return remapped
}

// UpdateQueriesForNewLocations updates saved queries/monitors for new attribute locations.
func (u *SemconvVersionUpgrader) UpdateQueriesForNewLocations(ctx context.Context, toVersion string) (int, error) {
	changes, err := u.DetermineVersionChanges(ctx, u.config.DefaultVersion, toVersion)
	if err != nil {
		return 0, fmt.Errorf("determine changes: %w", err)
	}

	coll := u.db.Collection("saved_queries")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("find queries: %w", err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		var queryDoc map[string]interface{}
		if err := cursor.Decode(&queryDoc); err != nil {
			return count, fmt.Errorf("decode query: %w", err)
		}

		query, ok := queryDoc["query"].(string)
		if !ok {
			continue
		}

		var queryMap map[string]interface{}
		if err := json.Unmarshal([]byte(query), &queryMap); err != nil {
			continue
		}

		remapped := u.applySchemaChanges(queryMap, changes)
		newQuery, err := json.Marshal(remapped)
		if err != nil {
			continue
		}

		_, err = coll.UpdateOne(ctx, bson.M{"_id": queryDoc["_id"]}, bson.M{
			"$set": bson.M{
				"query":         string(newQuery),
				"semconv_version": toVersion,
				"_sysTime":      time.Now(),
			},
		})
		if err != nil {
			return count, fmt.Errorf("update query: %w", err)
		}
		count++
	}

	u.logger.Info("Queries updated", zap.String("toVersion", toVersion), zap.Int("count", count))
	return count, nil
}

// UpdateQueriesForBothLocations adds coalesce for old/new attribute locations.
func (u *SemconvVersionUpgrader) UpdateQueriesForBothLocations(ctx context.Context, toVersion string) (int, error) {
	changes, err := u.DetermineVersionChanges(ctx, u.config.DefaultVersion, toVersion)
	if err != nil {
		return 0, fmt.Errorf("determine changes: %w", err)
	}

	coll := u.db.Collection("saved_queries")
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("find queries: %w", err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		var queryDoc map[string]interface{}
		if err := cursor.Decode(&queryDoc); err != nil {
			return count, fmt.Errorf("decode query: %w", err)
		}

		query, ok := queryDoc["query"].(string)
		if !ok {
			continue
		}

		var queryMap map[string]interface{}
		if err := json.Unmarshal([]byte(query), &queryMap); err != nil {
			continue
		}

		coalesced := make(map[string]interface{})
		for k, v := range queryMap {
			for oldKey, newKey := range changes {
				if k == newKey {
					coalesced[k] = bson.M{"$coalesce": []string{oldKey, newKey}}
				} else {
					coalesced[k] = v
				}
			}
		}

		newQuery, err := json.Marshal(coalesced)
		if err != nil {
			continue
		}

		_, err = coll.UpdateOne(ctx, bson.M{"_id": queryDoc["_id"]}, bson.M{
			"$set": bson.M{
				"query":         string(newQuery),
				"semconv_version": toVersion,
				"_sysTime":      time.Now(),
			},
		})
		if err != nil {
			return count, fmt.Errorf("update query: %w", err)
		}
		count++
	}

	u.logger.Info("Queries updated with coalesce", zap.String("toVersion", toVersion), zap.Int("count", count))
	return count, nil
}

// Shutdown closes connections.
func (u *SemconvVersionUpgrader) Shutdown() error {
	if err := u.client.Disconnect(context.Background()); err != nil {
		u.logger.Error("Shutdown failed", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}
	u.logger.Info("Semconv upgrader shutdown")
	return u.logger.Sync()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	config := SemconvConfig{
		MongoURI:         "mongodb://localhost:27017/zero0x_db",
		OTelSchemaURL:    "https://opentelemetry.io/schemas",
		SupportedVersions: []string{"1.25", "1.32"},
		DefaultVersion:   "1.32",
		Timeout:          10 * time.Second,
		MaxWorkers:       4,
	}

	upgrader, err := NewSemconvVersionUpgrader(config)
	if err != nil {
		logger.Fatal("Upgrader init failed", zap.Error(err))
	}

	ctx := context.Background()
	if err := upgrader.Initialize(ctx); err != nil {
		logger.Fatal("Initialize failed", zap.Error(err))
	}

	// Determine changes
	changes, err := upgrader.DetermineVersionChanges(ctx, "1.25", "1.32")
	if err != nil {
		logger.Error("Determine changes failed", zap.Error(err))
	} else {
		logger.Info("Changes detected", zap.Any("changes", changes))
	}

	// Upgrade traces
	count, err := upgrader.UpgradeTraces(ctx, "1.25", "1.32")
	if err != nil {
		logger.Error("Upgrade traces failed", zap.Error(err))
	} else {
		logger.Info("Traces upgraded", zap.Int("count", count))
	}

	// Update queries
	count, err = upgrader.UpdateQueriesForNewLocations(ctx, "1.32")
	if err != nil {
		logger.Error("Update queries failed", zap.Error(err))
	} else {
		logger.Info("Queries updated", zap.Int("count", count))
	}

	// Update queries with coalesce
	count, err = upgrader.UpdateQueriesForBothLocations(ctx, "1.32")
	if err != nil {
		logger.Error("Update coalesce queries failed", zap.Error(err))
	} else {
		logger.Info("Coalesce queries updated", zap.Int("count", count))
	}

	if err := upgrader.Shutdown(); err != nil {
		logger.Fatal("Shutdown failed", zap.Error(err))
	}
}
