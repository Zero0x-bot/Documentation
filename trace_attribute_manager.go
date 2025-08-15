package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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

// TraceConfig defines Zero0x trace configuration.
type TraceConfig struct {
	MongoURI          string
	SupportedVersions []string
	DefaultVersion    string
	SemconvSchemaURL  string
	MaxRetries        int
	Timeout           time.Duration
}

// TraceAttributeManager handles trace attributes with versioning.
type TraceAttributeManager struct {
	client     *mongo.Client
	db         *mongo.Database
	logger     *zap.Logger
	tracer     trace.Tracer
	config     TraceConfig
	cache      sync.Map // Cache for schema mappings
	versions   map[string]map[string]string // version -> oldAttr -> newAttr
	mu         sync.Mutex
}

// NewTraceAttributeManager initializes the manager with OTel and MongoDB.
func NewTraceAttributeManager(config TraceConfig) (*TraceAttributeManager, error) {
	logger, err := zap.NewProduction(zap.Fields(zap.String("component", "trace_attribute_manager")))
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		logger.Error("MongoDB connect failed", zap.Error(err))
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	tracer := trace.NewNoopTracerProvider().Tracer("zero0x/traces")
	return &TraceAttributeManager{
		client:   client,
		db:       client.Database("zero0x_db"),
		logger:   logger,
		tracer:   tracer,
		config:   config,
		versions: make(map[string]map[string]string),
	}, nil
}

// Initialize sets up schema mappings and indexes.
func (m *TraceAttributeManager) Initialize(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load schema mappings for supported versions
	for _, version := range m.config.SupportedVersions {
		m.versions[version] = m.loadSchemaMapping(version)
	}

	// Ensure trace collection index
	coll := m.db.Collection("traces")
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"attributes.trade_id": 1, "_time": -1},
		Options: options.Index().SetSparse(true).SetBackground(true),
	})
	if err != nil {
		m.logger.Error("Index creation failed", zap.Error(err))
		return fmt.Errorf("create index: %w", err)
	}

	m.logger.Info("Trace manager initialized", zap.Strings("versions", m.config.SupportedVersions))
	return nil
}

// loadSchemaMapping simulates fetching OTel schema changes (e.g., from changelog).
func (m *TraceAttributeManager) loadSchemaMapping(version string) map[string]string {
	// Example mappings for trade-related attributes
	switch version {
	case "1.32":
		return map[string]string{
			"attributes.custom.trade_type": "attributes.trade.type",
			"attributes.custom.chain_id":   "attributes.chain.id",
		}
	default:
		return map[string]string{}
	}
}

// ProcessTrace processes and stores trace with version-specific attributes.
func (m *TraceAttributeManager) ProcessTrace(ctx context.Context, traceData map[string]interface{}, semconvVersion string) error {
	_, span := m.tracer.Start(ctx, "ProcessTrace")
	defer span.End()

	if !m.isSupportedVersion(semconvVersion) {
		semconvVersion = m.config.DefaultVersion
		m.logger.Warn("Unsupported version, using default", zap.String("version", semconvVersion))
	}

	// Validate and remap attributes
	remapped, err := m.remapAttributes(traceData, semconvVersion)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("remap attributes: %w", err)
	}

	coll := m.db.Collection("traces")
	remapped["_time"] = time.Now()
	remapped["_sysTime"] = time.Now()

	_, err = coll.InsertOne(ctx, remapped)
	if err != nil {
		span.RecordError(err)
		m.logger.Error("Insert trace failed", zap.Error(err))
		return fmt.Errorf("insert trace: %w", err)
	}

	m.logger.Info("Trace processed", zap.String("trade_id", fmt.Sprint(remapped["attributes.trade_id"])))
	span.AddEvent("TraceProcessed", trace.WithAttributes(
		attribute.String("semconvVersion", semconvVersion),
	))
	return nil
}

// remapAttributes applies version-specific attribute mappings.
func (m *TraceAttributeManager) remapAttributes(data map[string]interface{}, version string) (map[string]interface{}, error) {
	mappings, exists := m.versions[version]
	if !exists {
		return data, nil
	}

	remapped := make(map[string]interface{})
	for k, v := range data {
		if newKey, ok := mappings[k]; ok {
			remapped[newKey] = v
		} else {
			remapped[k] = v
		}
	}

	hash := sha256.Sum256([]byte(fmt.Sprint(remapped)))
	m.cache.Store(hex.EncodeToString(hash[:]), remapped)
	return remapped, nil
}

// isSupportedVersion checks if a version is supported.
func (m *TraceAttributeManager) isSupportedVersion(version string) bool {
	for _, v := range m.config.SupportedVersions {
		if v == version {
			return true
		}
	}
	return false
}

// MigrateToVersion migrates traces to a new semantic conventions version.
func (m *TraceAttributeManager) MigrateToVersion(ctx context.Context, fromVersion, toVersion string) (int, error) {
	if !m.isSupportedVersion(toVersion) {
		return 0, fmt.Errorf("target version %s not supported", toVersion)
	}

	coll := m.db.Collection("traces")
	cursor, err := coll.Find(ctx, bson.M{"attributes.semconv_version": fromVersion})
	if err != nil {
		return 0, fmt.Errorf("find traces: %w", err)
	}
	defer cursor.Close(ctx)

	count := 0
	g, ctx := errgroup.WithContext(ctx)
	for cursor.Next(ctx) {
		var traceData map[string]interface{}
		if err := cursor.Decode(&traceData); err != nil {
			return count, fmt.Errorf("decode trace: %w", err)
		}

		g.Go(func() error {
			remapped, err := m.remapAttributes(traceData, toVersion)
			if err != nil {
				return err
			}
			remapped["attributes.semconv_version"] = toVersion
			_, err = coll.UpdateOne(ctx, bson.M{"_id": traceData["_id"]}, bson.M{"$set": remapped})
			if err != nil {
				return fmt.Errorf("update trace: %w", err)
			}
			return nil
		})
		count++
	}

	if err := g.Wait(); err != nil {
		m.logger.Error("Migration failed", zap.Error(err))
		return count, fmt.Errorf("migration: %w", err)
	}

	m.logger.Info("Migration completed", zap.String("from", fromVersion), zap.String("to", toVersion), zap.Int("count", count))
	return count, nil
}

// QueryWithLocationFallback queries traces with fallback to old/new attribute locations.
func (m *TraceAttributeManager) QueryWithLocationFallback(ctx context.Context, query map[string]interface{}, version string) ([]map[string]interface{}, error) {
	coll := m.db.Collection("traces")
	results := []map[string]interface{}{}

	// Try new location
	newQuery := m.remapQuery(query, version)
	cursor, err := coll.Find(ctx, newQuery)
	if err != nil {
		return nil, fmt.Errorf("query new location: %w", err)
	}
	for cursor.Next(ctx) {
		var result map[string]interface{}
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode result: %w", err)
		}
		results = append(results, result)
	}
	cursor.Close(ctx)

	// Fallback to old location if needed
	if len(results) == 0 {
		oldQuery := m.remapQueryToOld(query, version)
		cursor, err := coll.Find(ctx, oldQuery)
		if err != nil {
			return nil, fmt.Errorf("query old location: %w", err)
		}
		for cursor.Next(ctx) {
			var result map[string]interface{}
			if err := cursor.Decode(&result); err != nil {
				return nil, fmt.Errorf("decode fallback: %w", err)
			}
			results = append(results, result)
		}
		cursor.Close(ctx)
	}

	m.logger.Info("Query executed", zap.String("version", version), zap.Int("results", len(results)))
	return results, nil
}

// remapQuery adjusts query for new attribute locations.
func (m *TraceAttributeManager) remapQuery(query map[string]interface{}, version string) map[string]interface{} {
	mappings, exists := m.versions[version]
	if !exists {
		return query
	}

	remapped := make(map[string]interface{})
	for k, v := range query {
		if newKey, ok := mappings[k]; ok {
			remapped[newKey] = v
		} else {
			remapped[k] = v
		}
	}
	return remapped
}

// remapQueryToOld adjusts query for old attribute locations.
func (m *TraceAttributeManager) remapQueryToOld(query map[string]interface{}, version string) map[string]interface{} {
	mappings, exists := m.versions[version]
	if !exists {
		return query
	}

	remapped := make(map[string]interface{})
	for k, v := range query {
		for oldKey, newKey := range mappings {
			if newKey == k {
				remapped[oldKey] = v
				break
			}
		}
		remapped[k] = v
	}
	return remapped
}

// Shutdown closes connections.
func (m *TraceAttributeManager) Shutdown() error {
	if err := m.client.Disconnect(context.Background()); err != nil {
		m.logger.Error("Shutdown failed", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}
	m.logger.Info("Trace manager shutdown")
	return m.logger.Sync()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	config := TraceConfig{
		MongoURI:          "mongodb://localhost:27017/zero0x_db",
		SupportedVersions: []string{"1.25", "1.32"},
		DefaultVersion:    "1.32",
		SemconvSchemaURL:  "https://opentelemetry.io/schemas/1.32",
		MaxRetries:        3,
		Timeout:           10 * time.Second,
	}

	manager, err := NewTraceAttributeManager(config)
	if err != nil {
		logger.Fatal("Manager init failed", zap.Error(err))
	}

	ctx := context.Background()
	if err := manager.Initialize(ctx); err != nil {
		logger.Fatal("Initialize failed", zap.Error(err))
	}

	// Example trace processing
	traceData := map[string]interface{}{
		"attributes.custom.trade_type": "arbitrage",
		"attributes.trade_id":          "trade_123",
		"attributes.semconv_version":   "1.25",
	}
	if err := manager.ProcessTrace(ctx, traceData, "1.25"); err != nil {
		logger.Error("Process trace failed", zap.Error(err))
	}

	// Migrate to new version
	count, err := manager.MigrateToVersion(ctx, "1.25", "1.32")
	if err != nil {
		logger.Error("Migration failed", zap.Error(err))
	} else {
		logger.Info("Migrated traces", zap.Int("count", count))
	}

	// Query with fallback
	query := map[string]interface{}{"attributes.trade.type": "arbitrage"}
	results, err := manager.QueryWithLocationFallback(ctx, query, "1.32")
	if err != nil {
		logger.Error("Query failed", zap.Error(err))
	} else {
		logger.Info("Query results", zap.Int("count", len(results)))
	}

	if err := manager.Shutdown(); err != nil {
		logger.Fatal("Shutdown failed", zap.Error(err))
	}
}
