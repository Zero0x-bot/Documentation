package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// StorageConfig defines storage optimization parameters.
type StorageConfig struct {
	MongoURI           string
	RetentionDays      int     // Default retention
	CompressionRatio   float64 // Expected compression savings
	CostPerGBPerMonth  float64 // USD
	TrimBatchSize      int
	VacuumInterval     time.Duration
}

// StorageCostOptimizer optimizes dataset storage costs.
type StorageCostOptimizer struct {
	client *mongo.Client
	db     *mongo.Database
	logger *zap.Logger
	config StorageConfig
}

// NewStorageCostOptimizer initializes the optimizer.
func NewStorageCostOptimizer(config StorageConfig) (*StorageCostOptimizer, error) {
	logger, err := zap.NewProduction(zap.Fields(zap.String("component", "storage_cost_optimizer")))
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		logger.Error("MongoDB connect failed", zap.Error(err))
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	return &StorageCostOptimizer{
		client: client,
		db:     client.Database("zero0x_db"),
		logger: logger,
		config: config,
	}, nil
}

// OptimizeStorage performs trimming and vacuuming to reduce costs.
func (o *StorageCostOptimizer) OptimizeStorage(ctx context.Context, datasetName string) (float64, error) {
	coll := o.db.Collection(datasetName)

	// Calculate trim date
	trimDate := time.Now().AddDate(0, 0, -o.config.RetentionDays)

	g, ctx := errgroup.WithContext(ctx)
	var savings float64

	g.Go(func() error {
		// Trim old data in batches
		for {
			result := coll.FindOneAndDelete(ctx, bson.M{"_time": bson.M{"$lt": trimDate}}, options.FindOneAndDelete().SetSort(bson.M{"_time": 1}))
			if result.Err() == mongo.ErrNoDocuments {
				break
			}
			if result.Err() != nil {
				return fmt.Errorf("trim error: %w", result.Err())
			}
		}
		o.logger.Info("Data trimmed", zap.String("dataset", datasetName), zap.Time("before", trimDate))
		return nil
	})

	g.Go(func() error {
		// Vacuum fields (rebuild schema)
		pipeline := []bson.M{
			{"$match": {"_time": bson.M{"$gte": trimDate}}},
			{"$project": {"fields": {"$objectToArray": "$$ROOT"}}},
			{"$unwind": "$fields"},
			{"$group": {"_id": "$fields.k"}},
		}
		cursor, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			return fmt.Errorf("vacuum aggregate: %w", err)
		}
		var fields []struct{ ID string `bson:"_id"` }
		if err := cursor.All(ctx, &fields); err != nil {
			return fmt.Errorf("decode fields: %w", err)
		}

		// Simulate cost savings from compression
		savings = float64(len(fields)) * o.config.CompressionRatio * o.config.CostPerGBPerMonth

		o.logger.Info("Fields vacuumed", zap.String("dataset", datasetName), zap.Int("fields", len(fields)))
		return nil
	})

	if err := g.Wait(); err != nil {
		o.logger.Error("Optimization failed", zap.Error(err))
		return 0, fmt.Errorf("optimize: %w", err)
	}

	o.logger.Info("Storage optimized", zap.String("dataset", datasetName), zap.Float64("savingsUSD", savings))
	return savings, nil
}

// ScheduleVacuum runs vacuum at intervals.
func (o *StorageCostOptimizer) ScheduleVacuum(ctx context.Context, datasetName string) {
	ticker := time.NewTicker(o.config.VacuumInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := o.OptimizeStorage(ctx, datasetName)
			if err != nil {
				o.logger.Error("Scheduled vacuum failed", zap.Error(err))
			}
		}
	}
}

// Shutdown closes connections.
func (o *StorageCostOptimizer) Shutdown() error {
	if err := o.client.Disconnect(context.Background()); err != nil {
		o.logger.Error("Shutdown failed", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}
	o.logger.Info("Storage optimizer shutdown")
	return o.logger.Sync()
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	config := StorageConfig{
		MongoURI:          "mongodb://localhost:27017/zero0x_db",
		RetentionDays:     30,
		CompressionRatio:  0.5,
		CostPerGBPerMonth: 0.02,
		TrimBatchSize:     1000,
		VacuumInterval:    24 * time.Hour,
	}

	optimizer, err := NewStorageCostOptimizer(config)
	if err != nil {
		logger.Fatal("Optimizer init failed", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := optimizer.Initialize(ctx); err != nil {
		logger.Fatal("Initialize failed", zap.Error(err))
	}

	go optimizer.ScheduleVacuum(ctx, "trades_dataset")

	savings, err := optimizer.OptimizeStorage(ctx, "trades_dataset")
	if err != nil {
		logger.Error("Optimize failed", zap.Error(err))
	} else {
		logger.Info("Savings", zap.Float64("usd", savings))
	}

	if err := optimizer.Shutdown(); err != nil {
		logger.Fatal("Shutdown failed", zap.Error(err))
	}
}
