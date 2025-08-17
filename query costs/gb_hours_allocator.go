package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// GBHoursConfig defines plan-based GB-hours limits.
type GBHoursConfig struct {
	MongoURI           string
	PlanGBHoursMonthly map[string]float64 // plan -> GB-hours
}

// GBHoursAllocator manages GB-hours allowances.
type GBHoursAllocator struct {
	client *mongo.Client
	db     *mongo.Database
	logger *zap.Logger
	config GBHoursConfig
}

// NewGBHoursAllocator initializes the allocator.
func NewGBHoursAllocator(config GBHoursConfig) (*GBHoursAllocator, error) {
	logger, err := zap.NewProduction(zap.Fields(zap.String("component", "gb_hours_allocator")))
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	return &GBHoursAllocator{client: client, db: client.Database("zero0x_db"), logger: logger, config: config}, nil
}

// AllocateGBHours checks and allocates GB-hours for a query.
func (a *GBHoursAllocator) AllocateGBHours(ctx context.Context, orgID, plan string, gbHours float64) (bool, error) {
	coll := a.db.Collection("gb_hours_usage")
	limit, ok := a.config.PlanGBHoursMonthly[plan]
	if !ok {
		return false, fmt.Errorf("unknown plan: %s", plan)
	}

	result := coll.FindOneAndUpdate(
		ctx,
		bson.M{"org_id": orgID, "month": time.Now().Format("2006-01")},
		bson.M{"$inc": bson.M{"gb_hours": gbHours}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)

	var doc struct{ GBHours float64 `bson:"gb_hours"` }
	if err := result.Decode(&doc); err != nil {
		return false, fmt.Errorf("decode usage: %w", err)
	}

	if doc.GBHours > limit {
		a.logger.Warn("GB-hours limit exceeded", zap.String("org_id", orgID), zap.Float64("usage", doc.GBHours))
		return false, nil
	}

	a.logger.Info("GB-hours allocated", zap.String("org_id", orgID), zap.Float64("gb_hours", gbHours))
	return true, nil
}

// Shutdown closes connections.
func (a *GBHoursAllocator) Shutdown() error {
	if err := a.client.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return a.logger.Sync()
}

func main() {
	config := GBHoursConfig{
		MongoURI: "mongodb://localhost:27017/zero0x_db",
		PlanGBHoursMonthly: map[string]float64{"basic": 100, "pro": 500},
	}
	allocator, err := NewGBHoursAllocator(config)
	if err != nil {
		zap.L().Fatal("Allocator init failed", zap.Error(err))
	}

	ctx := context.Background()
	// Example: 2-second query with 512MB memory
	allowed, err := allocator.AllocateGBHours(ctx, "org123", "basic", (512.0/1024)*(2.0/3600))
	if err != nil {
		zap.L().Error("Allocation failed", zap.Error(err))
	} else {
		zap.L().Info("Allocation result", zap.Bool("allowed", allowed))
	}

	allocator.Shutdown()
}
