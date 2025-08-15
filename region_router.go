package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// RegionConfig defines the structure for Zero0x region configurations.
type RegionConfig struct {
	Region        string        `json:"region" bson:"region"`
	BaseDomain    string        `json:"baseDomain" bson:"baseDomain"`
	ApiBase       string        `json:"apiBase" bson:"apiBase"`
	HmacKey       []byte        `json:"-" bson:"hmacKey"`
	Timeout       time.Duration `json:"timeout" bson:"timeout"`
	MaxRetries    int           `json:"maxRetries" bson:"maxRetries"`
	DatasetPrefix string        `json:"datasetPrefix" bson:"datasetPrefix"`
}

// Zero0xRegionRouter manages multi-region dataset operations.
type Zero0xRegionRouter struct {
	client        *mongo.Client
	db            *mongo.Database
	logger        *zap.Logger
	configs       map[string]RegionConfig
	cacheColl     *mongo.Collection
	httpClient    *http.Client
	token         string
}

// NewZero0xRegionRouter initializes the router with MongoDB and logger.
func NewZero0xRegionRouter(mongoURI, dbName string) (*Zero0xRegionRouter, error) {
	logger, err := zap.NewProduction(zap.Fields(
		zap.String("component", "region_router"),
	))
	if err != nil {
		return nil, fmt.Errorf("failed to init logger: %w", err)
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		logger.Error("MongoDB connection failed", zap.Error(err))
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	db := client.Database(dbName)
	cacheColl := db.Collection("region_cache")

	configs := map[string]RegionConfig{
		"US": {
			Region:        "US",
			BaseDomain:    "https://app.zero0x.app",
			ApiBase:       "https://api.zero0x.app",
			HmacKey:       []byte("us-secret-key-2025"), // In prod, use env vars
			Timeout:       10 * time.Second,
			MaxRetries:    3,
			DatasetPrefix: "us_",
		},
		"EU": {
			Region:        "EU",
			BaseDomain:    "https://app.eu.zero0x.app",
			ApiBase:       "https://api.eu.zero0x.app",
			HmacKey:       []byte("eu-secret-key-2025"), // In prod, use env vars
			Timeout:       10 * time.Second,
			MaxRetries:    3,
			DatasetPrefix: "eu_",
		},
	}

	return &Zero0xRegionRouter{
		client:     client,
		db:         db,
		logger:     logger,
		configs:    configs,
		cacheColl:  cacheColl,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		token:      "", // Set via auth method
	}, nil
}

// Authenticate generates a region-specific HMAC token for API calls.
func (r *Zero0xRegionRouter) Authenticate(region, orgID string) error {
	config, exists := r.configs[region]
	if !exists {
		return fmt.Errorf("invalid region: %s", region)
	}

	mac := hmac.New(sha256.New, config.HmacKey)
	mac.Write([]byte(orgID + time.Now().Format(time.RFC3339)))
	r.token = hex.EncodeToString(mac.Sum(nil))

	r.logger.Info("Generated HMAC token", zap.String("region", region), zap.String("orgID", orgID))
	return nil
}

// IngestDataset sends data to the region-specific API endpoint with retries.
func (r *Zero0xRegionRouter) IngestDataset(ctx context.Context, region, datasetName string, data []map[string]interface{}) error {
	config, exists := r.configs[region]
	if !exists {
		return fmt.Errorf("invalid region: %s", region)
	}

	url := fmt.Sprintf("%s/v1/datasets/%s%s/ingest", config.ApiBase, config.DatasetPrefix, datasetName)
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+r.token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request: %w", err)
			r.logger.Warn("Request failed, retrying", zap.Int("attempt", attempt), zap.Error(err))
			time.Sleep(time.Duration(attempt*attempt) * 100 * time.Millisecond) // Exponential backoff
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			r.logger.Info("Dataset ingested", zap.String("region", region), zap.String("dataset", datasetName), zap.Int("events", len(data)))
			return r.cacheRegionMetadata(ctx, region, datasetName)
		}
		body, _ := io.ReadAll(resp.Body)
		lastErr = fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return fmt.Errorf("ingest failed after %d retries: %w", config.MaxRetries, lastErr)
}

// QueryDataset retrieves data from a region-specific dataset with caching.
func (r *Zero0xRegionRouter) QueryDataset(ctx context.Context, region, datasetName, query string) ([]map[string]interface{}, error) {
	config, exists := r.configs[region]
	if !exists {
		return nil, fmt.Errorf("invalid region: %s", region)
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%s:%s", region, datasetName, query)
	var cached []map[string]interface{}
	if err := r.cacheColl.FindOne(ctx, bson.M{"key": cacheKey}).Decode(&cached); err == nil {
		r.logger.Info("Cache hit", zap.String("key", cacheKey))
		return cached, nil
	}

	url := fmt.Sprintf("%s/v1/datasets/%s%s/query", config.ApiBase, config.DatasetPrefix, datasetName)
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("create query request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed: %s", string(body))
	}

	var results []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Cache results
	go r.cacheColl.InsertOne(context.Background(), bson.M{
		"key":      cacheKey,
		"results":  results,
		"_time":    time.Now(),
		"_sysTime": time.Now(),
	})

	r.logger.Info("Dataset queried", zap.String("region", region), zap.String("dataset", datasetName), zap.Int("results", len(results)))
	return results, nil
}

// CheckRegionStatus verifies region availability concurrently.
func (r *Zero0xRegionRouter) CheckRegionStatus(ctx context.Context) (map[string]bool, error) {
	g, ctx := errgroup.WithContext(ctx)
	status := make(map[string]bool, len(r.configs))

	for region, config := range r.configs {
		region := region // Capture loop variable
		config := config
		g.Go(func() error {
			url := fmt.Sprintf("%s/v1/status", config.ApiBase)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				return fmt.Errorf("create status request: %w", err)
			}

			resp, err := r.httpClient.Do(req)
			if err != nil {
				return fmt.Errorf("status request: %w", err)
			}
			defer resp.Body.Close()

			status[region] = resp.StatusCode == http.StatusOK
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		r.logger.Error("Region status check failed", zap.Error(err))
		return nil, fmt.Errorf("check status: %w", err)
	}

	return status, nil
}

// Cache region metadata in MongoDB
func (r *Zero0xRegionRouter) cacheRegionMetadata(ctx context.Context, region, datasetName string) error {
	config, exists := r.configs[region]
	if !exists {
		return fmt.Errorf("invalid region: %s", region)
	}

	_, err := r.cacheColl.UpdateOne(
		ctx,
		bson.M{"datasetName": datasetName, "region": region},
		bson.M{
			"$set": bson.M{
				"baseDomain": config.BaseDomain,
				"apiBase":    config.ApiBase,
				"_time":      time.Now(),
				"_sysTime":   time.Now(),
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		r.logger.Error("Cache metadata failed", zap.String("region", region), zap.String("dataset", datasetName), zap.Error(err))
		return fmt.Errorf("cache metadata: %w", err)
	}

	return nil
}

// Shutdown gracefully
func (r *Zero0xRegionRouter) Shutdown() error {
	if err := r.client.Disconnect(context.Background()); err != nil {
		r.logger.Error("MongoDB disconnect failed", zap.Error(err))
		return fmt.Errorf("shutdown: %w", err)
	}
	r.logger.Info("Region router shutdown")
	r.logger.Sync()
	return nil
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	router, err := NewZero0xRegionRouter("mongodb://localhost:27017/zero0x_db", "zero0x_db")
	if err != nil {
		logger.Fatal("Router init failed", zap.Error(err))
	}

	ctx := context.Background()

	// Authenticate for US region
	if err := router.Authenticate("US", "org_12345"); err != nil {
		logger.Fatal("Authentication failed", zap.Error(err))
	}

	// Ingest sample trade data
	data := []map[string]interface{}{
		{
			"chain":     "solana",
			"type":      "arbitrage",
			"amount":    100.5,
			"_time":     time.Now(),
			"_sysTime":  time.Now(),
		},
	}
	if err := router.IngestDataset(ctx, "US", "trades_dataset", data); err != nil {
		logger.Error("Ingest failed", zap.Error(err))
	}

	// Query dataset
	query := `{"match": {"chain": "solana"}}`
	results, err := router.QueryDataset(ctx, "US", "trades_dataset", query)
	if err != nil {
		logger.Error("Query failed", zap.Error(err))
	} else {
		logger.Info("Query results", zap.Int("count", len(results)))
	}

	// Check region status
	status, err := router.CheckRegionStatus(ctx)
	if err != nil {
		logger.Error("Status check failed", zap.Error(err))
	} else {
		logger.Info("Region status", zap.Any("status", status))
	}

	// Shutdown
	if err := router.Shutdown(); err != nil {
		logger.Fatal("Shutdown failed", zap.Error(err))
	}
}
