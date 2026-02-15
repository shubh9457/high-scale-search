package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/api"
	"github.com/shubhsaxena/high-scale-search/internal/cache"
	"github.com/shubhsaxena/high-scale-search/internal/clickhouse"
	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/elasticsearch"
	"github.com/shubhsaxena/high-scale-search/internal/firestore"
	"github.com/shubhsaxena/high-scale-search/internal/indexing"
	"github.com/shubhsaxena/high-scale-search/internal/kafka"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
	"github.com/shubhsaxena/high-scale-search/internal/orchestrator"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	// Load config
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Initialize logger
	logger, err := observability.NewLogger(cfg.Observability.LogLevel)
	if err != nil {
		return fmt.Errorf("creating logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("starting search service",
		zap.String("service", cfg.Observability.ServiceName),
	)

	// Initialize tracing
	tracerShutdown, err := observability.InitTracer(cfg.Observability.ServiceName)
	if err != nil {
		logger.Warn("tracing initialization failed, continuing without tracing", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize clients
	redisCache, err := cache.NewRedisCache(cfg.Redis, logger)
	if err != nil {
		return fmt.Errorf("initializing redis: %w", err)
	}
	defer redisCache.Close()
	logger.Info("redis cache initialized")

	esClient, err := elasticsearch.NewClient(cfg.Elasticsearch, cfg.Search, logger)
	if err != nil {
		return fmt.Errorf("initializing elasticsearch: %w", err)
	}
	defer esClient.Close()
	logger.Info("elasticsearch client initialized")

	var chClient *clickhouse.Client
	chClient, err = clickhouse.NewClient(cfg.ClickHouse, logger)
	if err != nil {
		logger.Warn("clickhouse initialization failed, analytics will be unavailable", zap.Error(err))
	} else {
		defer chClient.Close()
		if err := chClient.EnsureTables(ctx); err != nil {
			logger.Warn("clickhouse table creation failed", zap.Error(err))
		}
		logger.Info("clickhouse client initialized")
	}

	var fsClient *firestore.Client
	if cfg.Firestore.ProjectID != "" {
		fsClient, err = firestore.NewClient(ctx, cfg.Firestore, logger)
		if err != nil {
			logger.Warn("firestore initialization failed, hydration will be unavailable", zap.Error(err))
		} else {
			defer fsClient.Close()
			logger.Info("firestore client initialized")
		}
	}

	// Initialize slow query detector
	var analyticsWriter observability.AnalyticsWriter
	if chClient != nil {
		analyticsWriter = chClient
	}
	slowQueryDetector := observability.NewSlowQueryDetector(
		cfg.Search.SlowQuery.WarningThreshold,
		cfg.Search.SlowQuery.CriticalThreshold,
		logger,
		analyticsWriter,
	)

	// Initialize search orchestrator
	orch := orchestrator.New(
		esClient, chClient, fsClient, redisCache,
		slowQueryDetector, cfg.Search, cfg.Elasticsearch, logger,
	)

	// Initialize indexing pipeline
	streamProcessor := indexing.NewStreamProcessor(
		esClient, chClient, redisCache, cfg.Elasticsearch, logger,
	)
	defer streamProcessor.Stop()

	consumer := kafka.NewConsumer(cfg.Kafka, streamProcessor.HandleEvent, logger)
	if err := consumer.Start(ctx); err != nil {
		logger.Warn("kafka consumer start failed, indexing pipeline will be unavailable", zap.Error(err))
	} else {
		defer consumer.Stop()
		logger.Info("kafka consumer started")
	}

	// Initialize HTTP server
	handler := api.NewHandler(orch, redisCache, logger)

	healthHandler := api.NewHealthHandler(logger)
	healthHandler.Register("redis", redisCache)
	healthHandler.RegisterES(esClient)
	if chClient != nil {
		healthHandler.Register("clickhouse", chClient)
	}
	healthHandler.Register("kafka", consumer)

	router := api.NewRouter(handler, healthHandler, logger)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server starting", zap.String("addr", addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		return err
	}

	// Graceful shutdown
	logger.Info("starting graceful shutdown", zap.Duration("timeout", cfg.Server.ShutdownTimeout))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	// Stop accepting new requests
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", zap.Error(err))
	}

	// Cancel background operations
	cancel()

	// Shutdown tracing
	if tracerShutdown != nil {
		if err := tracerShutdown(shutdownCtx); err != nil {
			logger.Error("tracer shutdown error", zap.Error(err))
		}
	}

	logger.Info("shutdown complete")
	return nil
}
