package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("expected host 0.0.0.0, got %s", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 10*time.Second {
		t.Errorf("expected read timeout 10s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 10*time.Second {
		t.Errorf("expected write timeout 10s, got %v", cfg.Server.WriteTimeout)
	}
	if cfg.Server.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected shutdown timeout 30s, got %v", cfg.Server.ShutdownTimeout)
	}
	if len(cfg.Elasticsearch.Addresses) != 1 || cfg.Elasticsearch.Addresses[0] != "http://localhost:9200" {
		t.Errorf("unexpected ES addresses: %v", cfg.Elasticsearch.Addresses)
	}
	if cfg.Elasticsearch.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", cfg.Elasticsearch.MaxRetries)
	}
	if cfg.Elasticsearch.IndexPrefix != "search" {
		t.Errorf("expected index prefix 'search', got %s", cfg.Elasticsearch.IndexPrefix)
	}
	if cfg.Elasticsearch.BulkSize != 5000 {
		t.Errorf("expected bulk size 5000, got %d", cfg.Elasticsearch.BulkSize)
	}
	if cfg.Redis.PoolSize != 100 {
		t.Errorf("expected pool size 100, got %d", cfg.Redis.PoolSize)
	}
	if cfg.Redis.TTL.SearchResults != 2*time.Minute {
		t.Errorf("expected search results TTL 2m, got %v", cfg.Redis.TTL.SearchResults)
	}
	if cfg.Redis.TTL.StaleFallback != 1*time.Hour {
		t.Errorf("expected stale fallback TTL 1h, got %v", cfg.Redis.TTL.StaleFallback)
	}
	if cfg.Search.DefaultPageSize != 20 {
		t.Errorf("expected default page size 20, got %d", cfg.Search.DefaultPageSize)
	}
	if cfg.Search.MaxPageSize != 100 {
		t.Errorf("expected max page size 100, got %d", cfg.Search.MaxPageSize)
	}
	if cfg.Search.QueryTimeout != 200*time.Millisecond {
		t.Errorf("expected query timeout 200ms, got %v", cfg.Search.QueryTimeout)
	}
	if cfg.Search.CircuitBreaker.FailureThreshold != 5 {
		t.Errorf("expected failure threshold 5, got %d", cfg.Search.CircuitBreaker.FailureThreshold)
	}
	if cfg.Search.Retry.MaxAttempts != 2 {
		t.Errorf("expected max attempts 2, got %d", cfg.Search.Retry.MaxAttempts)
	}
	if cfg.Search.Retry.Multiplier != 2.0 {
		t.Errorf("expected multiplier 2.0, got %f", cfg.Search.Retry.Multiplier)
	}
	if cfg.Observability.LogLevel != "info" {
		t.Errorf("expected log level 'info', got %s", cfg.Observability.LogLevel)
	}
	if cfg.Observability.ServiceName != "search-orchestrator" {
		t.Errorf("expected service name 'search-orchestrator', got %s", cfg.Observability.ServiceName)
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no error for default config, got %v", err)
	}
}

func TestValidate_InvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"zero port", 0},
		{"negative port", -1},
		{"port too high", 65536},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Server.Port = tt.port
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for port %d, got nil", tt.port)
			}
		})
	}
}

func TestValidate_EmptyESAddresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Elasticsearch.Addresses = nil
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty ES addresses")
	}
}

func TestValidate_EmptyRedisAddresses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Redis.Addresses = nil
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty Redis addresses")
	}
}

func TestValidate_EmptyKafkaBrokers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Kafka.Brokers = nil
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty Kafka brokers")
	}
}

func TestValidate_InvalidPageSize(t *testing.T) {
	tests := []struct {
		name        string
		defaultSize int
		maxSize     int
	}{
		{"zero default page size", 0, 100},
		{"negative default page size", -1, 100},
		{"zero max page size", 20, 0},
		{"negative max page size", 20, -1},
		{"max page size too large", 20, 1001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Search.DefaultPageSize = tt.defaultSize
			cfg.Search.MaxPageSize = tt.maxSize
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for default=%d, max=%d", tt.defaultSize, tt.maxSize)
			}
		})
	}
}

func TestValidate_ValidPortBoundaries(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"port 1", 1},
		{"port 8080", 8080},
		{"port 65535", 65535},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Server.Port = tt.port
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected no error for port %d, got %v", tt.port, err)
			}
		})
	}
}

func TestValidate_ValidMaxPageSize(t *testing.T) {
	tests := []struct {
		name    string
		maxSize int
	}{
		{"max 1", 1},
		{"max 500", 500},
		{"max 1000", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Search.MaxPageSize = tt.maxSize
			if err := cfg.Validate(); err != nil {
				t.Errorf("expected no error for max page size %d, got %v", tt.maxSize, err)
			}
		})
	}
}

func TestLoad_ValidFile(t *testing.T) {
	content := `
server:
  host: "127.0.0.1"
  port: 9090
elasticsearch:
  addresses:
    - "http://es:9200"
redis:
  addresses:
    - "redis:6379"
kafka:
  brokers:
    - "kafka:9092"
search:
  default_page_size: 10
  max_page_size: 50
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", cfg.Server.Host)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Search.DefaultPageSize != 10 {
		t.Errorf("expected default page size 10, got %d", cfg.Search.DefaultPageSize)
	}
	if cfg.Search.MaxPageSize != 50 {
		t.Errorf("expected max page size 50, got %d", cfg.Search.MaxPageSize)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("{{invalid yaml"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoad_InvalidConfig(t *testing.T) {
	content := `
server:
  port: 0
elasticsearch:
  addresses:
    - "http://es:9200"
redis:
  addresses:
    - "redis:6379"
kafka:
  brokers:
    - "kafka:9092"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Error("expected validation error")
	}
}

func TestLoad_EnvExpansion(t *testing.T) {
	t.Setenv("TEST_ES_HOST", "http://prod-es:9200")

	content := `
server:
  port: 8080
elasticsearch:
  addresses:
    - "$TEST_ES_HOST"
redis:
  addresses:
    - "redis:6379"
kafka:
  brokers:
    - "kafka:9092"
search:
  default_page_size: 20
  max_page_size: 100
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Elasticsearch.Addresses[0] != "http://prod-es:9200" {
		t.Errorf("expected expanded env var, got %s", cfg.Elasticsearch.Addresses[0])
	}
}

func TestLoad_DefaultsPreservedWhenNotOverridden(t *testing.T) {
	content := `
server:
  port: 8080
elasticsearch:
  addresses:
    - "http://es:9200"
redis:
  addresses:
    - "redis:6379"
kafka:
  brokers:
    - "kafka:9092"
search:
  default_page_size: 20
  max_page_size: 100
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	// Values not specified in YAML should keep defaults
	if cfg.Server.ReadTimeout != 10*time.Second {
		t.Errorf("expected default read timeout preserved, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Elasticsearch.BulkSize != 5000 {
		t.Errorf("expected default bulk size preserved, got %d", cfg.Elasticsearch.BulkSize)
	}
}
