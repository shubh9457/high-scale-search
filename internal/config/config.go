package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server       ServerConfig       `yaml:"server"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
	Redis        RedisConfig        `yaml:"redis"`
	ClickHouse   ClickHouseConfig   `yaml:"clickhouse"`
	Firestore    FirestoreConfig    `yaml:"firestore"`
	Kafka        KafkaConfig        `yaml:"kafka"`
	Search       SearchConfig       `yaml:"search"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type ServerConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type ElasticsearchConfig struct {
	Addresses       []string      `yaml:"addresses"`
	Username        string        `yaml:"username"`
	Password        string        `yaml:"password"`
	MaxRetries      int           `yaml:"max_retries"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
	IndexPrefix     string        `yaml:"index_prefix"`
	NumShards       int           `yaml:"num_shards"`
	NumReplicas     int           `yaml:"num_replicas"`
	RefreshInterval string        `yaml:"refresh_interval"`
	BulkSize        int           `yaml:"bulk_size"`
	BulkFlushInterval time.Duration `yaml:"bulk_flush_interval"`
}

type RedisConfig struct {
	Addresses    []string      `yaml:"addresses"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	MinIdleConns int           `yaml:"min_idle_conns"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	TTL          CacheTTLConfig `yaml:"ttl"`
}

type CacheTTLConfig struct {
	Autocomplete   time.Duration `yaml:"autocomplete"`
	Trending       time.Duration `yaml:"trending"`
	SearchResults  time.Duration `yaml:"search_results"`
	FacetCounts    time.Duration `yaml:"facet_counts"`
	UserRecent     time.Duration `yaml:"user_recent"`
	PopularQueries time.Duration `yaml:"popular_queries"`
	StaleFallback  time.Duration `yaml:"stale_fallback"`
}

type ClickHouseConfig struct {
	Addresses    []string      `yaml:"addresses"`
	Database     string        `yaml:"database"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	QueryTimeout time.Duration `yaml:"query_timeout"`
	MaxOpenConns int           `yaml:"max_open_conns"`
	MaxIdleConns int           `yaml:"max_idle_conns"`
}

type FirestoreConfig struct {
	ProjectID      string        `yaml:"project_id"`
	CredentialsFile string       `yaml:"credentials_file"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	MaxBatchSize   int           `yaml:"max_batch_size"`
}

type KafkaConfig struct {
	Brokers         []string      `yaml:"brokers"`
	TopicChanges    string        `yaml:"topic_changes"`
	TopicDLQ        string        `yaml:"topic_dlq"`
	ConsumerGroup   string        `yaml:"consumer_group"`
	NumPartitions   int           `yaml:"num_partitions"`
	ReplicationFactor int         `yaml:"replication_factor"`
	BatchSize       int           `yaml:"batch_size"`
	BatchTimeout    time.Duration `yaml:"batch_timeout"`
	MaxRetries      int           `yaml:"max_retries"`
}

type SearchConfig struct {
	DefaultPageSize int           `yaml:"default_page_size"`
	MaxPageSize     int           `yaml:"max_page_size"`
	QueryTimeout    time.Duration `yaml:"query_timeout"`
	CircuitBreaker  CircuitBreakerConfig `yaml:"circuit_breaker"`
	Retry           RetryConfig   `yaml:"retry"`
	SlowQuery       SlowQueryConfig `yaml:"slow_query"`
}

type CircuitBreakerConfig struct {
	MaxRequests     uint32        `yaml:"max_requests"`
	Interval        time.Duration `yaml:"interval"`
	Timeout         time.Duration `yaml:"timeout"`
	FailureThreshold uint32       `yaml:"failure_threshold"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"max_attempts"`
	InitialWait time.Duration `yaml:"initial_wait"`
	MaxWait     time.Duration `yaml:"max_wait"`
	Multiplier  float64       `yaml:"multiplier"`
}

type SlowQueryConfig struct {
	WarningThreshold  time.Duration `yaml:"warning_threshold"`
	CriticalThreshold time.Duration `yaml:"critical_threshold"`
}

type ObservabilityConfig struct {
	MetricsPort   int    `yaml:"metrics_port"`
	TracingEndpoint string `yaml:"tracing_endpoint"`
	LogLevel      string `yaml:"log_level"`
	ServiceName   string `yaml:"service_name"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	data = []byte(os.ExpandEnv(string(data)))

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
			ShutdownTimeout: 30 * time.Second,
		},
		Elasticsearch: ElasticsearchConfig{
			Addresses:       []string{"http://localhost:9200"},
			MaxRetries:      3,
			RequestTimeout:  150 * time.Millisecond,
			IndexPrefix:     "search",
			NumShards:       2,
			NumReplicas:     2,
			RefreshInterval: "1s",
			BulkSize:        5000,
			BulkFlushInterval: 5 * time.Second,
		},
		Redis: RedisConfig{
			Addresses:    []string{"localhost:6379"},
			PoolSize:     100,
			MinIdleConns: 10,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  1 * time.Second,
			WriteTimeout: 1 * time.Second,
			TTL: CacheTTLConfig{
				Autocomplete:   10 * time.Minute,
				Trending:       60 * time.Second,
				SearchResults:  2 * time.Minute,
				FacetCounts:    5 * time.Minute,
				UserRecent:     24 * time.Hour,
				PopularQueries: 5 * time.Minute,
				StaleFallback:  1 * time.Hour,
			},
		},
		ClickHouse: ClickHouseConfig{
			Addresses:    []string{"localhost:9000"},
			Database:     "search_analytics",
			DialTimeout:  5 * time.Second,
			QueryTimeout: 2 * time.Second,
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
		Firestore: FirestoreConfig{
			RequestTimeout: 2 * time.Second,
			MaxBatchSize:   100,
		},
		Kafka: KafkaConfig{
			Brokers:           []string{"localhost:9092"},
			TopicChanges:      "docs.changes",
			TopicDLQ:          "docs.changes.dlq",
			ConsumerGroup:     "search-indexer",
			NumPartitions:     12,
			ReplicationFactor: 3,
			BatchSize:         1000,
			BatchTimeout:      1 * time.Second,
			MaxRetries:        3,
		},
		Search: SearchConfig{
			DefaultPageSize: 20,
			MaxPageSize:     100,
			QueryTimeout:    200 * time.Millisecond,
			CircuitBreaker: CircuitBreakerConfig{
				MaxRequests:      100,
				Interval:         30 * time.Second,
				Timeout:          30 * time.Second,
				FailureThreshold: 5,
			},
			Retry: RetryConfig{
				MaxAttempts: 2,
				InitialWait: 50 * time.Millisecond,
				MaxWait:     500 * time.Millisecond,
				Multiplier:  2.0,
			},
			SlowQuery: SlowQueryConfig{
				WarningThreshold:  200 * time.Millisecond,
				CriticalThreshold: 500 * time.Millisecond,
			},
		},
		Observability: ObservabilityConfig{
			MetricsPort:   9090,
			LogLevel:      "info",
			ServiceName:   "search-orchestrator",
		},
	}
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}
	if len(c.Elasticsearch.Addresses) == 0 {
		return fmt.Errorf("at least one elasticsearch address required")
	}
	if len(c.Redis.Addresses) == 0 {
		return fmt.Errorf("at least one redis address required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one kafka broker required")
	}
	if c.Search.DefaultPageSize <= 0 {
		return fmt.Errorf("default page size must be positive")
	}
	if c.Search.MaxPageSize <= 0 || c.Search.MaxPageSize > 1000 {
		return fmt.Errorf("max page size must be between 1 and 1000")
	}
	return nil
}
