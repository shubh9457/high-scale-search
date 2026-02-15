package models

import "time"

type Intent int

const (
	IntentFullText Intent = iota
	IntentAnalytics
	IntentFaceted
	IntentAutocomplete
)

func (i Intent) String() string {
	switch i {
	case IntentFullText:
		return "fulltext"
	case IntentAnalytics:
		return "analytics"
	case IntentFaceted:
		return "faceted"
	case IntentAutocomplete:
		return "autocomplete"
	default:
		return "unknown"
	}
}

type SearchRequest struct {
	Query       string            `json:"query"`
	Filters     map[string]any    `json:"filters,omitempty"`
	Page        int               `json:"page"`
	PageSize    int               `json:"page_size"`
	Sort        string            `json:"sort,omitempty"`
	Region      string            `json:"region,omitempty"`
	UserID      string            `json:"user_id,omitempty"`
	ForceFresh  bool              `json:"force_fresh,omitempty"`
	Fields      []string          `json:"fields,omitempty"`
	UserContext *UserContext       `json:"user_context,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
}

type UserContext struct {
	UserID     string   `json:"user_id"`
	Region     string   `json:"region"`
	Locale     string   `json:"locale"`
	Preferences []string `json:"preferences,omitempty"`
}

type SearchResponse struct {
	Results    []SearchResult    `json:"results"`
	Total      int64             `json:"total"`
	Page       int               `json:"page"`
	PageSize   int               `json:"page_size"`
	TookMs     int64             `json:"took_ms"`
	Source     string            `json:"source"`
	Facets     map[string][]Facet `json:"facets,omitempty"`
	Metadata   ResponseMetadata  `json:"metadata"`
}

type SearchResult struct {
	ID              string         `json:"id"`
	Score           float64        `json:"score"`
	Title           string         `json:"title,omitempty"`
	Description     string         `json:"description,omitempty"`
	Category        string         `json:"category,omitempty"`
	Tags            []string       `json:"tags,omitempty"`
	Region          string         `json:"region,omitempty"`
	CreatedAt       time.Time      `json:"created_at,omitempty"`
	PopularityScore float64        `json:"popularity_score,omitempty"`
	Highlights      map[string][]string `json:"highlights,omitempty"`
	Fields          map[string]any `json:"fields,omitempty"`
}

type Facet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type ResponseMetadata struct {
	RequestID    string `json:"request_id"`
	Source       string `json:"source"`
	CacheHit     bool   `json:"cache_hit"`
	Stale        bool   `json:"stale"`
	Intent       string `json:"intent"`
	ShardsHit    int    `json:"shards_hit,omitempty"`
	TimedOut     bool   `json:"timed_out"`
	SpellCorrect string `json:"spell_correct,omitempty"`
}

type ParsedQuery struct {
	Original     string
	Normalized   string
	Tokens       []string
	SpellCorrected string
	HasWildcard  bool
	HasQuotes    bool
	IsPhrase     bool
	Fields       map[string]string
}

type ChangeEvent struct {
	Type       string         `json:"type"` // CREATE, UPDATE, DELETE
	DocumentID string         `json:"document_id"`
	Collection string         `json:"collection"`
	Document   map[string]any `json:"document,omitempty"`
	Region     string         `json:"region,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	Version    int64          `json:"version"`
}

type IndexAction struct {
	Action    string         `json:"action"` // index, delete
	Index     string         `json:"index"`
	ID        string         `json:"id"`
	Routing   string         `json:"routing,omitempty"`
	Body      map[string]any `json:"body,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

type AnalyticsEvent struct {
	EventType   string         `json:"event_type"`
	QueryHash   string         `json:"query_hash"`
	QueryType   string         `json:"query_type"`
	DurationMs  float64        `json:"duration_ms"`
	TotalHits   int64          `json:"total_hits"`
	ShardsHit   int            `json:"shards_hit"`
	TimedOut    bool           `json:"timed_out"`
	Timestamp   time.Time      `json:"timestamp"`
	TraceID     string         `json:"trace_id"`
	Source      string         `json:"source"`
	ExtraFields map[string]any `json:"extra_fields,omitempty"`
}
