package firestore

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type Client struct {
	client  *firestore.Client
	cfg     config.FirestoreConfig
	logger  *zap.Logger
}

func NewClient(ctx context.Context, cfg config.FirestoreConfig, logger *zap.Logger) (*Client, error) {
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	client, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating firestore client: %w", err)
	}

	logger.Info("firestore client connected", zap.String("project", cfg.ProjectID))

	return &Client{
		client: client,
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (c *Client) GetDocument(ctx context.Context, collection, docID string) (map[string]any, error) {
	ctx, span := observability.StartSpan(ctx, "firestore.get_doc",
		attribute.String("collection", collection),
		attribute.String("doc_id", docID),
	)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()

	doc, err := c.client.Collection(collection).Doc(docID).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("firestore get doc %s/%s: %w", collection, docID, err)
	}

	return doc.Data(), nil
}

func (c *Client) GetMulti(ctx context.Context, collection string, docIDs []string) (map[string]map[string]any, error) {
	ctx, span := observability.StartSpan(ctx, "firestore.get_multi",
		attribute.String("collection", collection),
		attribute.Int("count", len(docIDs)),
	)
	defer span.End()

	result := make(map[string]map[string]any, len(docIDs))

	batchSize := c.cfg.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for i := 0; i < len(docIDs); i += batchSize {
		end := i + batchSize
		if end > len(docIDs) {
			end = len(docIDs)
		}
		batch := docIDs[i:end]

		// Each batch gets its own timeout so sequential batches don't starve.
		batchCtx, batchCancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)

		refs := make([]*firestore.DocumentRef, len(batch))
		for j, id := range batch {
			refs[j] = c.client.Collection(collection).Doc(id)
		}

		docs, err := c.client.GetAll(batchCtx, refs)
		batchCancel()
		if err != nil {
			return nil, fmt.Errorf("firestore get_all batch %d: %w", i/batchSize, err)
		}

		for _, doc := range docs {
			if doc.Exists() {
				result[doc.Ref.ID] = doc.Data()
			}
		}
	}

	return result, nil
}

func (c *Client) HydrateResults(ctx context.Context, results []models.SearchResult, collection string) ([]models.SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}

	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}

	docs, err := c.GetMulti(ctx, collection, ids)
	if err != nil {
		c.logger.Warn("hydration failed, returning unhydrated results", zap.Error(err))
		return results, nil
	}

	for i, r := range results {
		if doc, ok := docs[r.ID]; ok {
			if results[i].Fields == nil {
				results[i].Fields = make(map[string]any)
			}
			for k, v := range doc {
				results[i].Fields[k] = v
			}
		}
	}

	return results, nil
}

type ChangeListener struct {
	client     *firestore.Client
	collection string
	logger     *zap.Logger
	handler    func(context.Context, *models.ChangeEvent) error
}

func (c *Client) NewChangeListener(collection string, handler func(context.Context, *models.ChangeEvent) error) *ChangeListener {
	return &ChangeListener{
		client:     c.client,
		collection: collection,
		logger:     c.logger,
		handler:    handler,
	}
}

func (cl *ChangeListener) Listen(ctx context.Context) error {
	snapIter := cl.client.Collection(cl.collection).Snapshots(ctx)
	defer snapIter.Stop()

	for {
		snap, err := snapIter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cl.logger.Error("snapshot iterator error", zap.Error(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		for _, change := range snap.Changes {
			var eventType string
			switch change.Kind {
			case firestore.DocumentAdded:
				eventType = "CREATE"
			case firestore.DocumentModified:
				eventType = "UPDATE"
			case firestore.DocumentRemoved:
				eventType = "DELETE"
			}

			event := &models.ChangeEvent{
				Type:       eventType,
				DocumentID: change.Doc.Ref.ID,
				Collection: cl.collection,
				Document:   change.Doc.Data(),
				Timestamp:  time.Now().UTC(),
			}

			if err := cl.handler(ctx, event); err != nil {
				cl.logger.Error("change event handler error",
					zap.String("doc_id", event.DocumentID),
					zap.String("type", eventType),
					zap.Error(err),
				)
			}
		}
	}
}

func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	iter := c.client.Collection("_health_check").Limit(1).Documents(ctx)
	defer iter.Stop()

	_, err := iter.Next()
	// iterator.Done means the collection is empty — Firestore is reachable.
	if err != nil && err != iterator.Done {
		return fmt.Errorf("firestore health check: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	return c.client.Close()
}
