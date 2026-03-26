package memoryaf

import (
	"context"
	"net/http"
	"sync"
	"time"

	libtermite "github.com/antflydb/antfly/lib/termite"
	termiteclient "github.com/antflydb/termite/pkg/client"
	"go.uber.org/zap"
)

const (
	defaultNERModel        = "fastino/gliner2-base-v1"
	availabilityCacheTTL   = 60 * time.Second
	availabilityCheckTimeout = 2 * time.Second
)

var defaultNERLabels = []string{
	"person", "organization", "project", "technology",
	"service", "tool", "framework", "pattern",
}

// NERClient wraps the Termite SDK client for named entity recognition
// with availability caching and graceful degradation.
type NERClient struct {
	client    *termiteclient.TermiteClient
	nerModel  string
	nerLabels []string
	logger    *zap.Logger

	mu        sync.Mutex
	available *bool
	checkedAt time.Time
}

// NewNERClient creates an NER client using the official Termite SDK.
func NewNERClient(termiteURL, nerModel string, nerLabels []string, logger *zap.Logger) (*NERClient, error) {
	tc, err := termiteclient.NewTermiteClient(termiteURL, &http.Client{
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &NERClient{
		client:    tc,
		nerModel:  nerModel,
		nerLabels: nerLabels,
		logger:    logger,
	}, nil
}

// DefaultNERClient creates an NER client with default settings,
// resolving the Termite URL via lib/termite.ResolveURL.
func DefaultNERClient(logger *zap.Logger) (*NERClient, error) {
	url := libtermite.ResolveURL("")
	if url == "" {
		url = "http://localhost:11433"
	}
	return NewNERClient(url, defaultNERModel, defaultNERLabels, logger)
}

func (c *NERClient) isAvailable(ctx context.Context) bool {
	c.mu.Lock()
	if c.available != nil && time.Since(c.checkedAt) < availabilityCacheTTL {
		avail := *c.available
		c.mu.Unlock()
		return avail
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, availabilityCheckTimeout)
	defer cancel()

	_, err := c.client.ListModels(ctx)
	avail := err == nil
	c.setAvailable(avail)
	return avail
}

func (c *NERClient) setAvailable(v bool) {
	c.mu.Lock()
	c.available = &v
	c.checkedAt = time.Now()
	c.mu.Unlock()
}

// RecognizeEntities calls Termite GLiNER2 to extract named entities.
// Returns empty slice if Termite is unavailable (graceful degradation).
func (c *NERClient) RecognizeEntities(ctx context.Context, text string) []Entity {
	if !c.isAvailable(ctx) {
		return nil
	}

	resp, err := c.client.Recognize(ctx, c.nerModel, []string{text}, c.nerLabels)
	if err != nil {
		c.logger.Warn("Termite NER request failed", zap.Error(err))
		return nil
	}

	if len(resp.Entities) == 0 || len(resp.Entities[0]) == 0 {
		return nil
	}

	entities := make([]Entity, 0, len(resp.Entities[0]))
	for _, e := range resp.Entities[0] {
		entities = append(entities, Entity{
			Text:  e.Text,
			Label: e.Label,
			Score: float64(e.Score),
		})
	}
	return entities
}
