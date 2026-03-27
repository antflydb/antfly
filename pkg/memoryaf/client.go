package memoryaf

import (
	"context"

	"github.com/antflydb/antfly/pkg/client"
)

// Client is the minimal Antfly client interface required by memoryaf.
// Callers provide an implementation backed by their Antfly connection
// (e.g. a direct HTTP client or a cluster-aware wrapper).
type Client interface {
	CreateTable(ctx context.Context, tableName string, config *client.CreateTableRequest) error
	Batch(ctx context.Context, tableID string, batchRequest client.BatchRequest) (*client.BatchResult, error)
	QueryWithBody(ctx context.Context, requestBody []byte) ([]byte, error)
}
