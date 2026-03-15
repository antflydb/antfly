// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// HTTPShardNotifier implements ShardNotifier using HTTP RPC calls through the metadata server
type HTTPShardNotifier struct {
	client *http.Client
	logger *zap.Logger
	// metadataURL is the base URL for the metadata server (e.g., "http://localhost:4000")
	// The metadata server will route the request to the correct storage node
	metadataURL string
}

// NewHTTPShardNotifier creates a new HTTP-based shard notifier
// The metadataURL should be the base URL of the metadata server (e.g., "http://localhost:4000")
func NewHTTPShardNotifier(client *http.Client, metadataURL string, logger *zap.Logger) *HTTPShardNotifier {
	return &HTTPShardNotifier{
		client:      client,
		metadataURL: metadataURL,
		logger:      logger,
	}
}

// NotifyResolveIntent sends a resolve intent RPC to a participant shard via the metadata server
func (n *HTTPShardNotifier) NotifyResolveIntent(
	ctx context.Context,
	shardIDBytes []byte,
	txnID []byte,
	status int32,
) error {
	// Convert shard ID bytes to types.ID
	if len(shardIDBytes) != 8 {
		return fmt.Errorf("invalid shard ID length: %d", len(shardIDBytes))
	}

	var shardID types.ID
	for i := range 8 {
		shardID |= types.ID(shardIDBytes[i]) << (i * 8)
	}

	// Build the ResolveIntentsOp
	resolveOp := db.ResolveIntentsOp_builder{
		TxnId:  txnID,
		Status: status,
	}.Build()

	body, err := proto.Marshal(resolveOp)
	if err != nil {
		return fmt.Errorf("marshaling resolve intents op: %w", err)
	}

	// Send request to metadata server's forwarding endpoint
	// The metadata server will look up which node hosts the shard and forward the request
	url := fmt.Sprintf("%s/_internal/v1/shard/%s/txn/resolve", n.metadataURL, shardID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := n.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response: %d %s", resp.StatusCode, string(bodyBytes))
	}

	n.logger.Debug("Successfully notified participant shard via metadata server",
		zap.Stringer("shardID", shardID),
		zap.Binary("txnID", txnID),
		zap.Int32("status", status))

	return nil
}
