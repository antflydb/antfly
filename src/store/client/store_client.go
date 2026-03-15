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

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"google.golang.org/protobuf/proto"
)

type StoreClient struct {
	id     types.ID
	client *http.Client
	url    string
}

func NewStoreClient(client *http.Client, id types.ID, url string) *StoreClient {
	return &StoreClient{
		id:     id,
		client: client,
		url:    url,
	}
}

func (sc *StoreClient) ID() types.ID {
	return sc.id
}

// doRequest executes an HTTP request and returns a ResponseError if the status code
// indicates failure. The caller must close resp.Body when done.
func (sc *StoreClient) doRequest(req *http.Request, successCode int) (*http.Response, error) {
	resp, err := sc.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	if resp.StatusCode != successCode {
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, &ResponseError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}
	return resp, nil
}

func (sc *StoreClient) Batch(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	syncLevel db.Op_SyncLevel,
) error {
	timestamp := storeutils.GetTimestampFromContext(ctx)
	batch := db.BatchOp_builder{
		Deletes:    deletes,
		Timestamp:  timestamp,
		Transforms: transforms,
		SyncLevel:  &syncLevel,
		Writes:     db.WritesFromTuples(writes),
	}

	body, err := proto.Marshal(batch.Build())
	if err != nil {
		return fmt.Errorf("error marshaling values: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/batch",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The server returns 204 No Content on full success, or 202 Accepted
	// when writes succeeded but full-text indexing is still queued
	// (ErrPartialSuccess). Both are success for the caller.
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &ResponseError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}
	return nil
}

func (sc *StoreClient) Backup(ctx context.Context, shardID types.ID, loc, id string) error {
	backupReq := common.BackupConfig{
		BackupID: id,
		Location: loc,
	}
	// Create the request
	url := sc.url + "/shard/backup"
	body, err := json.Marshal(backupReq)
	if err != nil {
		return fmt.Errorf("marshaling values: %w", err)
	}

	req, err := common.NewShardRequest(shardID, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	// Send the request
	resp, err := sc.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &ResponseError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}
	if after, ok := strings.CutPrefix(backupReq.Location, "file://"); ok {
		loc := after
		if err := os.MkdirAll(loc, os.ModePerm); err != nil && !os.IsExist(err) { //nolint:gosec // G301: standard permissions for data directory
			return fmt.Errorf("creating backup directory: %w", err)
		}
		// Create a file to save the streamed data
		filePath := path.Join(loc, common.ShardBackupFileName(backupReq.BackupID, shardID))
		file, err := os.Create(filepath.Clean(filePath))
		if err != nil {
			return fmt.Errorf("creating file: %w", err)
		}
		defer func() { _ = file.Close() }()

		// Copy the response body to the file in chunks
		buffer := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buffer)
			if err != nil && err != io.EOF {
				return fmt.Errorf("reading response body: %w", err)
			}
			if n == 0 {
				break // End of stream
			}
			_, err = file.Write(buffer[:n])
			if err != nil {
				return fmt.Errorf("writing to file: %w", err)
			}
		}
	}

	return nil
}

// lookupResultEntry matches the per-key JSON returned by POST /lookup.
type lookupResultEntry struct {
	Value   json.RawMessage `json:"value"`
	Version uint64          `json:"version"`
}

// Lookup performs a batch lookup for multiple keys on a shard.
// Returns a map from key to raw JSON document bytes. Missing keys are omitted.
func (sc *StoreClient) Lookup(ctx context.Context, shardID types.ID, keys []string) (map[string][]byte, error) {
	results, err := sc.batchLookup(ctx, shardID, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(results))
	for k, entry := range results {
		out[k] = entry.Value
	}
	return out, nil
}

// LookupWithVersion performs a single-key lookup and returns both the document bytes
// and the version timestamp for OCC transaction support.
func (sc *StoreClient) LookupWithVersion(ctx context.Context, shardID types.ID, key string) ([]byte, uint64, error) {
	results, err := sc.batchLookup(ctx, shardID, []string{key})
	if err != nil {
		return nil, 0, err
	}
	entry, ok := results[key]
	if !ok {
		return nil, 0, &ResponseError{StatusCode: 404, Body: "key not found"}
	}
	return entry.Value, entry.Version, nil
}

// batchLookup is the shared implementation for POST /lookup.
func (sc *StoreClient) batchLookup(ctx context.Context, shardID types.ID, keys []string) (map[string]lookupResultEntry, error) {
	reqBody, err := json.Marshal(keys)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}
	url := fmt.Sprintf("%s/lookup", sc.url)
	req, err := common.NewShardRequest(shardID, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	resp, err := sc.client.Do(req) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, &ResponseError{StatusCode: resp.StatusCode, Body: string(bodyBytes)}
	}
	var results map[string]lookupResultEntry
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return results, nil
}

func (sc *StoreClient) AddIndex(
	ctx context.Context,
	shardID types.ID,
	name string,
	config *indexes.IndexConfig,
) error {
	// Create the request
	url := sc.url + "/shard/index"
	config.Type = indexes.IndexTypeAknnV0
	if strings.HasPrefix(name, "full_text_index") {
		config.Type = indexes.IndexTypeFullTextV0
	}

	// Ensure the config carries the index name for the receiving handler.
	config.Name = name

	// Marshal the request to JSON
	b, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	hreq, err := common.NewShardRequest(shardID, http.MethodPost, url, bytes.NewBuffer(b))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) MergeRange(
	ctx context.Context,
	shardID types.ID,
	byteRange [2][]byte,
) error {
	req := &store.ShardSetRangeRequest{
		ByteRange: byteRange,
	}

	// Marshal the request to JSON
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	// Create the HTTP request
	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/setrange",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) UpdateSchema(
	ctx context.Context,
	shardID types.ID,
	tableSchema *schema.TableSchema,
) error {
	req := &store.ShardUpdateSchemaRequest{
		Schema: tableSchema,
	}

	// Marshal the request to JSON
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	// Create the HTTP request
	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/schema",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// PrepareSplit sets pendingSplitKey on the shard to reject writes in the split-off range.
// This is a fast, local operation that should be called before updating metadata.
func (sc *StoreClient) PrepareSplit(
	ctx context.Context,
	shardID types.ID,
	splitKey []byte,
) error {
	req := &store.ShardPrepareSplitRequest{
		SplitKey: splitKey,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/prepare-split",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) SplitShard(
	ctx context.Context,
	shardID types.ID,
	newShardID types.ID,
	splitKey []byte,
) error {
	if newShardID == 0 {
		return errors.New("new shard ID cannot be empty")
	}
	if shardID == newShardID {
		return errors.New("shard ID and new shard ID cannot be the same")
	}
	req := &store.ShardSplitRequest{
		NewShardID: newShardID.String(),
		SplitKey:   splitKey,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/split",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// ShardFinalizeSplitRequest is the request body for the finalize split endpoint
type ShardFinalizeSplitRequest struct {
	NewRangeEnd []byte `json:"new_range_end"`
}

func (sc *StoreClient) FinalizeSplit(
	ctx context.Context,
	shardID types.ID,
	newRangeEnd []byte,
) error {
	req := &ShardFinalizeSplitRequest{
		NewRangeEnd: newRangeEnd,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/finalize-split",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) RollbackSplit(
	ctx context.Context,
	shardID types.ID,
) error {
	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/shard/rollback-split",
		nil,
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) TransferLeadership(
	ctx context.Context,
	shardID types.ID,
	targetNodeID types.ID,
) error {
	url := fmt.Sprintf("%s/shard/transferleadership/%s", sc.url, targetNodeID.String())
	hreq, err := common.NewShardRequest(shardID, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusAccepted)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) AddPeer(
	ctx context.Context,
	shardID types.ID,
	newPeerID types.ID,
	newPeerURL string,
) error {
	req := &store.ShardAddPeerRequest{
		PeerURL: newPeerURL,
	}
	// Marshal the request to JSON
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	// Create the request URL
	url := fmt.Sprintf("%s/shard/peer/%d", sc.url, uint64(newPeerID))
	hreq, err := common.NewShardRequest(shardID, http.MethodPost, url, bytes.NewBuffer(b))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusNoContent)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) RemovePeer(
	ctx context.Context,
	shardID types.ID,
	peerToRemoveID types.ID,
	timestamp []byte,
) error {
	return sc.removePeer(ctx, shardID, peerToRemoveID, timestamp, false)
}

// RemovePeerSync removes a peer from a shard and waits for the conf change to be applied.
// This is synchronous - it blocks until the Raft conf change is committed and applied.
func (sc *StoreClient) RemovePeerSync(
	ctx context.Context,
	shardID types.ID,
	peerToRemoveID types.ID,
	timestamp []byte,
) error {
	return sc.removePeer(ctx, shardID, peerToRemoveID, timestamp, true)
}

func (sc *StoreClient) removePeer(
	ctx context.Context,
	shardID types.ID,
	peerToRemoveID types.ID,
	timestamp []byte,
	sync bool,
) error {
	req := &store.ShardRemovePeerRequest{
		Timestamp: timestamp,
	}
	// Marshal the request to JSON
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	url := fmt.Sprintf("%s/shard/peer/%d", sc.url, uint64(peerToRemoveID))
	if sync {
		url += "?sync=true"
	}
	hreq, err := common.NewShardRequest(shardID, http.MethodDelete, url, bytes.NewBuffer(b))
	if err != nil {
		return fmt.Errorf("creating delete request: %w", err)
	}
	hreq = hreq.WithContext(ctx)
	resp, err := sc.doRequest(hreq, http.StatusNoContent)
	if err != nil {
		return fmt.Errorf("removing peer from shard %s: %w", shardID, err)
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) StopShard(ctx context.Context, shardID types.ID) error {
	url := sc.url + "/shard"
	req, err := common.NewShardRequest(shardID, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// startShardOnNode attempts to start a shard on a node  and sends a restore command if a restore is needed.
// If the location is local (file://), it uploads the backup file via multipart.
// Otherwise, it sends the location, and the shard is expected to pull from there (e.g., S3).
func (sc *StoreClient) StartShard(
	ctx context.Context,
	shardID types.ID,
	shardStartReq *store.ShardStartRequest,
) (err error) {
	var reqBody io.Reader
	var contentType string

	if restoreConfig := shardStartReq.RestoreConfig; restoreConfig != nil {
		location := restoreConfig.Location
		if strings.HasPrefix(location, "file://") {
			backupID := restoreConfig.BackupID
			// Local file: stream as multipart
			localDir := strings.TrimPrefix(location, "file://")
			backupFileName := common.ShardBackupFileName(backupID, shardID)
			fullBackupFilePath := filepath.Join(localDir, backupFileName)

			// Use a pipe to avoid loading the whole file into memory
			pipeR, pipeW := io.Pipe()
			mpWriter := multipart.NewWriter(pipeW)
			contentType = mpWriter.FormDataContentType()

			// Start a goroutine to write multipart data to the pipe.
			// This goroutine will block until the HTTP client reads from pipeR.
			go func() {
				defer func() { _ = pipeW.Close() }() // Crucial: close pipeW when done to signal EOF to pipeR

				// Part 1: JSON payload
				payloadPart, err := mpWriter.CreateFormField("payload")
				if err != nil {
					// ln.logger.Error("Failed to create form field for payload", zap.Error(err))
					pipeW.CloseWithError(fmt.Errorf("creating payload form field: %w", err))
					return
				}
				if err := json.NewEncoder(payloadPart).Encode(shardStartReq); err != nil {
					// ln.logger.Error("Failed to encode restore payload", zap.Error(err))
					pipeW.CloseWithError(fmt.Errorf("encoding restore payload: %w", err))
					return
				}

				// Part 2: Backup file
				filePart, err := mpWriter.CreateFormFile("backup_file", backupFileName)
				if err != nil {
					// ln.logger.Error("Failed to create form file for backup", zap.String("filename", backupFileName), zap.Error(err))
					pipeW.CloseWithError(
						fmt.Errorf("creating backup form file %s: %w", backupFileName, err),
					)
					return
				}

				backupFile, err := os.Open(filepath.Clean(fullBackupFilePath))
				if err != nil {
					//  ln.logger.Error("Failed to open local backup file", zap.String("path", fullBackupFilePath), zap.Error(err))
					pipeW.CloseWithError(
						fmt.Errorf("opening local backup file %s: %w", fullBackupFilePath, err),
					)
					return
				}
				defer func() { _ = backupFile.Close() }()

				buf := make([]byte, 64*1024*1024) // 64MB buffer
				if _, err := io.CopyBuffer(filePart, backupFile, buf); err != nil {
					// ln.logger.Error("Failed to copy backup file to multipart writer", zap.String("path", fullBackupFilePath), zap.Error(err))
					pipeW.CloseWithError(
						fmt.Errorf(
							"copying backup file %s to multipart: %w",
							fullBackupFilePath,
							err,
						),
					)
					return
				}

				// Close multipart writer to finalize the form data
				if err := mpWriter.Close(); err != nil {
					// ln.logger.Error("Failed to close multipart writer", zap.Error(err))
					pipeW.CloseWithError(fmt.Errorf("closing multipart writer: %w", err))
					return
				}
			}()
			reqBody = pipeR
		} else {
			// Remote location: send JSON payload directly
			jsonPayloadBytes, err := json.Marshal(shardStartReq)
			if err != nil {
				return fmt.Errorf("marshalling restore payload for remote location: %w", err)
			}
			reqBody = bytes.NewBuffer(jsonPayloadBytes)
			contentType = "application/json"
		}
	} else {
		jsonPayloadBytes, err := json.Marshal(shardStartReq)
		if err != nil {
			return fmt.Errorf("marshalling restore payload for remote location: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonPayloadBytes)
		contentType = "application/json"
	}

	// ln.logger.Info("Starting shard on node", zap.String("node_id", nodeID.String()), zap.String("shard_id", shardID.String()))
	url := sc.url + "/shard"
	hreq, err := common.NewShardRequest(shardID, http.MethodPost, url, reqBody)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", contentType)
	hreq = hreq.WithContext(ctx)

	resp, err := sc.client.Do(hreq) //nolint:gosec // G704: HTTP client calling configured endpoint
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &ResponseError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	return nil
}

func (sc *StoreClient) DropIndex(ctx context.Context, shardID types.ID, name string) error {
	// Create the request
	url := sc.url + "/shard/index"

	b, err := json.Marshal(store.ShardDropIndexRequest{
		Name: name,
	})
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	req, err := common.NewShardRequest(shardID, http.MethodDelete, url, bytes.NewBuffer(b))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) Status(ctx context.Context) (*store.StoreStatus, error) {
	req, err := http.NewRequest(http.MethodGet, sc.url+"/status", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var storeStatus store.StoreStatus
	if err := json.NewDecoder(resp.Body).Decode(&storeStatus); err != nil {
		return nil, fmt.Errorf("decoding node status: %w", err)
	}
	return &storeStatus, nil
}

func (sc *StoreClient) MedianKeyForShard(ctx context.Context, shardID types.ID) ([]byte, error) {
	url := sc.url + "/shard/mediankey"
	req, err := common.NewShardRequest(shardID, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		MedianKey []byte `json:"median_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding median key response: %w", err)
	}
	return result.MedianKey, nil
}

func (sc *StoreClient) IsIDRemoved(
	ctx context.Context,
	shardID types.ID,
	nodeID types.ID,
) (bool, error) {
	url := fmt.Sprintf("%s/shard/isremoved/%s", sc.url, nodeID)
	req, err := common.NewShardRequest(shardID, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("creating request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	var r bool
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return false, fmt.Errorf("decoding response: %w", err)
	}
	return r, nil
}

// ScanOptions configures the Scan operation
type ScanOptions struct {
	// InclusiveFrom makes the lower bound inclusive instead of exclusive
	// Default: false (exclusive lower bound for pagination: (fromKey, toKey])
	InclusiveFrom bool
	// ExclusiveTo makes the upper bound exclusive instead of inclusive
	// Default: false (inclusive upper bound)
	ExclusiveTo bool
	// IncludeDocuments makes Scan return the actual documents in addition to hashes
	// Default: false (only return hashes)
	IncludeDocuments bool
	// FilterQuery is a Bleve query (as JSON) to filter documents. Only documents
	// matching this query are included in results. If nil, no filtering is applied.
	FilterQuery json.RawMessage
	// Limit is the maximum number of results to return. If 0, no limit is applied.
	Limit int
}

func (sc *StoreClient) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts ScanOptions,
) (*db.ScanResult, error) {
	req := &store.ScanRequest{
		FromKey:          fromKey,
		ToKey:            toKey,
		InclusiveFrom:    opts.InclusiveFrom,
		ExclusiveTo:      opts.ExclusiveTo,
		IncludeDocuments: opts.IncludeDocuments,
		FilterQuery:      opts.FilterQuery,
		Limit:            opts.Limit,
	}

	// Marshal the request to JSON
	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshalling request: %w", err)
	}

	// Create the HTTP request
	hreq, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/scan",
		bytes.NewBuffer(b),
	)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	resp, err := sc.doRequest(hreq, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var result db.ScanResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding scan response: %w", err)
	}
	return &result, nil
}

// Transaction RPC methods

func (sc *StoreClient) InitTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	participants [][]byte,
) error {
	initOp := db.InitTransactionOp_builder{
		TxnId:        txnID,
		Timestamp:    timestamp,
		Participants: participants,
	}.Build()

	body, err := proto.Marshal(initOp)
	if err != nil {
		return fmt.Errorf("marshaling init transaction op: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/init",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) WriteIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	timestamp uint64,
	coordinatorShard []byte,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	predicates []*db.VersionPredicate,
) error {
	// Build batch op
	batchBuilder := db.BatchOp_builder{
		Deletes:    deletes,
		Transforms: transforms,
		Writes:     db.WritesFromTuples(writes),
	}

	writeIntentOp := db.WriteIntentOp_builder{
		TxnId:            txnID,
		Timestamp:        timestamp,
		CoordinatorShard: coordinatorShard,
		Batch:            batchBuilder.Build(),
		Predicates:       predicates,
	}.Build()

	body, err := proto.Marshal(writeIntentOp)
	if err != nil {
		return fmt.Errorf("marshaling write intent op: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/write-intent",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) CommitTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
) error {
	commitOp := db.CommitTransactionOp_builder{
		TxnId: txnID,
	}.Build()

	body, err := proto.Marshal(commitOp)
	if err != nil {
		return fmt.Errorf("marshaling commit transaction op: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/commit",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) AbortTransaction(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
) error {
	abortOp := db.AbortTransactionOp_builder{
		TxnId: txnID,
	}.Build()

	body, err := proto.Marshal(abortOp)
	if err != nil {
		return fmt.Errorf("marshaling abort transaction op: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/abort",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) ResolveIntent(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	status int32,
) error {
	resolveOp := db.ResolveIntentsOp_builder{
		TxnId:  txnID,
		Status: status,
	}.Build()

	body, err := proto.Marshal(resolveOp)
	if err != nil {
		return fmt.Errorf("marshaling resolve intents op: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/resolve",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (sc *StoreClient) GetTransactionStatus(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
) (int32, error) {
	reqBody := map[string]any{
		"txn_id": txnID,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := common.NewShardRequest(
		shardID,
		http.MethodPost,
		sc.url+"/txn/status",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	var response map[string]int32
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("decoding response: %w", err)
	}

	return response["status"], nil
}

// GetEdges retrieves edges for a document in a graph index
func (sc *StoreClient) GetEdges(
	ctx context.Context,
	shardID types.ID,
	indexName string,
	key string,
	edgeType string,
	direction indexes.EdgeDirection,
) ([]indexes.Edge, error) {
	// Build query parameters
	query := fmt.Sprintf("?index=%s&key=%s", indexName, key)
	if edgeType != "" {
		query += fmt.Sprintf("&edge_type=%s", edgeType)
	}
	query += fmt.Sprintf("&direction=%s", direction)

	req, err := common.NewShardRequest(
		shardID,
		http.MethodGet,
		sc.url+"/graph/edges"+query,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req = req.WithContext(ctx)
	resp, err := sc.doRequest(req, http.StatusOK)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var response struct {
		Edges []indexes.Edge `json:"edges"`
		Count int            `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return response.Edges, nil
}
