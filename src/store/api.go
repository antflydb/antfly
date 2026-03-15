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
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/raft"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/minio/minio-go/v7"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

// API handler for a http based key-value store backed by raft
type StoreAPI struct {
	logger       *zap.Logger
	store        StoreIface
	antflyConfig *common.Config

	// Used by split/archive paths that still need request-level deduplication.
	shardStart singleflight.Group

	startMu        sync.Mutex
	startingShards map[types.ID]struct{}
}

func (store *Store) NewHttpAPI() http.Handler {
	api := &StoreAPI{
		logger:         store.logger,
		store:          store,
		antflyConfig:   store.antflyConfig,
		startingShards: make(map[types.ID]struct{}),
	}
	return api.setupRoutes()
}

func (h *StoreAPI) beginShardStart(shardID types.ID) error {
	h.startMu.Lock()
	defer h.startMu.Unlock()

	// Keep zero-value StoreAPI usable in tests or ad hoc construction.
	if h.startingShards == nil {
		h.startingShards = make(map[types.ID]struct{})
	}
	if _, ok := h.startingShards[shardID]; ok {
		return errors.New("shard already exists")
	}
	if _, ok := h.store.Shard(shardID); ok {
		return errors.New("shard already exists")
	}
	h.startingShards[shardID] = struct{}{}
	return nil
}

func (h *StoreAPI) finishShardStart(shardID types.ID) {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	delete(h.startingShards, shardID)
}

func downloadFromS3(
	ctx context.Context,
	logger *zap.Logger,
	bucketURL, objectNameKey, destPath string,
	s3Info *common.S3Info,
) error {
	bucketName, prefix, err := common.ParseS3URL(bucketURL)
	if err != nil {
		return fmt.Errorf("parsing bucket URL %s: %w", bucketURL, err)
	}

	// Construct full object key with optional prefix, matching the write path
	// in WriteBackupToBlobStore.
	fullObjectKey := objectNameKey
	if prefix != "" {
		fullObjectKey = path.Join(prefix, objectNameKey)
	}

	logger.Info("Downloading from S3",
		zap.String("bucket", bucketName),
		zap.String("object", fullObjectKey),
		zap.String("destination", destPath))

	// TODO: Enable multipart download with progress tracking using the SDK's DownloadObject
	// method (pkg/libaf/s3) when ready:
	//
	// opts := &s3.DownloadObjectOptions{
	// 	ProgressFn: func(partNumber, partSize, totalParts int) {
	// 		logger.Info("Downloading backup part",
	// 			zap.Int("partNumber", partNumber),
	// 			zap.Int("size", partSize),
	// 			zap.Int("total", totalParts),
	// 		)
	// 	},
	// }
	// if err := s3Info.GetS3Credentials().DownloadObject(ctx, bucketName, fullObjectKey, destPath, opts); err != nil {
	// 	return err
	// }

	// Simple download without progress tracking
	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client for endpoint %s: %w", s3Info.Endpoint, err)
	}
	if err := os.MkdirAll(filepath.Dir(destPath), os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return fmt.Errorf("creating directory %s: %w", filepath.Dir(destPath), err)
	}
	if err := minioClient.FGetObject(ctx, bucketName, fullObjectKey, destPath, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("downloading object %s from bucket %s: %w", fullObjectKey, bucketName, err)
	}

	logger.Info("Successfully downloaded from S3",
		zap.String("bucket", bucketName),
		zap.String("object", fullObjectKey),
		zap.String("destination", destPath))
	return nil
}

// getShardID extracts the shard ID from the request header
func (h *StoreAPI) getShardID(w http.ResponseWriter, r *http.Request) (types.ID, bool) {
	shardID, err := types.IDFromString(r.Header.Get("X-Raft-Shard-Id"))
	if err != nil {
		h.logger.Warn("Failed to get shard ID", zap.Error(err))
		http.Error(w, "Failed to get shard ID", http.StatusBadRequest)
		return types.ID(0), false
	}
	if shardID == types.ID(0) {
		http.Error(w, "Shard ID cannot be 0", http.StatusBadRequest)
		return types.ID(0), false
	}
	return shardID, true
}

// validateShard checks if the shard exists
func (h *StoreAPI) validateShard(w http.ResponseWriter, _ *http.Request, shardID types.ID) bool {
	if _, ok := h.store.Shard(shardID); !ok {
		http.Error(w, "Shard not found", http.StatusNotFound)
		return false
	}
	return true
}

func (h *StoreAPI) handleStopShard(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	h.store.StopRaftGroup(shardID)
	w.WriteHeader(http.StatusOK)
}

var ErrBadRequest = errors.New("bad request")

// handleStartShard handles requests to start a new raft shard
func (h *StoreAPI) handleStartShard(w http.ResponseWriter, r *http.Request) {
	var req ShardStartRequest
	initWithDBArchive := ""

	newShardID, ok := h.getShardID(w, r)
	if !ok {
		return
	}

	currentNodeID := h.store.ID()
	if currentNodeID == 0 { // Check if nodeID is valid (0 is often an invalid/uninitialized ID)
		h.logger.Error(
			"Critical: Store's nodeID is not set or invalid (0) for shard snapshot handling.",
			zap.Stringer("newShardID", newShardID),
		)
		http.Error(
			w,
			"Failed to determine current node ID; store nodeID is not set or invalid.",
			http.StatusInternalServerError,
		)
		return
	}
	if err := h.beginShardStart(newShardID); err != nil {
		http.Error(w, "Shard already exists", http.StatusBadRequest)
		return
	}
	releaseStartReservation := true
	defer func() {
		if releaseStartReservation {
			h.finishShardStart(newShardID)
		}
	}()

	h.logger.Info("Received request to start shard", zap.Stringer("shardID", newShardID))
	err := func() error {
		contentType := r.Header.Get("Content-Type")
		if strings.HasPrefix(contentType, "multipart/form-data") {

			mr, err := r.MultipartReader()
			if err != nil {
				return fmt.Errorf("%w: creating multipart reader: %v", ErrBadRequest, err)
			}
			var filePart *multipart.Part
			for {
				p, nextErr := mr.NextPart()
				if nextErr == io.EOF {
					break // End of form
				}
				if nextErr != nil {
					return fmt.Errorf(
						"%w: reading next part from multipart form: %v",
						ErrBadRequest,
						nextErr,
					)
				}
				// Check if it's a regular form field or a file.
				if p.FileName() == "" {
					// It's a form field. Read its value.
					formName := p.FormName()
					// Process the metadata fields
					if formName == "payload" {
						if err := json.NewDecoder(p).Decode(&req); err != nil {
							return fmt.Errorf(
								"%w: unmarshaling payload from multipart form: %v",
								ErrBadRequest,
								err,
							)
						}
					}
				} else {
					// It's a file. We don't read it yet.
					// Just store the part reference for later.
					if filePart != nil {
						// Handle case where more than one file is uploaded, if necessary
						_ = p.Close() // Close the part if we are not processing it
						continue
					}
					filePart = p
					defer func() { _ = filePart.Close() }()
					if req.RestoreConfig == nil {
						return fmt.Errorf("%w: RestoreConfig is required when uploading a backup file", ErrBadRequest)
					}
					if !strings.HasPrefix(filePart.FileName(), req.RestoreConfig.BackupID) {
						return fmt.Errorf("%w: uploaded file name %s does not match expected backup ID %s", ErrBadRequest, filePart.FileName(), req.RestoreConfig.BackupID)
					}

					// Here newShardID is shardIDForBackup and req.ShardConfig.RestoreConfig.BackupID is backupIDForLeader
					backupFileName := common.ShardBackupFileName(req.RestoreConfig.BackupID, newShardID)
					dataDir := h.antflyConfig.GetBaseDir()
					snapDir := common.SnapDir(dataDir, newShardID, currentNodeID)
					if err := os.MkdirAll(snapDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
						return fmt.Errorf("creating snapshot directory %s: %w", snapDir, err)
					}
					destPath := filepath.Join(snapDir, backupFileName)
					if _, err := os.Stat(destPath); err == nil { //nolint:gosec // G703: path from internal config
						// TODO (ajr) Send the expected checksum from the leader to verify integrity
						h.logger.Info("Backup file already exists, skipping save",
							zap.String("destPath", destPath),
							zap.Stringer("newShardID", newShardID))
					} else if !os.IsNotExist(err) {
						return fmt.Errorf("statting destination file %s: %w", destPath, err)
					} else {
						h.logger.Info("Saving uploaded backup file",
							zap.String("backupFileName", backupFileName),
							zap.String("destPath", destPath),
							zap.Stringer("newShardID", newShardID))
						outFile, err := os.Create(filepath.Clean(destPath) + ".tmp")
						if err != nil {
							return fmt.Errorf("creating destination file %s: %w", destPath, err)
						}
						// Use a 64MB buffer for copying
						buf := make([]byte, 64*1024*1024)
						if _, err := io.CopyBuffer(outFile, filePart, buf); err != nil {
							_ = outFile.Close()
							return fmt.Errorf("saving uploaded file to %s: %w", destPath, err)
						}
						// Ensure all data is written to disk before rename
						if err := outFile.Sync(); err != nil {
							return fmt.Errorf("syncing file %s: %w", destPath, err)
						}
						if err := outFile.Close(); err != nil {
							return fmt.Errorf("closing file %s: %w", destPath, err)
						}
						if err := os.Rename(outFile.Name(), filepath.Clean(destPath)); err != nil { //nolint:gosec // G703: internal path with traversal protection
							return fmt.Errorf("renaming temp file to final destination %s: %w", destPath, err)
						}
						h.logger.Info("Successfully saved uploaded backup",
							zap.String("backupFileName", backupFileName),
							zap.String("destPath", destPath),
							zap.Stringer("newShardID", newShardID))
					}

					initWithDBArchive = strings.TrimSuffix(backupFileName, ".tar.zst") // Use the base filename
					// If err is http.ErrMissingFile, it's fine, just means no file was uploaded.
				}
			}
			// Validation
			if req.RestoreConfig != nil && initWithDBArchive == "" {
				if strings.HasPrefix(req.RestoreConfig.Location, "file://") {
					return fmt.Errorf(
						"%w: file part missing from multipart restore request",
						ErrBadRequest,
					)
				}
			}
		} else { // Assume JSON body
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				h.logger.Warn("Failed to read JSON request body for shard",
					zap.Stringer("newShardID", newShardID),
					zap.Error(err))
				return fmt.Errorf("%w: reading request body: %v", ErrBadRequest, err)
			}
		}

		// Process RestoreConfig if present and archive not already set by multipart
		if req.RestoreConfig != nil && initWithDBArchive == "" {
			restoreConf := req.RestoreConfig
			// newShardID is the shardID for this node, restoreConf.BackupID is the ID given by the leader.
			backupFileName := common.ShardBackupFileName(restoreConf.BackupID, newShardID)
			dataDir := h.antflyConfig.GetBaseDir()
			snapDir := common.SnapDir(
				dataDir,
				newShardID,
				currentNodeID,
			) // Directory creation handled by downloadFromS3 or local copy

			if strings.HasPrefix(restoreConf.Location, "s3://") {
				destPath := filepath.Join(snapDir, backupFileName)
				// Don't cancel the download if the request context is canceled,
				// it could take a while.
				if err := downloadFromS3(context.Background(), h.logger, restoreConf.Location, backupFileName, destPath, &h.antflyConfig.Storage.S3); err != nil {
					h.logger.Error(
						"Failed to download backup from S3 for shard",
						zap.Stringer("newShardID", newShardID),
						zap.Error(err),
					)
					return fmt.Errorf(
						"downloading s3 backup (%s, object: %s): %w",
						restoreConf.Location,
						backupFileName,
						err,
					)
				}
				initWithDBArchive = strings.TrimSuffix(backupFileName, ".tar.zst")
			} else if after, ok0 := strings.CutPrefix(restoreConf.Location, "file://"); ok0 {
				// This case might occur if the leader specifies a local file path accessible to this node,
				// though typically for file transfers it would use multipart.
				localBasePath := after
				srcPath := filepath.Join(localBasePath, backupFileName)
				destPath := filepath.Join(snapDir, backupFileName)

				if _, err := os.Stat(srcPath); err == nil { //nolint:gosec // G703: path from internal config
					if err := os.MkdirAll(snapDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
						return fmt.Errorf("creating snapshot directory %s: %w", snapDir, err)
					}
					input, err := os.Open(filepath.Clean(srcPath))
					if err != nil {
						return fmt.Errorf("opening local backup file %s: %w", srcPath, err)
					}
					defer func() { _ = input.Close() }()
					output, err := os.Create(filepath.Clean(destPath))
					if err != nil {
						return fmt.Errorf("creating destination backup file %s: %w", destPath, err)
					}
					defer func() { _ = output.Close() }()
					buf := make([]byte, 64*1024*1024) // 64MB buffer
					if _, err := io.CopyBuffer(output, input, buf); err != nil {
						return fmt.Errorf("copying local backup file from %s to %s: %w", srcPath, destPath, err)
					}
					h.logger.Info("Successfully copied local backup",
						zap.String("backupFileName", backupFileName),
						zap.String("destPath", destPath),
						zap.Stringer("newShardID", newShardID))
					initWithDBArchive = strings.TrimSuffix(backupFileName, ".tar.zst")
				} else {
					h.logger.Warn("RestoreConfig for shard specified local file, but it was not found",
						zap.Stringer("newShardID", newShardID),
						zap.String("srcPath", srcPath))
					// Not necessarily a fatal error for the handler here, PebbleStorage will try to load this archive name.
				}
			}
		}

		if req.SplitStart && initWithDBArchive == "" {
			// If this is a split operation (signaled by SplitStart) and no specific restore archive
			// has been identified yet, use the conventional name for a split archive.
			// The leader signals SplitStart true for the *new* shard resulting from a split.
			h.logger.Info(
				"Shard is starting as part of a split without explicit archive, using default split archive name",
				zap.Stringer("newShardID", newShardID),
			)
			initWithDBArchive = common.SplitArchive(newShardID)
		}

		return nil
	}()
	if err != nil {
		h.logger.Error("Failed to start shard",
			zap.Stringer("newShardID", newShardID),
			zap.Error(err))
		if errors.Is(err, ErrBadRequest) {
			http.Error(w, fmt.Sprintf("Failed to start shard: %v", err), http.StatusBadRequest)
		} else {
			http.Error(w, fmt.Sprintf("Failed to start shard: %v", err), http.StatusInternalServerError)
		}
		return
	}
	// Ownership of the start reservation transfers to the async start goroutine
	// once request validation and archive setup succeed.
	releaseStartReservation = false
	go func() {
		defer h.finishShardStart(newShardID)
		err := h.store.StartRaftGroup(newShardID, req.Peers, req.Join, &ShardStartConfig{
			InitWithDBArchive: initWithDBArchive, // This will be the base name of the archive file in the snapDir
			ShardConfig:       req.ShardConfig,
			Timestamp:         req.Timestamp,
		})
		if err != nil {
			h.logger.Error("Failed to start shard",
				zap.Stringer("newShardID", newShardID),
				zap.Error(err),
			)
		}
	}()
	w.WriteHeader(http.StatusOK)
}

type MedianKeyResponse struct {
	MedianKey []byte `json:"median_key"`
}

// handleGetStoreStatus returns information about all shards on this node
func (h *StoreAPI) handleGetStoreStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(h.store.Status()); err != nil {
		h.logger.Error("Failed to marshal node state", zap.Error(err))
		http.Error(w, "Failed to marshal node state", http.StatusInternalServerError)
	}
}

// handleGetMedianKey handles requests to get the median key of a shard
func (h *StoreAPI) handleGetMedianKey(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	shard, _ := h.store.Shard(shardID)
	medianKey, err := shard.FindMedianKey()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to find median key: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := &MedianKeyResponse{
		MedianKey: medianKey,
	}
	h.logger.Info("Found median key", zap.ByteString("medianKey", medianKey))
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode median key response", zap.Error(err))
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// handlePrepareSplit prepares a shard for split by setting pendingSplitKey.
// This is a fast, local operation that should be called before updating metadata.
func (h *StoreAPI) handlePrepareSplit(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardPrepareSplitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read prepare split request body", zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)
	h.logger.Info("Preparing shard for split",
		zap.Stringer("shardID", shardID),
		zap.ByteString("splitKey", req.SplitKey))

	if err := shard.PrepareSplit(r.Context(), req.SplitKey); err != nil {
		if strings.Contains(err.Error(), "key out of range") {
			http.Error(w, fmt.Sprintf("Failed to prepare split: %v", err), http.StatusBadRequest)
			return
		}
		h.logger.Error("Failed to prepare shard for split",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to prepare split: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleSplitShard handles requests to split a shard
func (h *StoreAPI) handleSplitShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardSplitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read split shard request body", zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	newShardID, err := types.IDFromString(req.NewShardID)
	if err != nil {
		h.logger.Warn("Failed to parse new shard ID",
			zap.String("newShardIDString", req.NewShardID),
			zap.Error(err))
		http.Error(w, "Invalid new shard ID", http.StatusBadRequest)
		return
	} else if newShardID == shardID {
		h.logger.Warn("New shard ID must be different from the current shard ID",
			zap.Stringer("newShardID", newShardID),
			zap.Stringer("currentShardID", shardID))
		http.Error(w, "New shard ID must be different from the current shard ID", http.StatusBadRequest)
		return
	} else if newShardID == 0 {
		http.Error(w, "Shard ID must be greater than 0", http.StatusBadRequest)
		return
	}
	_, err, _ = h.shardStart.Do(shardID.String()+":"+newShardID.String(), func() (any, error) {
		// TODO (ajr) Now that we optimistically put shards into an initializing state,
		// we can't check if the newShardID already exists here and error out.
		//
		// We should add a check that the byte range matches that shard maybe?
		//
		// else if _, ok := h.store.Shard(newShardID); ok {
		// 	http.Error(w, "Shard already exists", http.StatusBadRequest)
		// 	return
		// }
		shard, _ := h.store.Shard(shardID)
		h.logger.Info("Splitting shard",
			zap.Stringer("shardID", shardID),
			zap.Stringer("newShardID", newShardID),
			zap.ByteString("splitKey", req.SplitKey))
		if err := shard.Split(r.Context(), uint64(newShardID), req.SplitKey); err != nil {
			if strings.Contains(err.Error(), "key out of range") {
				return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
			}
			if strings.Contains(err.Error(), "proposal dropped") {
				h.logger.Warn("Shard split operation was dropped",
					zap.Stringer("shardID", shardID),
					zap.Stringer("newShardID", newShardID),
					zap.Error(err))
				return nil, err
			}
			if errors.Is(err, context.Canceled) {
				h.logger.Info("Shard split operation request was canceled",
					zap.Stringer("shardID", shardID),
					zap.Stringer("newShardID", newShardID),
				)
				return nil, err
			}
			h.logger.Error("Failed to split shard",
				zap.Stringer("shardID", shardID),
				zap.Stringer("newShardID", newShardID),
				zap.Error(err))
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		if errors.Is(err, ErrBadRequest) {
			http.Error(w,
				fmt.Sprintf("Failed to split shard: %v", err),
				http.StatusBadRequest,
			)
			return
		}
		http.Error(w,
			fmt.Sprintf("Failed to split shard: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ShardFinalizeSplitRequest is the request body for the finalize split endpoint
type ShardFinalizeSplitRequest struct {
	NewRangeEnd []byte `json:"new_range_end"`
}

func (h *StoreAPI) handleFinalizeSplitShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardFinalizeSplitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read finalize split request body", zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	if len(req.NewRangeEnd) == 0 {
		http.Error(w, "new_range_end is required", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)
	h.logger.Info("Finalizing split on shard",
		zap.Stringer("shardID", shardID),
		zap.ByteString("newRangeEnd", req.NewRangeEnd))

	if err := shard.FinalizeSplit(r.Context(), req.NewRangeEnd); err != nil {
		h.logger.Error("Failed to finalize split on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w,
			fmt.Sprintf("Failed to finalize split: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleRollbackSplit aborts a split operation and restores the shard to its pre-split state.
// This is called by the reconciler when a split times out or fails.
func (h *StoreAPI) handleRollbackSplit(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	shard, _ := h.store.Shard(shardID)
	h.logger.Info("Rolling back split on shard",
		zap.Stringer("shardID", shardID))

	if err := shard.RollbackSplit(r.Context()); err != nil {
		h.logger.Error("Failed to rollback split on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w,
			fmt.Sprintf("Failed to rollback split: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleDropIndexShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardDropIndexRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read drop index request body", zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// TODO Validate the shard's key range is contained within the new range?
	shard, _ := h.store.Shard(shardID)
	if err := shard.DropIndex(r.Context(), req.Name); err != nil {
		h.logger.Error("Failed to drop index on shard",
			zap.String("indexName", req.Name),
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to drop index", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleAddIndexShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req indexes.IndexConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read add index request body", zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// TODO Validate the shard's key range is contained within the new range?
	shard, _ := h.store.Shard(shardID)
	if err := shard.AddIndex(r.Context(), req); err != nil {
		h.logger.Error("Failed to add index on shard",
			zap.String("indexName", req.Name),
			zap.Any("indexType", req.Type),
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to add index: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleBackup(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req common.BackupConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)
	// FIXME (ajr) Backups should include the byte range of the shard maybe?
	fileName := common.ShardBackupFileName(req.BackupID, shardID)
	if err := shard.Backup(r.Context(), req.Location, strings.TrimSuffix(fileName, ".tar.zst")); err != nil {
		http.Error(w, fmt.Sprintf("Failed to backup: %v", err), http.StatusInternalServerError)
		return
	}

	if after, ok0 := strings.CutPrefix(req.Location, "file://"); ok0 {
		// Try user-provided location first (single-node), then fall back to snapDir (distributed)
		userPath := filepath.Join(after, fileName)
		dataDir := h.antflyConfig.GetBaseDir()
		snapDir := common.SnapDir(dataDir, shardID, h.store.ID())
		snapPath := filepath.Join(snapDir, fileName)

		filePath := userPath
		if _, err := os.Stat(userPath); os.IsNotExist(err) { //nolint:gosec // G703: internal path with traversal protection
			filePath = snapPath
		}
		file, err := os.Open(filepath.Clean(filePath)) //nolint:gosec // G703: internal path with traversal protection
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open backup file: %v", err), http.StatusNotFound)
			return
		}
		defer func() { _ = file.Close() }()

		fileInfo, err := file.Stat()
		if err != nil {
			http.Error(
				w,
				fmt.Sprintf("Failed to stat backup file: %v", err),
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Disposition", "attachment; filename=file.dat")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(fileInfo.Size(), 10))

		// Stream the file content to the response body
		buf := make([]byte, 64*1024*1024) // 64MB buffer
		if _, err := io.CopyBuffer(w, file, buf); err != nil {
			http.Error(w, "Error streaming file", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleUpdateSchemaShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardUpdateSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	h.logger.Info("Update schema on shard",
		zap.Stringer("shardID", shardID),
		zap.Any("schema", req.Schema))
	shard, _ := h.store.Shard(shardID)

	// Convert indexes.TableSchema to schema.TableSchema via JSON
	var tableSchema schema.TableSchema
	if req.Schema != nil {
		data, err := json.Marshal(req.Schema)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to marshal schema: %v", err), http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(data, &tableSchema); err != nil {
			http.Error(w, fmt.Sprintf("Failed to unmarshal schema: %v", err), http.StatusInternalServerError)
			return
		}
	}

	if err := shard.UpdateSchema(r.Context(), &tableSchema); err != nil {
		h.logger.Error("Failed to update schema on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(
			w,
			fmt.Sprintf("Failed to update schema: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleSetRangeShard(w http.ResponseWriter, r *http.Request) {
	// Get the current shard ID from the header
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req ShardSetRangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	// TODO Validate the shard's key range is contained within the new range?
	h.logger.Info("Setting range on shard",
		zap.Stringer("shardID", shardID),
		zap.ByteString("rangeStart", req.ByteRange[0]),
		zap.ByteString("rangeEnd", req.ByteRange[1]))
	shard, _ := h.store.Shard(shardID)
	if err := shard.SetRange(r.Context(), req.ByteRange); err != nil {
		if strings.Contains(err.Error(), "key out of range") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.logger.Error("Failed to set range on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to set range: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleIsIDRemoved(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	targetNodeIDStr := r.PathValue("id")
	if targetNodeIDStr == "" {
		http.Error(w, "Missing target node ID in URL path", http.StatusBadRequest)
		return
	}

	targetNodeID, err := types.IDFromString(targetNodeIDStr)
	if err != nil {
		http.Error(w, "Invalid target node ID", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)
	// TODO (ajr) Add timeout?
	removed := shard.IsIDRemoved(uint64(targetNodeID))
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(removed); err != nil {
		http.Error(
			w,
			fmt.Sprintf("Failed to write search response: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleTransferLeadership handles requests to transfer leadership of a shard
func (h *StoreAPI) handleTransferLeadership(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	// Extract target node ID from URL path parameter (assuming Go 1.22+ mux pattern)
	targetNodeIDStr := r.PathValue("id")
	if targetNodeIDStr == "" {
		http.Error(w, "Missing target node ID in URL path", http.StatusBadRequest)
		return
	}

	targetNodeID, err := types.IDFromString(targetNodeIDStr)
	if err != nil {
		h.logger.Warn("Failed to parse target node ID for leadership transfer",
			zap.String("targetNodeIDStr", targetNodeIDStr),
			zap.Error(err))
		http.Error(w, "Invalid target node ID", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	// Check if the target node is a peer in the shard
	// Note: This might require adding a method to the shard or store to get peers easily.
	// Assuming shard.IsPeer(targetNodeID) exists for now.
	// if !shard.IsPeer(targetNodeID) {
	// 	http.Error(w, "Target node is not a peer of this shard", http.StatusBadRequest)
	// 	return
	// }

	// TODO (ajr) Add timeout?
	shard.TransferLeadership(r.Context(), targetNodeID)
	// TransferLeadership is asynchronous in etcd/raft, so we don't get immediate confirmation or error here.
	// We might want to add a mechanism to check leadership status later if needed.

	w.WriteHeader(http.StatusAccepted)
}

// handleSearch handles search requests that use the bleve index
// Supports both single JSON requests and ndjson batch requests
func (h *StoreAPI) handleSearch(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	body, err := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		h.logger.Warn("Failed to read search request body for shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	// Detect if this is ndjson (multiple JSON objects) by checking for newline-separated objects
	// ndjson has multiple JSON objects separated by newlines
	requests := splitNDJSON(body)

	if len(requests) <= 1 {
		// Single request - use original path
		resp, err := shard.Search(r.Context(), body)
		if err != nil {
			h.logger.Error("Failed to execute search on shard",
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(
				w,
				fmt.Sprintf("Failed to execute search: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(resp); err != nil { //nolint:gosec // G705: JSON/SSE API response, not HTML
			h.logger.Error("Failed to write search response for shard",
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(w, "Failed to write search response", http.StatusInternalServerError)
			return
		}
		return
	}

	// Multiple requests - execute in parallel and return ndjson
	responses := make([][]byte, len(requests))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, req := range requests {
		wg.Add(1)
		go func(idx int, reqBody []byte) {
			defer wg.Done()
			resp, err := shard.Search(r.Context(), reqBody)
			mu.Lock()
			if err != nil {
				// Return error as JSON object
				responses[idx] = fmt.Appendf(nil, `{"error":%q}`, err.Error())
			} else {
				responses[idx] = resp
			}
			mu.Unlock()
		}(i, req)
	}

	wg.Wait()

	// Write ndjson response
	w.Header().Set("Content-Type", "application/x-ndjson")
	for i, resp := range responses {
		if _, err := w.Write(resp); err != nil {
			h.logger.Error("Failed to write batch search response for shard",
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			return
		}
		if i < len(responses)-1 {
			_, _ = w.Write([]byte("\n"))
		}
	}
}

// splitNDJSON splits a byte slice containing ndjson into individual JSON objects
func splitNDJSON(data []byte) [][]byte {
	var results [][]byte
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			break
		}
		results = append(results, raw)
	}
	return results
}

// handleBatchWrite handles batch write requests
func (h *StoreAPI) handleBatchWrite(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var b db.BatchOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read full batch write request body for shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &b); err != nil {
		h.logger.Warn("Failed to decode batch write request body for shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	// BatchOp already has sync_level set by client
	if err := shard.Batch(r.Context(), &b, false); err != nil {
		// Check for partial success
		if errors.Is(err, db.ErrPartialSuccess) {
			w.Header().Set("X-Antfly-Partial-Success", "full-text-queued")
			w.WriteHeader(http.StatusAccepted) // 202 Accepted
			return
		}
		if errors.Is(err, context.Canceled) {
			h.logger.Warn("Batch write operation request was canceled",
				zap.Stringer("shardID", shardID),
			)
			http.Error(w, "Batch write operation was canceled", http.StatusRequestTimeout)
			return
		}
		h.logger.Warn("Failed to execute batch write on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		// Check if the error is ErrKeyOutOfRange and return a specific status code
		if strings.Contains(err.Error(), "key out of range") {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(
			w,
			fmt.Sprintf("Batch write operation failed: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// lookupResult is the per-key result returned by the batch lookup endpoint.
type lookupResult struct {
	Value   any    `json:"value"`
	Version uint64 `json:"version"`
}

// handleLookup handles a batch lookup (POST /lookup) request for multiple keys.
func (h *StoreAPI) handleLookup(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	var keys []string
	if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)
	results := make(map[string]lookupResult, len(keys))
	for _, key := range keys {
		v, err := shard.Lookup(r.Context(), key)
		if err != nil {
			continue // skip missing keys
		}
		ts, _ := shard.GetTimestamp(key)
		results[key] = lookupResult{Value: v, Version: ts}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		h.logger.Warn("Failed to write lookup response",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
	}
}

// handleConfChange handles configuration change operations (POST, DELETE)
func (h *StoreAPI) handleConfChange(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	key := r.PathValue("peer")
	defer func() { _ = r.Body.Close() }()

	// Extract node ID from URL path
	nodeID, err := strconv.ParseUint(key, 0, 64)
	if err != nil {
		h.logger.Warn("Failed to convert ID for conf change on shard",
			zap.String("idStr", key),
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to parse node ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		req := &ShardAddPeerRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.logger.Warn("Failed to read on POST for conf change (add node)",
				zap.Uint64("nodeID", nodeID),
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		ccc := raft.ConfChangeContext_builder{
			Url: req.PeerURL,
		}.Build()
		b, err := proto.Marshal(ccc)
		if err != nil {
			h.logger.Error("Failed to encode conf change context for adding node",
				zap.Uint64("nodeID", nodeID),
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(
				w,
				fmt.Sprintf("Failed to encode conf change context: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
			Context: b,
		}
		shard, _ := h.store.Shard(shardID)
		h.logger.Info("Received proposal to add node to shard",
			zap.Uint64("nodeID", nodeID),
			zap.Stringer("shardID", shardID),
			zap.String("context", req.PeerURL))
		if err := shard.ProposeConfChange(cc); err != nil {
			h.logger.Error("Failed to propose conf change for adding node",
				zap.Uint64("nodeID", nodeID),
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(
				w,
				fmt.Sprintf("Failed to propose conf change: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		// Optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)

	case http.MethodDelete:
		req := &ShardRemovePeerRequest{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.logger.Warn("Failed to read on POST for conf change (add node)",
				zap.Uint64("nodeID", nodeID),
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		ccc := raft.ConfChangeContext_builder{
			Timestamp: req.Timestamp,
		}.Build()
		b, err := proto.Marshal(ccc)
		if err != nil {
			h.logger.Error("Failed to encode conf change context for removing node",
				zap.Uint64("nodeID", nodeID),
				zap.Stringer("shardID", shardID),
				zap.Error(err))
			http.Error(
				w,
				fmt.Sprintf("Failed to encode conf change context: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeRemoveNode,
			NodeID:  nodeID,
			Context: b,
		}
		shard, _ := h.store.Shard(shardID)
		h.logger.Info("Received proposal to remove node from shard",
			zap.Uint64("nodeID", nodeID),
			zap.Stringer("shardID", shardID))

		// Check if sync mode is requested (wait for conf change to be applied)
		syncMode := r.URL.Query().Get("sync") == "true"
		if syncMode {
			if err := shard.ApplyConfChange(r.Context(), cc); err != nil {
				h.logger.Error("Failed to apply conf change for removing node",
					zap.Uint64("nodeID", nodeID),
					zap.Stringer("shardID", shardID),
					zap.Error(err))
				http.Error(
					w,
					fmt.Sprintf("Failed to apply conf change: %v", err),
					http.StatusInternalServerError,
				)
				return
			}
		} else {
			if err := shard.ProposeConfChange(cc); err != nil {
				h.logger.Error("Failed to propose conf change for removing node",
					zap.Uint64("nodeID", nodeID),
					zap.Stringer("shardID", shardID),
					zap.Error(err))
				http.Error(
					w,
					fmt.Sprintf("Failed to propose conf change: %v", err),
					http.StatusInternalServerError,
				)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type ScanRequest struct {
	FromKey          []byte          `json:"from_key"`
	ToKey            []byte          `json:"to_key"`
	InclusiveFrom    bool            `json:"inclusive_from,omitempty"`
	ExclusiveTo      bool            `json:"exclusive_to,omitempty"`
	IncludeDocuments bool            `json:"include_documents,omitempty"`
	FilterQuery      json.RawMessage `json:"filter_query,omitempty"`
	Limit            int             `json:"limit,omitempty"`
}

// handleScan handles requests to scan a key range in a shard
func (h *StoreAPI) handleScan(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	// Parse the request body
	var req ScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to read scan request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	opts := db.ScanOptions{
		InclusiveFrom:    req.InclusiveFrom,
		ExclusiveTo:      req.ExclusiveTo,
		IncludeDocuments: req.IncludeDocuments,
		FilterQuery:      req.FilterQuery,
		Limit:            req.Limit,
	}

	// Execute the scan
	result, err := h.store.Scan(r.Context(), shardID, req.FromKey, req.ToKey, opts)
	if err != nil {
		h.logger.Error("Failed to execute scan on shard",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w,
			fmt.Sprintf("Failed to execute scan: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	// Return the result as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		h.logger.Error("Failed to encode scan response",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// handleGetEdges retrieves edges for a document in a graph index
func (h *StoreAPI) handleGetEdges(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok {
		return
	}
	if !h.validateShard(w, r, shardID) {
		return
	}

	// Parse query parameters
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		http.Error(w, "index parameter required", http.StatusBadRequest)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key parameter required", http.StatusBadRequest)
		return
	}

	edgeType := r.URL.Query().Get("edge_type")
	directionStr := r.URL.Query().Get("direction")
	if directionStr == "" {
		directionStr = "out"
	}

	// Parse direction
	var direction indexes.EdgeDirection
	switch directionStr {
	case "out":
		direction = indexes.EdgeDirectionOut
	case "in":
		direction = indexes.EdgeDirectionIn
	case "both":
		direction = indexes.EdgeDirectionBoth
	default:
		http.Error(w, "invalid direction: must be 'out', 'in', or 'both'", http.StatusBadRequest)
		return
	}

	// Get the shard
	shard, ok := h.store.Shard(shardID)
	if !ok {
		http.Error(w, "shard not found", http.StatusNotFound)
		return
	}

	// Get edges
	edges, err := shard.GetEdges(r.Context(), indexName, []byte(key), edgeType, direction)
	if err != nil {
		h.logger.Error("Failed to get edges",
			zap.Stringer("shardID", shardID),
			zap.String("index", indexName),
			zap.String("key", key),
			zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to get edges: %v", err), http.StatusInternalServerError)
		return
	}

	// Return edges as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"edges": edges,
		"count": len(edges),
	}); err != nil {
		h.logger.Error("Failed to encode edges response",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// setupRoutes sets up the HTTP routes for the API
func (h *StoreAPI) setupRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	// Special command endpoints
	mux.HandleFunc("POST /shard", h.handleStartShard)
	mux.HandleFunc("DELETE /shard", h.handleStopShard)
	mux.HandleFunc("POST /shard/prepare-split", h.handlePrepareSplit)
	mux.HandleFunc("POST /shard/split", h.handleSplitShard)
	mux.HandleFunc("POST /shard/finalize-split", h.handleFinalizeSplitShard)
	mux.HandleFunc("POST /shard/rollback-split", h.handleRollbackSplit)
	mux.HandleFunc("POST /shard/backup", h.handleBackup)
	mux.HandleFunc("POST /shard/setrange", h.handleSetRangeShard)
	mux.HandleFunc("POST /shard/schema", h.handleUpdateSchemaShard)
	mux.HandleFunc("POST /shard/index", h.handleAddIndexShard)
	mux.HandleFunc("DELETE /shard/index", h.handleDropIndexShard)
	mux.HandleFunc("POST /shard/transferleadership/{id}", h.handleTransferLeadership)
	mux.HandleFunc("GET /shard/isremoved/{id}", h.handleIsIDRemoved)
	mux.HandleFunc("GET /shard/mediankey", h.handleGetMedianKey)
	mux.HandleFunc("GET /status", h.handleGetStoreStatus)
	mux.HandleFunc("POST /search", h.handleSearch)
	mux.HandleFunc("POST /batch", h.handleBatchWrite)
	mux.HandleFunc("POST /scan", h.handleScan)

	// Graph endpoints
	mux.HandleFunc("GET /graph/edges", h.handleGetEdges)

	mux.HandleFunc("DELETE /shard/peer/{peer}", h.handleConfChange)
	mux.HandleFunc("POST /shard/peer/{peer}", h.handleConfChange)

	// Transaction endpoints
	mux.HandleFunc("POST /txn/init", h.handleInitTransaction)
	mux.HandleFunc("POST /txn/write-intent", h.handleWriteIntent)
	mux.HandleFunc("POST /txn/commit", h.handleCommitTransaction)
	mux.HandleFunc("POST /txn/abort", h.handleAbortTransaction)
	mux.HandleFunc("POST /txn/resolve", h.handleResolveIntent)
	mux.HandleFunc("POST /txn/status", h.handleGetTransactionStatus)

	mux.HandleFunc("POST /lookup", h.handleLookup)
	return mux
}

// Transaction handlers

func (h *StoreAPI) handleInitTransaction(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req db.InitTransactionOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read init transaction request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &req); err != nil {
		h.logger.Warn("Failed to decode init transaction request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	op := db.Op_builder{
		Op:              db.Op_OpInitTransaction,
		InitTransaction: &req,
	}.Build()

	if err := shard.SyncWriteOp(r.Context(), op); err != nil {
		h.logger.Warn("Failed to initialize transaction",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleWriteIntent(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req db.WriteIntentOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read write intent request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &req); err != nil {
		h.logger.Warn("Failed to decode write intent request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	op := db.Op_builder{
		Op:          db.Op_OpWriteIntent,
		WriteIntent: &req,
	}.Build()

	if err := shard.SyncWriteOp(r.Context(), op); err != nil {
		h.logger.Warn("Failed to write intent",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req db.CommitTransactionOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read commit transaction request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &req); err != nil {
		h.logger.Warn("Failed to decode commit transaction request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	op := db.Op_builder{
		Op:                db.Op_OpCommitTransaction,
		CommitTransaction: &req,
	}.Build()

	if err := shard.SyncWriteOp(r.Context(), op); err != nil {
		h.logger.Warn("Failed to commit transaction",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleAbortTransaction(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req db.AbortTransactionOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read abort transaction request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &req); err != nil {
		h.logger.Warn("Failed to decode abort transaction request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	op := db.Op_builder{
		Op:               db.Op_OpAbortTransaction,
		AbortTransaction: &req,
	}.Build()

	if err := shard.SyncWriteOp(r.Context(), op); err != nil {
		h.logger.Warn("Failed to abort transaction",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleResolveIntent(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req db.ResolveIntentsOp
	full, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Warn("Failed to read resolve intent request body",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := proto.Unmarshal(full, &req); err != nil {
		h.logger.Warn("Failed to decode resolve intent request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	op := db.Op_builder{
		Op:             db.Op_OpResolveIntents,
		ResolveIntents: &req,
	}.Build()

	if err := shard.SyncWriteOp(r.Context(), op); err != nil {
		h.logger.Warn("Failed to resolve intents",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *StoreAPI) handleGetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	shardID, ok := h.getShardID(w, r)
	if !ok || !h.validateShard(w, r, shardID) {
		return
	}

	var req struct {
		TxnID []byte `json:"txn_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("Failed to decode get transaction status request",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, "Failed to decode request body", http.StatusBadRequest)
		return
	}

	shard, _ := h.store.Shard(shardID)

	status, err := shard.GetTransactionStatus(r.Context(), req.TxnID)
	if err != nil {
		h.logger.Warn("Failed to get transaction status",
			zap.Stringer("shardID", shardID),
			zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]int32{
		"status": status,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.Error("Failed to encode response", zap.Error(err))
	}
}
