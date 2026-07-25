// Copyright 2026 Antfly, Inc.
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

package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	json "github.com/antflydb/antfly/go/pkg/libaf/json"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/multirafthttp"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/workerpool"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tablemgr"
	"github.com/antflydb/antfly/go/pkg/antfly/src/usermgr"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (t *TableApi) acquireBackupTransfer(ctx context.Context) (func(), error) {
	t.backupTransferOnce.Do(func() {
		t.backupTransfers = make(chan struct{}, innerFanOutLimit)
	})
	select {
	case t.backupTransfers <- struct{}{}:
		return func() { <-t.backupTransfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *TableApi) BackupTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()
	var br BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&br); err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := common.ValidateBackupID(br.BackupId); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup ID: %v", err), http.StatusBadRequest)
		return
	}
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		err := fmt.Errorf("getting table %s: %w", tableName, err)
		errorResponse(w, err.Error(), http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	backupConfig := common.BackupConfig{
		BackupID:   br.BackupId,
		Connection: br.Connection,
		Location:   br.Location,
		Format:     backupFormatFromRequest(br.Format),
	}
	metadataStore, err := newBackupStore(
		t.ln.config,
		br.Connection,
		"backup.write",
		br.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup location: %v", err), http.StatusBadRequest)
		return
	}
	if err := metadataStore.ReserveBackupID(ctx, br.BackupId); err != nil {
		writeBackupError(w, "Backup ID is not available", err)
		return
	}
	backupConfig.ResolvedLocation = metadataStore.ResolvedLocation()
	g, _ := workerpool.NewGroup(ctx, t.pool)
	for shardID := range table.Shards {
		g.Go(func(ctx context.Context) error {
			release, err := t.acquireBackupTransfer(ctx)
			if err != nil {
				return err
			}
			defer release()
			// Forward the insert to the appropriate shard
			if err := t.ln.forwardBackupToShard(ctx, shardID, backupConfig); err != nil {
				if !errors.Is(err, context.Canceled) {
					t.logger.Error("Error forwarding backup", zap.Error(err))
				}
				return fmt.Errorf("backing up shard %s: %w", shardID, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		writeBackupError(w, "Failed to forward backup request", err)
		return
	}
	if err := ctx.Err(); err != nil {
		writeBackupError(w, "Backup operation was interrupted", err)
		return
	}

	if err := metadataStore.WriteMetadata(ctx, br.BackupId, table, backupConfig.Format); err != nil {
		writeBackupError(w, "Failed to write backup metadata", err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"backup": "successful",
	}); err != nil {
		t.logger.Warn("Error encoding response", zap.Error(err))
		errorResponse(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func (t *TableApi) RestoreTable(
	w http.ResponseWriter,
	r *http.Request,
	tableName string,
	_ RestoreTableParams,
) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()
	var rr RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to parse restore request: %v", err),
			http.StatusBadRequest,
		)
		return
	}
	if err := common.ValidateBackupID(rr.BackupId); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup ID: %v", err), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	metadataStore, err := newBackupStore(
		t.ln.config,
		rr.Connection,
		"restore.read",
		rr.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid restore location: %v", err), http.StatusBadRequest)
		return
	}
	tableMetadata, backupFormat, err := metadataStore.ReadMetadata(ctx, rr.BackupId)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Failed to read backup metadata: %v", err), http.StatusInternalServerError)
		return
	}

	if tableMetadata.Name != tableName {
		errorResponse(
			w,
			fmt.Sprintf(
				"Table name mismatch: expected %s, but backup metadata is for %s",
				tableName,
				tableMetadata.Name,
			),
			http.StatusBadRequest,
		)
		return
	}

	// RestoreTable should create the table with the exact shard configuration from metadata.
	// It should also handle persistence of this table structure.
	// FIXME (ajr) Restore should put shards into a needs snapshot state
	// and the reconciliation loop needs to detect that state and use the restore config when
	// autoscaling on this tables shards.
	// MVP (ajr) Contains side-effects for raft log
	if err := t.tm.RestoreTable(tableMetadata, &common.BackupConfig{
		Location:         rr.Location,
		ResolvedLocation: metadataStore.ResolvedLocation(),
		Connection:       rr.Connection,
		BackupID:         rr.BackupId,
		Format:           backupFormat,
	}); err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to restore table structure: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	// Trigger reconciliation to ensure new raft groups are formed and shards become operational.
	t.ln.TriggerReconciliation()

	// TODO (ajr) Restore is asynchronous, maybe we should poll the status for synchronous?
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"restore": "triggered",
	}); err != nil {
		t.logger.Warn("Error encoding restore success response", zap.Error(err))
		// Don't write another http.Error here as headers/status might have been sent.
	}
}

// backupFormatFromRequest converts the OpenAPI-generated format type to the
// internal BackupFormat used throughout the backup pipeline.
func backupFormatFromRequest(format BackupRequestFormat) common.BackupFormat {
	return common.NormalizeBackupFormat(common.BackupFormat(format))
}

func clusterBackupFormatFromRequest(format ClusterBackupRequestFormat) common.BackupFormat {
	return common.NormalizeBackupFormat(common.BackupFormat(format))
}

func backupInfoFormatFromMetadata(format common.BackupFormat) BackupInfoFormat {
	return BackupInfoFormat(format)
}

func tableBackupMetadataID(tableName, backupID string) string {
	digest := sha256.Sum256([]byte(tableName + "\x00" + backupID))
	return fmt.Sprintf("table-%x", digest)
}

func validateBackupTableNames(tableNames []string) error {
	seen := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		if strings.TrimSpace(tableName) == "" {
			return errors.New("table names cannot be empty")
		}
		if _, ok := seen[tableName]; ok {
			return fmt.Errorf("table %q is selected more than once", tableName)
		}
		seen[tableName] = struct{}{}
	}
	return nil
}

func writeBackupError(w http.ResponseWriter, message string, err error) {
	switch {
	case errors.Is(err, ErrBackupAlreadyExists):
		errorResponse(w, message+": "+err.Error(), http.StatusConflict)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		errorResponse(w, message+": "+err.Error(), http.StatusRequestTimeout)
	default:
		errorResponse(w, message+": "+err.Error(), http.StatusInternalServerError)
	}
}

// ClusterBackupMetadata represents the metadata for a cluster-level backup
type ClusterBackupMetadata struct {
	Version       uint32                   `json:"version"`
	BackupID      string                   `json:"backup_id"`
	Timestamp     time.Time                `json:"timestamp"`
	AntflyVersion string                   `json:"antfly_version"`
	Format        common.BackupFormat      `json:"format,omitempty"`
	Tables        []ClusterBackupTableInfo `json:"tables"`
}

const clusterBackupMetadataVersion = 1

// ClusterBackupTableInfo tracks backup status for a single table in a cluster backup
type ClusterBackupTableInfo struct {
	Name           string `json:"name"`
	BackupLocation string `json:"backup_location"`
	ShardCount     int    `json:"shard_count"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

func validateClusterBackupMetadata(id string, meta *ClusterBackupMetadata) error {
	if meta == nil {
		return fmt.Errorf("cluster backup metadata is required")
	}
	if meta.Version != clusterBackupMetadataVersion {
		return fmt.Errorf("unsupported cluster backup metadata version %d", meta.Version)
	}
	if meta.BackupID != id {
		return fmt.Errorf(
			"cluster backup metadata ID mismatch: requested %q, found %q",
			id,
			meta.BackupID,
		)
	}
	switch meta.Format {
	case common.BackupFormatNative, common.BackupFormatPortable:
		return nil
	default:
		return fmt.Errorf("unsupported cluster backup format %q", meta.Format)
	}
}

func writeClusterMetadataToFile(ctx context.Context, location, id string, meta *ClusterBackupMetadata) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	if err := validateClusterBackupMetadata(id, meta); err != nil {
		return err
	}
	filePath := filepath.Join(
		strings.TrimPrefix(location, "file://"),
		id+"-cluster-metadata.json",
	)
	return writeJSONFileAtomically(ctx, filepath.Clean(filePath), meta)
}

func writeClusterMetadataToBlobStore(ctx context.Context, id string, meta *ClusterBackupMetadata, s3Info *common.S3Info) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	if err := validateClusterBackupMetadata(id, meta); err != nil {
		return err
	}
	bucket := s3Info.Bucket
	prefix := s3Info.Prefix
	minioClient, err := s3Info.EnsureBucket(ctx)
	if err != nil {
		return err
	}
	b := bytes.NewBuffer(nil)
	writer := &boundedWriter{writer: b, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(meta); err != nil {
		return fmt.Errorf("encoding cluster metadata to JSON: %w", err)
	}
	// Construct object key with optional prefix
	objectKey := id + "-cluster-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	options := minio.PutObjectOptions{ContentType: "application/json"}
	options.SetMatchETagExcept("*")
	if _, err := minioClient.PutObject(ctx, bucket, objectKey, b, int64(b.Len()), options); err != nil {
		if common.IsS3CreateConflict(err) {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		}
		return fmt.Errorf("uploading cluster metadata to object store: %w", err)
	}
	return nil
}

func readClusterMetadataFromFile(_ context.Context, location, id string) (*ClusterBackupMetadata, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return nil, err
	}
	filePath := filepath.Join(
		strings.TrimPrefix(location, "file://"),
		id+"-cluster-metadata.json",
	)
	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata file %s: %w", filePath, err)
	}
	defer func() { _ = file.Close() }()
	data, err := readBackupMetadata(file)
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata file %s: %w", filePath, err)
	}
	var meta ClusterBackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshalling cluster metadata: %w", err)
	}
	if err := validateClusterBackupMetadata(id, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func readClusterMetadataFromBlobStore(ctx context.Context, id string, s3Info *common.S3Info) (*ClusterBackupMetadata, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return nil, err
	}
	bucket := s3Info.Bucket
	prefix := s3Info.Prefix
	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}

	// Construct object key with optional prefix
	objectKey := id + "-cluster-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	obj, err := minioClient.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %s from bucket %s: %w", objectKey, bucket, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := readBackupMetadata(obj)
	if err != nil {
		return nil, fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}

	var meta ClusterBackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshalling cluster metadata: %w", err)
	}
	if err := validateClusterBackupMetadata(id, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func ensureClusterMetadataAbsent(
	ctx context.Context,
	location, id string,
	s3Info *common.S3Info,
) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	if strings.HasPrefix(location, "s3://") {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return err
		}
		objectKey := id + "-cluster-metadata.json"
		if s3Info.Prefix != "" {
			objectKey = path.Join(s3Info.Prefix, objectKey)
		}
		if _, err := client.StatObject(
			ctx,
			s3Info.Bucket,
			objectKey,
			minio.StatObjectOptions{},
		); err == nil {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		} else if !isS3ObjectNotFound(err) {
			return fmt.Errorf("checking cluster backup metadata %s: %w", objectKey, err)
		}
		return nil
	}
	filePath := filepath.Join(
		strings.TrimPrefix(location, "file://"),
		id+"-cluster-metadata.json",
	)
	if _, err := os.Stat(filePath); err == nil {
		return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking cluster backup metadata %s: %w", filePath, err)
	}
	return nil
}

// Backup backs up all tables or selected tables
func (t *TableApi) Backup(w http.ResponseWriter, r *http.Request) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	var req ClusterBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}
	if err := common.ValidateBackupID(req.BackupId); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup ID: %v", err), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	resolvedLocation, s3Info, err := resolveBackupLocation(
		t.ln.config,
		req.Connection,
		"backup.write",
		req.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup location: %v", err), http.StatusBadRequest)
		return
	}
	metadataStore, err := newBackupStore(
		t.ln.config,
		req.Connection,
		"backup.write",
		req.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup location: %v", err), http.StatusBadRequest)
		return
	}
	backupConfig := common.BackupConfig{
		BackupID:         req.BackupId,
		Connection:       req.Connection,
		Location:         req.Location,
		Format:           clusterBackupFormatFromRequest(req.Format),
		ResolvedLocation: metadataStore.ResolvedLocation(),
	}

	// Get list of tables to backup
	var tableNames []string
	if len(req.TableNames) > 0 {
		tableNames = req.TableNames
	} else {
		// Backup all tables
		tables, err := t.tm.Tables(nil, nil)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Failed to list tables: %v", err), http.StatusInternalServerError)
			return
		}
		for _, table := range tables {
			tableNames = append(tableNames, table.Name)
		}
	}

	if len(tableNames) == 0 {
		errorResponse(w, "No tables to backup", http.StatusBadRequest)
		return
	}
	if err := validateBackupTableNames(tableNames); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid table selection: %v", err), http.StatusBadRequest)
		return
	}
	if err := ensureClusterMetadataAbsent(ctx, resolvedLocation, req.BackupId, s3Info); err != nil {
		writeBackupError(w, "Backup ID is not available", err)
		return
	}
	if err := metadataStore.ReserveBackupID(ctx, req.BackupId); err != nil {
		writeBackupError(w, "Backup ID is not available", err)
		return
	}

	// Create cluster metadata
	backupFormat := backupConfig.Format
	clusterMeta := &ClusterBackupMetadata{
		Version:       clusterBackupMetadataVersion,
		BackupID:      req.BackupId,
		Timestamp:     time.Now(),
		AntflyVersion: multirafthttp.Version,
		Format:        backupFormat,
		Tables:        make([]ClusterBackupTableInfo, len(tableNames)),
	}

	// Track results for response
	results := make([]TableBackupStatus, len(tableNames))

	// Backup each table in parallel
	g, _ := workerpool.NewGroup(ctx, t.pool)
	for i, tableName := range tableNames {
		g.Go(func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			table, err := t.tm.GetTable(tableName)
			if err != nil {
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  fmt.Sprintf("table not found: %v", err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:   tableName,
					Status: "failed",
					Error:  err.Error(),
				}
				return nil // Don't fail entire backup for one table
			}

			tableBackupID := tableBackupMetadataID(tableName, req.BackupId)
			if err := metadataStore.EnsureMetadataAbsent(ctx, tableBackupID); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  fmt.Sprintf("backup metadata ID is not available: %v", err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      err.Error(),
				}
				return nil
			}

			// Backup all shards for this table.
			// Use errgroup (not the shared pool) to avoid deadlock: the outer
			// group already occupies pool workers, so nesting on the same pool
			// can exhaust all slots when there are many tables.
			shardEg, shardCtx := errgroup.WithContext(ctx)
			shardEg.SetLimit(innerFanOutLimit)
			for shardID := range table.Shards {
				shardEg.Go(func() error {
					release, err := t.acquireBackupTransfer(shardCtx)
					if err != nil {
						return err
					}
					defer release()
					if err := t.ln.forwardBackupToShard(shardCtx, shardID, backupConfig); err != nil {
						if !errors.Is(err, context.Canceled) {
							t.logger.Error("Error forwarding backup", zap.String("table", tableName), zap.Error(err))
						}
						return fmt.Errorf("backing up shard %s: %w", shardID, err)
					}
					return nil
				})
			}

			if err := shardEg.Wait(); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  err.Error(),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      err.Error(),
				}
				return nil // Don't fail entire backup for one table
			}

			// Write table metadata with table-specific backup ID
			if err := metadataStore.WriteMetadata(ctx, tableBackupID, table, backupFormat); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  fmt.Sprintf("failed to write metadata: %v", err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      err.Error(),
				}
				return nil
			}

			results[i] = TableBackupStatus{
				Name:   tableName,
				Status: TableBackupStatusStatusCompleted,
			}
			clusterMeta.Tables[i] = ClusterBackupTableInfo{
				Name:           tableName,
				BackupLocation: fmt.Sprintf("%s/%s-metadata.json", req.Location, tableBackupID),
				ShardCount:     len(table.Shards),
				Status:         "completed",
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		writeBackupError(w, "Cluster backup was interrupted", err)
		return
	}
	if err := ctx.Err(); err != nil {
		writeBackupError(w, "Cluster backup was interrupted", err)
		return
	}

	// Write cluster-level metadata
	if strings.HasPrefix(req.Location, "s3://") {
		if err := writeClusterMetadataToBlobStore(ctx, req.BackupId, clusterMeta, s3Info); err != nil {
			writeBackupError(w, "Failed to write cluster metadata", err)
			return
		}
	} else {
		if err := writeClusterMetadataToFile(ctx, resolvedLocation, req.BackupId, clusterMeta); err != nil {
			writeBackupError(w, "Failed to write cluster metadata", err)
			return
		}
	}

	// Determine overall status
	status := ClusterBackupResponseStatusCompleted
	failedCount := 0
	for _, result := range results {
		if result.Status == TableBackupStatusStatusFailed {
			failedCount++
		}
	}
	if failedCount == len(results) {
		status = ClusterBackupResponseStatusFailed
	} else if failedCount > 0 {
		status = ClusterBackupResponseStatusPartial
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(ClusterBackupResponse{
		BackupId: req.BackupId,
		Tables:   results,
		Status:   status,
	}); err != nil {
		t.logger.Warn("Error encoding response", zap.Error(err))
	}
}

// Restore restores multiple tables from a cluster backup
func (t *TableApi) Restore(w http.ResponseWriter, r *http.Request, _ RestoreParams) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	var req ClusterRestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}
	if err := common.ValidateBackupID(req.BackupId); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup ID: %v", err), http.StatusBadRequest)
		return
	}

	// Default restore mode
	restoreMode := req.RestoreMode
	if restoreMode == "" {
		restoreMode = ClusterRestoreRequestRestoreModeFailIfExists
	}
	switch restoreMode {
	case ClusterRestoreRequestRestoreModeFailIfExists,
		ClusterRestoreRequestRestoreModeSkipIfExists,
		ClusterRestoreRequestRestoreModeOverwrite:
	default:
		errorResponse(w, fmt.Sprintf("Invalid restore mode %q", restoreMode), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	resolvedLocation, s3Info, err := resolveBackupLocation(
		t.ln.config,
		req.Connection,
		"restore.read",
		req.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid restore location: %v", err), http.StatusBadRequest)
		return
	}
	metadataStore, err := newBackupStore(
		t.ln.config,
		req.Connection,
		"restore.read",
		req.Location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid restore location: %v", err), http.StatusBadRequest)
		return
	}

	// Read cluster backup metadata
	var clusterMeta *ClusterBackupMetadata
	if strings.HasPrefix(req.Location, "s3://") {
		clusterMeta, err = readClusterMetadataFromBlobStore(ctx, req.BackupId, s3Info)
	} else {
		clusterMeta, err = readClusterMetadataFromFile(ctx, resolvedLocation, req.BackupId)
	}
	if err != nil {
		errorResponse(w, fmt.Sprintf("Failed to read cluster backup metadata: %v", err), http.StatusInternalServerError)
		return
	}
	restoreFormat := clusterMeta.Format
	switch restoreFormat {
	case common.BackupFormatNative, common.BackupFormatPortable:
	default:
		errorResponse(
			w,
			fmt.Sprintf("Invalid backup format in cluster metadata: %q", restoreFormat),
			http.StatusInternalServerError,
		)
		return
	}

	// Determine which tables to restore
	tablesToRestore := req.TableNames
	if len(tablesToRestore) == 0 {
		// Restore all tables from backup
		for _, tableInfo := range clusterMeta.Tables {
			if tableInfo.Status == "completed" {
				tablesToRestore = append(tablesToRestore, tableInfo.Name)
			}
		}
	}

	if len(tablesToRestore) == 0 {
		errorResponse(w, "No tables to restore", http.StatusBadRequest)
		return
	}
	if err := validateBackupTableNames(tablesToRestore); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid table selection: %v", err), http.StatusBadRequest)
		return
	}

	// Validate tables exist in backup
	backupTables := make(map[string]bool)
	for _, tableInfo := range clusterMeta.Tables {
		if tableInfo.Status == "completed" {
			backupTables[tableInfo.Name] = true
		}
	}
	for _, tableName := range tablesToRestore {
		if !backupTables[tableName] {
			errorResponse(w, fmt.Sprintf("Table %s not found in backup or backup failed", tableName), http.StatusBadRequest)
			return
		}
	}

	// Preflight every selected table before mutating catalog state. This keeps
	// malformed or incomplete backups from producing a partially admitted
	// restore and makes fail_if_exists an operation-wide admission check.
	results := make([]TableRestoreStatus, len(tablesToRestore))
	tableMetadata := make([]*store.Table, len(tablesToRestore))
	tableExists := make([]bool, len(tablesToRestore))
	preflight, _ := workerpool.NewGroup(ctx, t.pool)
	for i, tableName := range tablesToRestore {
		preflight.Go(func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := t.tm.GetTable(tableName)
			switch {
			case err == nil:
				tableExists[i] = true
			case errors.Is(err, tablemgr.ErrNotFound):
			case err != nil:
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("failed to inspect existing table: %v", err),
				}
				return nil
			}
			if tableExists[i] && restoreMode == ClusterRestoreRequestRestoreModeSkipIfExists {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusSkipped,
				}
				return nil
			}

			tableBackupID := tableBackupMetadataID(tableName, req.BackupId)
			metadata, tableFormat, err := metadataStore.ReadMetadata(ctx, tableBackupID)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("failed to read backup metadata: %v", err),
				}
				return nil
			}
			if tableFormat != restoreFormat {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error: fmt.Sprintf(
						"backup format mismatch: cluster metadata has %q, table metadata has %q",
						restoreFormat,
						tableFormat,
					),
				}
				return nil
			}

			if metadata.Name != tableName {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("table name mismatch: expected %s, got %s", tableName, metadata.Name),
				}
				return nil
			}
			tableMetadata[i] = metadata
			return nil
		})
	}
	if err := preflight.Wait(); err != nil {
		writeBackupError(w, "Cluster restore preflight was interrupted", err)
		return
	}

	for i, tableName := range tablesToRestore {
		if results[i].Status == TableRestoreStatusStatusFailed {
			errorResponse(
				w,
				fmt.Sprintf("Restore preflight failed for table %s: %s", tableName, results[i].Error),
				http.StatusInternalServerError,
			)
			return
		}
		if !tableExists[i] {
			continue
		}
		switch restoreMode {
		case ClusterRestoreRequestRestoreModeFailIfExists:
			errorResponse(w, fmt.Sprintf("Table %s already exists", tableName), http.StatusConflict)
			return
		case ClusterRestoreRequestRestoreModeOverwrite:
			errorResponse(
				w,
				"Atomic overwrite restore is not supported by the Go metadata server",
				http.StatusNotImplemented,
			)
			return
		}
	}

	// Admission succeeded for the complete set. Restore non-skipped tables in
	// parallel; each asynchronous restore retains its per-table result.
	g, _ := workerpool.NewGroup(ctx, t.pool)
	for i, tableName := range tablesToRestore {
		g.Go(func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if tableMetadata[i] == nil {
				return nil
			}
			if err := t.tm.RestoreTable(tableMetadata[i], &common.BackupConfig{
				Location:         req.Location,
				ResolvedLocation: metadataStore.ResolvedLocation(),
				Connection:       req.Connection,
				BackupID:         req.BackupId,
				Format:           restoreFormat,
			}); err != nil {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("failed to restore table: %v", err),
				}
				return nil
			}

			results[i] = TableRestoreStatus{
				Name:   tableName,
				Status: TableRestoreStatusStatusTriggered,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		writeBackupError(w, "Cluster restore was interrupted", err)
		return
	}

	// Trigger reconciliation to ensure new raft groups are formed
	t.ln.TriggerReconciliation()

	// Determine overall status
	status := ClusterRestoreResponseStatusTriggered
	failedCount := 0
	for _, result := range results {
		if result.Status == TableRestoreStatusStatusFailed {
			failedCount++
		}
	}
	if failedCount == len(results) {
		status = ClusterRestoreResponseStatusFailed
	} else if failedCount > 0 {
		status = ClusterRestoreResponseStatusPartial
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(ClusterRestoreResponse{
		Tables: results,
		Status: status,
	}); err != nil {
		t.logger.Warn("Error encoding restore response", zap.Error(err))
	}
}

// ListBackups lists available cluster backups at a location
func (t *TableApi) ListBackups(w http.ResponseWriter, r *http.Request, params ListBackupsParams) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeRead) {
		return
	}

	ctx := r.Context()
	location := params.Location
	resolvedLocation, s3Info, err := resolveBackupLocation(
		t.ln.config,
		params.Connection,
		"restore.read",
		location,
	)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup location: %v", err), http.StatusBadRequest)
		return
	}

	var backups []BackupInfo

	if strings.HasPrefix(location, "s3://") {
		// e.g. "s3://my-bucket-name/optional/prefix"
		bucket, prefix, err := common.ParseS3URL(location)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Invalid location URL: %v", err), http.StatusBadRequest)
			return
		}
		minioClient, err := s3Info.NewMinioClient()
		if err != nil {
			errorResponse(w, fmt.Sprintf("Failed to create S3 client: %v", err), http.StatusInternalServerError)
			return
		}

		// List objects with cluster-metadata suffix
		objectCh := minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})
		for object := range objectCh {
			if object.Err != nil {
				t.logger.Warn("Error listing objects", zap.Error(object.Err))
				continue
			}
			if before, ok := strings.CutSuffix(object.Key, "-cluster-metadata.json"); ok {
				// Extract backup ID from filename (strip the prefix if present)
				backupID := before
				if prefix != "" {
					backupID = strings.TrimPrefix(backupID, prefix)
					backupID = strings.TrimPrefix(backupID, "/")
				}

				// Read the metadata
				meta, err := readClusterMetadataFromBlobStore(ctx, backupID, s3Info)
				if err != nil {
					t.logger.Warn("Error reading cluster metadata", zap.String("backup_id", backupID), zap.Error(err))
					continue
				}

				tableNames := make([]string, 0, len(meta.Tables))
				for _, tableInfo := range meta.Tables {
					if tableInfo.Status == "completed" {
						tableNames = append(tableNames, tableInfo.Name)
					}
				}

				backups = append(backups, BackupInfo{
					BackupId:      meta.BackupID,
					Timestamp:     meta.Timestamp,
					Tables:        tableNames,
					Location:      location,
					AntflyVersion: meta.AntflyVersion,
					Format:        backupInfoFormatFromMetadata(meta.Format),
				})
			}
		}
	} else {
		// File-based listing
		dirPath := strings.TrimPrefix(resolvedLocation, "file://")
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Failed to read directory: %v", err), http.StatusInternalServerError)
			return
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if before, ok := strings.CutSuffix(entry.Name(), "-cluster-metadata.json"); ok {
				// Extract backup ID from filename
				backupID := before

				// Read the metadata
				meta, err := readClusterMetadataFromFile(ctx, resolvedLocation, backupID)
				if err != nil {
					t.logger.Warn("Error reading cluster metadata", zap.String("backup_id", backupID), zap.Error(err))
					continue
				}

				tableNames := make([]string, 0, len(meta.Tables))
				for _, tableInfo := range meta.Tables {
					if tableInfo.Status == "completed" {
						tableNames = append(tableNames, tableInfo.Name)
					}
				}

				backups = append(backups, BackupInfo{
					BackupId:      meta.BackupID,
					Timestamp:     meta.Timestamp,
					Tables:        tableNames,
					Location:      location,
					AntflyVersion: meta.AntflyVersion,
					Format:        backupInfoFormatFromMetadata(meta.Format),
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(BackupListResponse{
		Backups: backups,
	}); err != nil {
		t.logger.Warn("Error encoding response", zap.Error(err))
	}
}
