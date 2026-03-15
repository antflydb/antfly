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

package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/multirafthttp"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/usermgr"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

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
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		err := fmt.Errorf("getting table %s: %w", tableName, err)
		errorResponse(w, err.Error(), http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	g, _ := workerpool.NewGroup(ctx, t.pool)
	for shardID := range table.Shards {
		g.Go(func(ctx context.Context) error {
			// Forward the insert to the appropriate shard
			if err := t.ln.forwardBackupToShard(ctx, shardID, br.Location, br.BackupId); err != nil {
				if !errors.Is(err, context.Canceled) {
					t.logger.Error("Error forwarding backup", zap.Error(err))
				}
				return fmt.Errorf("backing up shard %s: %v", shardID, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			t.logger.Warn("Backup operation cancelled", zap.Error(err))
		} else {
			errorResponse(w, fmt.Sprintf("Failed to forward backup request: %v", err), http.StatusInternalServerError)
			return
		}
	}

	if err := newBackupStore(br.Location, &t.ln.config.Storage.S3).WriteMetadata(ctx, br.BackupId, table); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to write backup metadata: %v", err), http.StatusInternalServerError)
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

func (t *TableApi) RestoreTable(w http.ResponseWriter, r *http.Request, tableName string) {
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

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	tableMetadata, err := newBackupStore(rr.Location, &t.ln.config.Storage.S3).ReadMetadata(ctx, rr.BackupId)
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
		Location: rr.Location, BackupID: rr.BackupId,
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

// ClusterBackupMetadata represents the metadata for a cluster-level backup
type ClusterBackupMetadata struct {
	BackupID      string                   `json:"backup_id"`
	Timestamp     time.Time                `json:"timestamp"`
	AntflyVersion string                   `json:"antfly_version"`
	Tables        []ClusterBackupTableInfo `json:"tables"`
}

// ClusterBackupTableInfo tracks backup status for a single table in a cluster backup
type ClusterBackupTableInfo struct {
	Name           string `json:"name"`
	BackupLocation string `json:"backup_location"`
	ShardCount     int    `json:"shard_count"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

func writeClusterMetadataToFile(_ context.Context, location, id string, meta *ClusterBackupMetadata) error {
	filePath := path.Join(
		strings.TrimPrefix(location, "file://"),
		id+"-cluster-metadata.json",
	)
	file, err := os.Create(filepath.Clean(filePath))
	if err != nil {
		return fmt.Errorf("creating cluster metadata file: %w", err)
	}
	defer func() { _ = file.Close() }()
	if err := json.NewEncoder(file).Encode(meta); err != nil {
		return fmt.Errorf("encoding cluster metadata to JSON: %w", err)
	}
	return nil
}

func writeClusterMetadataToBlobStore(ctx context.Context, bucketURL, id string, meta *ClusterBackupMetadata, s3Info *common.S3Info) error {
	// e.g. "s3://my-bucket-name/optional/prefix"
	bucket, prefix, err := common.ParseS3URL(bucketURL)
	if err != nil {
		return fmt.Errorf("parsing bucket URL: %w", err)
	}
	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	if ok, err := minioClient.BucketExists(ctx, bucket); err != nil {
		return fmt.Errorf("checking if bucket %s exists: %w", bucket, err)
	} else if !ok {
		return fmt.Errorf("bucket %s does not exist", bucket)
	}
	b := bytes.NewBuffer(nil)
	if err := json.NewEncoder(b).Encode(meta); err != nil {
		return fmt.Errorf("encoding cluster metadata to JSON: %w", err)
	}
	// Construct object key with optional prefix
	objectKey := id + "-cluster-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	if _, err := minioClient.PutObject(ctx, bucket, objectKey, b, int64(b.Len()), minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("uploading cluster metadata to object store: %w", err)
	}
	return nil
}

func readClusterMetadataFromFile(_ context.Context, location, id string) (*ClusterBackupMetadata, error) {
	filePath := path.Join(
		strings.TrimPrefix(location, "file://"),
		id+"-cluster-metadata.json",
	)
	data, err := os.ReadFile(filepath.Clean(filePath))
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata file %s: %w", filePath, err)
	}
	var meta ClusterBackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshalling cluster metadata: %w", err)
	}
	return &meta, nil
}

func readClusterMetadataFromBlobStore(ctx context.Context, bucketURL, id string, s3Info *common.S3Info) (*ClusterBackupMetadata, error) {
	// e.g. "s3://my-bucket-name/optional/prefix"
	bucket, prefix, err := common.ParseS3URL(bucketURL)
	if err != nil {
		return nil, fmt.Errorf("parsing bucket URL: %w", err)
	}
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

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}

	var meta ClusterBackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshalling cluster metadata: %w", err)
	}
	return &meta, nil
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

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

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

	// Create cluster metadata
	clusterMeta := &ClusterBackupMetadata{
		BackupID:      req.BackupId,
		Timestamp:     time.Now(),
		AntflyVersion: multirafthttp.Version,
		Tables:        make([]ClusterBackupTableInfo, 0, len(tableNames)),
	}

	// Track results for response
	results := make([]TableBackupStatus, len(tableNames))
	var mu sync.Mutex

	// Backup each table in parallel
	g, _ := workerpool.NewGroup(ctx, t.pool)
	for i, tableName := range tableNames {
		g.Go(func(ctx context.Context) error {
			table, err := t.tm.GetTable(tableName)
			if err != nil {
				mu.Lock()
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  fmt.Sprintf("table not found: %v", err),
				}
				clusterMeta.Tables = append(clusterMeta.Tables, ClusterBackupTableInfo{
					Name:   tableName,
					Status: "failed",
					Error:  err.Error(),
				})
				mu.Unlock()
				return nil // Don't fail entire backup for one table
			}

			// Backup all shards for this table.
			// Use errgroup (not the shared pool) to avoid deadlock: the outer
			// group already occupies pool workers, so nesting on the same pool
			// can exhaust all slots when there are many tables.
			shardEg, shardCtx := errgroup.WithContext(ctx)
			shardEg.SetLimit(innerFanOutLimit)
			for shardID := range table.Shards {
				shardEg.Go(func() error {
					if err := t.ln.forwardBackupToShard(shardCtx, shardID, req.Location, req.BackupId); err != nil {
						if !errors.Is(err, context.Canceled) {
							t.logger.Error("Error forwarding backup", zap.String("table", tableName), zap.Error(err))
						}
						return fmt.Errorf("backing up shard %s: %v", shardID, err)
					}
					return nil
				})
			}

			if err := shardEg.Wait(); err != nil {
				mu.Lock()
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  err.Error(),
				}
				clusterMeta.Tables = append(clusterMeta.Tables, ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      err.Error(),
				})
				mu.Unlock()
				return nil // Don't fail entire backup for one table
			}

			// Write table metadata with table-specific backup ID
			tableBackupID := tableName + "-" + req.BackupId
			if err := newBackupStore(req.Location, &t.ln.config.Storage.S3).WriteMetadata(ctx, tableBackupID, table); err != nil {
				mu.Lock()
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  fmt.Sprintf("failed to write metadata: %v", err),
				}
				clusterMeta.Tables = append(clusterMeta.Tables, ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      err.Error(),
				})
				mu.Unlock()
				return nil
			}

			mu.Lock()
			results[i] = TableBackupStatus{
				Name:   tableName,
				Status: TableBackupStatusStatusCompleted,
			}
			clusterMeta.Tables = append(clusterMeta.Tables, ClusterBackupTableInfo{
				Name:           tableName,
				BackupLocation: fmt.Sprintf("%s/%s-metadata.json", req.Location, tableBackupID),
				ShardCount:     len(table.Shards),
				Status:         "completed",
			})
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait() // We handle individual table errors above

	// Write cluster-level metadata
	if strings.HasPrefix(req.Location, "s3://") {
		if err := writeClusterMetadataToBlobStore(ctx, req.Location, req.BackupId, clusterMeta, &t.ln.config.Storage.S3); err != nil {
			errorResponse(w, fmt.Sprintf("Failed to write cluster metadata: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		if err := writeClusterMetadataToFile(ctx, req.Location, req.BackupId, clusterMeta); err != nil {
			errorResponse(w, fmt.Sprintf("Failed to write cluster metadata: %v", err), http.StatusInternalServerError)
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
func (t *TableApi) Restore(w http.ResponseWriter, r *http.Request) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	var req ClusterRestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}

	// Default restore mode
	restoreMode := req.RestoreMode
	if restoreMode == "" {
		restoreMode = ClusterRestoreRequestRestoreModeFailIfExists
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Read cluster backup metadata
	var clusterMeta *ClusterBackupMetadata
	var err error
	if strings.HasPrefix(req.Location, "s3://") {
		clusterMeta, err = readClusterMetadataFromBlobStore(ctx, req.Location, req.BackupId, &t.ln.config.Storage.S3)
	} else {
		clusterMeta, err = readClusterMetadataFromFile(ctx, req.Location, req.BackupId)
	}
	if err != nil {
		errorResponse(w, fmt.Sprintf("Failed to read cluster backup metadata: %v", err), http.StatusInternalServerError)
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

	// Process each table
	results := make([]TableRestoreStatus, len(tablesToRestore))
	var mu sync.Mutex

	g, _ := workerpool.NewGroup(ctx, t.pool)
	for i, tableName := range tablesToRestore {
		g.Go(func(ctx context.Context) error {
			// Check if table exists
			_, err := t.tm.GetTable(tableName)
			tableExists := err == nil

			switch restoreMode {
			case ClusterRestoreRequestRestoreModeFailIfExists:
				if tableExists {
					mu.Lock()
					results[i] = TableRestoreStatus{
						Name:   tableName,
						Status: TableRestoreStatusStatusFailed,
						Error:  "table already exists",
					}
					mu.Unlock()
					return fmt.Errorf("table %s already exists", tableName)
				}
			case ClusterRestoreRequestRestoreModeSkipIfExists:
				if tableExists {
					mu.Lock()
					results[i] = TableRestoreStatus{
						Name:   tableName,
						Status: TableRestoreStatusStatusSkipped,
					}
					mu.Unlock()
					return nil
				}
			case ClusterRestoreRequestRestoreModeOverwrite:
				if tableExists {
					if err := t.tm.RemoveTable(tableName); err != nil {
						mu.Lock()
						results[i] = TableRestoreStatus{
							Name:   tableName,
							Status: TableRestoreStatusStatusFailed,
							Error:  fmt.Sprintf("failed to remove existing table: %v", err),
						}
						mu.Unlock()
						return nil
					}
				}
			}

			// Read table metadata from backup using table-specific backup ID
			tableBackupID := tableName + "-" + req.BackupId
			tableMetadata, err := newBackupStore(req.Location, &t.ln.config.Storage.S3).ReadMetadata(ctx, tableBackupID)
			if err != nil {
				mu.Lock()
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("failed to read backup metadata: %v", err),
				}
				mu.Unlock()
				return nil
			}

			// Verify table name matches
			if tableMetadata.Name != tableName {
				mu.Lock()
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("table name mismatch: expected %s, got %s", tableName, tableMetadata.Name),
				}
				mu.Unlock()
				return nil
			}

			// Restore the table using the base backup ID (shard files use the base backup ID)
			if err := t.tm.RestoreTable(tableMetadata, &common.BackupConfig{
				Location: req.Location,
				BackupID: req.BackupId,
			}); err != nil {
				mu.Lock()
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("failed to restore table: %v", err),
				}
				mu.Unlock()
				return nil
			}

			mu.Lock()
			results[i] = TableRestoreStatus{
				Name:   tableName,
				Status: TableRestoreStatusStatusTriggered,
			}
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		// fail_if_exists mode returns error, but we still want to report status
		if restoreMode == ClusterRestoreRequestRestoreModeFailIfExists {
			errorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
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

	var backups []BackupInfo

	if strings.HasPrefix(location, "s3://") {
		// e.g. "s3://my-bucket-name/optional/prefix"
		bucket, prefix, err := common.ParseS3URL(location)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Invalid location URL: %v", err), http.StatusBadRequest)
			return
		}
		minioClient, err := t.ln.config.Storage.S3.NewMinioClient()
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
				meta, err := readClusterMetadataFromBlobStore(ctx, location, backupID, &t.ln.config.Storage.S3)
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
				})
			}
		}
	} else {
		// File-based listing
		dirPath := strings.TrimPrefix(location, "file://")
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
				meta, err := readClusterMetadataFromFile(ctx, location, backupID)
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
