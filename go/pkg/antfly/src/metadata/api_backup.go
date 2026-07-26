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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	json "github.com/antflydb/antfly/go/pkg/libaf/json"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/multirafthttp"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/workerpool"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tablemgr"
	"github.com/antflydb/antfly/go/pkg/antfly/src/usermgr"
	"github.com/gofrs/flock"
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

func backupArtifactNamesForFormat(
	backupID string,
	table *store.Table,
	format common.BackupFormat,
) []string {
	names := make([]string, 0, len(table.Shards))
	for shardID := range table.Shards {
		name := common.ShardBackupFileName(backupID, shardID)
		if format == common.BackupFormatPortable {
			name = common.ShardPortableBackupFileName(backupID, shardID)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var backupArtifactIdentityChecks = make(chan struct{}, innerFanOutLimit)

func validateBackupMetadataArtifactIdentities(
	ctx context.Context,
	metadataStore backupStore,
	artifactBackupID string,
	metadata *backupMetadata,
) error {
	if metadata == nil || metadata.Table == nil {
		return errors.New("backup metadata is missing table")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(innerFanOutLimit)
	switch metadata.Format {
	case common.BackupFormatNative:
		for _, artifactName := range backupArtifactNamesForFormat(
			artifactBackupID,
			metadata.Table,
			metadata.Format,
		) {
			artifactName := artifactName
			group.Go(func() error {
				select {
				case backupArtifactIdentityChecks <- struct{}{}:
					defer func() { <-backupArtifactIdentityChecks }()
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
				return metadataStore.ValidateArtifact(groupCtx, artifactName)
			})
		}
	case common.BackupFormatPortable:
		if err := common.ValidateBackupID(artifactBackupID); err != nil {
			return err
		}
		artifactsByName := make(
			map[string]common.BackupArtifactIntegrity,
			len(metadata.Artifacts),
		)
		for _, artifact := range metadata.Artifacts {
			artifactsByName[artifact.Name] = artifact
		}
		for shardID := range metadata.Table.Shards {
			expectedName := common.ShardPortableBackupFileName(
				artifactBackupID,
				shardID,
			)
			if _, ok := artifactsByName[expectedName]; !ok {
				return fmt.Errorf(
					"portable backup artifact %q is missing",
					expectedName,
				)
			}
		}
		for _, artifact := range metadata.Artifacts {
			artifact := artifact
			group.Go(func() error {
				select {
				case backupArtifactIdentityChecks <- struct{}{}:
					defer func() { <-backupArtifactIdentityChecks }()
				case <-groupCtx.Done():
					return groupCtx.Err()
				}
				return metadataStore.ValidateArtifactIdentity(groupCtx, artifact)
			})
		}
	default:
		return fmt.Errorf("unsupported backup format %q", metadata.Format)
	}
	return group.Wait()
}

func (t *TableApi) backupShardsWithIntegrity(
	ctx context.Context,
	table *store.Table,
	backup common.BackupConfig,
) ([]common.BackupArtifactIntegrity, error) {
	var mu sync.Mutex
	artifacts := make([]common.BackupArtifactIntegrity, 0, len(table.Shards))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(innerFanOutLimit)
	for shardID := range table.Shards {
		eg.Go(func() error {
			release, err := t.acquireBackupTransfer(egCtx)
			if err != nil {
				return err
			}
			defer release()
			integrity, err := t.ln.forwardBackupToShard(egCtx, shardID, backup)
			if err != nil {
				return fmt.Errorf("backing up shard %s: %w", shardID, err)
			}
			if common.NormalizeBackupFormat(backup.Format) != common.BackupFormatPortable {
				return nil
			}
			expectedName := common.ShardPortableBackupFileName(backup.BackupID, shardID)
			if integrity == nil || integrity.Name != expectedName {
				return fmt.Errorf("shard %s returned an invalid portable artifact identity", shardID)
			}
			mu.Lock()
			artifacts = append(artifacts, *integrity)
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i].Name < artifacts[j].Name
	})
	return artifacts, nil
}

func cleanupBackupAttempt(
	metadataStore backupStore,
	backupID string,
	metadataIDs, artifactNames []string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := cleanupBackupAttemptContents(ctx, metadataStore, metadataIDs, artifactNames); err != nil {
		return err
	}
	return metadataStore.ReleaseBackupID(ctx, backupID)
}

func cleanupBackupAttemptContents(
	ctx context.Context,
	metadataStore backupStore,
	metadataIDs, artifactNames []string,
) error {
	cleanupPhase := func(values []string, cleanup func(context.Context, string) error) error {
		var cleanupGroup errgroup.Group
		cleanupGroup.SetLimit(innerFanOutLimit)
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			value := value
			cleanupGroup.Go(func() error {
				return cleanup(ctx, value)
			})
		}
		return cleanupGroup.Wait()
	}

	// Metadata is the table-level commit record. Remove every published commit
	// before reclaiming payloads so a concurrent reader never observes a
	// manifest whose artifact cleanup has already started.
	if err := cleanupPhase(metadataIDs, metadataStore.DeleteMetadata); err != nil {
		return err
	}
	if err := cleanupPhase(artifactNames, metadataStore.DeleteArtifact); err != nil {
		return err
	}
	return nil
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
	committed := false
	cleanupSafe := true
	createdArtifacts := backupArtifactNamesForFormat(br.BackupId, table, backupConfig.Format)
	defer func() {
		if committed {
			return
		}
		if !cleanupSafe {
			t.logger.Error(
				"Table backup publication outcome is ambiguous; retaining fenced attempt",
				zap.String("backup_id", br.BackupId),
			)
			return
		}
		if err := cleanupBackupAttempt(
			metadataStore,
			br.BackupId,
			nil,
			createdArtifacts,
		); err != nil {
			t.logger.Error("Failed to clean abandoned table backup", zap.String("backup_id", br.BackupId), zap.Error(err))
		}
	}()
	backupConfig.ResolvedLocation = metadataStore.ResolvedLocation()
	artifactIntegrities, err := t.backupShardsWithIntegrity(ctx, table, backupConfig)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Error("Error forwarding backup", zap.Error(err))
		}
		writeBackupError(w, "Failed to forward backup request", err)
		return
	}
	if err := ctx.Err(); err != nil {
		writeBackupError(w, "Backup operation was interrupted", err)
		return
	}

	cleanupSafe = false
	if err := metadataStore.WriteMetadata(
		ctx,
		br.BackupId,
		table,
		backupConfig.Format,
		artifactIntegrities,
	); err != nil {
		cleanupSafe = errors.Is(err, ErrBackupAlreadyExists) ||
			errors.Is(err, ErrBackupMetadataTooLarge)
		writeBackupError(w, "Failed to write backup metadata", err)
		return
	}
	committed = true

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
	metadata, err := metadataStore.ReadMetadata(ctx, rr.BackupId)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Failed to read backup metadata: %v", err), http.StatusInternalServerError)
		return
	}
	tableMetadata := metadata.Table
	backupFormat := metadata.Format

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
	if err := validateBackupMetadataArtifactIdentities(
		ctx,
		metadataStore,
		rr.BackupId,
		metadata,
	); err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Backup artifact validation failed: %v", err),
			http.StatusInternalServerError,
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
	}, metadata.Artifacts); err != nil {
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

func validateBackupTableNames(tableNames []string, maxTables int) error {
	if len(tableNames) > maxTables {
		return fmt.Errorf("at most %d tables may be selected", maxTables)
	}
	seen := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		if strings.TrimSpace(tableName) == "" ||
			len(tableName) > clusterBackupAttemptMaxNameBytes {
			return fmt.Errorf(
				"table names must contain 1 to %d bytes",
				clusterBackupAttemptMaxNameBytes,
			)
		}
		if _, ok := seen[tableName]; ok {
			return fmt.Errorf("table %q is selected more than once", tableName)
		}
		seen[tableName] = struct{}{}
	}
	return nil
}

func writeBackupError(w http.ResponseWriter, message string, err error) {
	detail := sanitizedBackupFailure(err)
	switch {
	case errors.Is(err, ErrBackupAlreadyExists):
		errorResponse(w, message+": "+detail, http.StatusConflict)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		errorResponse(w, message+": "+detail, http.StatusRequestTimeout)
	default:
		errorResponse(w, message+": "+detail, http.StatusInternalServerError)
	}
}

// ClusterBackupMetadata represents the metadata for a cluster-level backup
type ClusterBackupMetadata struct {
	Version             uint32                   `json:"version"`
	State               string                   `json:"state"`
	BackupID            string                   `json:"backup_id"`
	Timestamp           time.Time                `json:"timestamp"`
	AntflyVersion       string                   `json:"antfly_version"`
	Format              common.BackupFormat      `json:"format,omitempty"`
	ExpectedTableCount  int                      `json:"expected_table_count"`
	CompletedTableCount int                      `json:"completed_table_count"`
	Tables              []ClusterBackupTableInfo `json:"tables"`
}

const (
	clusterBackupAttemptVersion      = 1
	clusterBackupAttemptDir          = ".antfly-incomplete"
	clusterBackupAttemptHeadName     = ".antfly-go-backup-attempt-head.json"
	zigClusterBackupAttemptHeadName  = ".antfly-backup-attempt-head.json"
	clusterBackupAttemptHeadVersion  = 1
	clusterBackupAttemptReclaimGrace = clusterBackupAttemptLeaseDuration +
		clusterBackupAttemptLeaseSafetyMargin
	clusterBackupCommitTimeout       = 2 * time.Minute
	clusterBackupAttemptScanLimit    = 64
	clusterBackupHealthScanLimit     = 10_000
	clusterBackupAttemptReclaimLimit = 2
	clusterBackupAttemptMaxTables    = 4096
	clusterBackupAttemptMaxNameBytes = 4096
	clusterBackupExplicitTableLimit  = 256
)

type ClusterBackupAttempt struct {
	Version            uint32              `json:"version"`
	AttemptID          string              `json:"attempt_id"`
	BackupID           string              `json:"backup_id"`
	CreatedAt          time.Time           `json:"created_at"`
	Format             common.BackupFormat `json:"format"`
	ExpectedTableCount int                 `json:"expected_table_count"`
	TableNames         []string            `json:"table_names"`
	MetadataIDs        []string            `json:"metadata_ids"`
	ArtifactNames      []string            `json:"artifact_names"`
}

// ClusterBackupAttemptHead is the compact Go-to-Zig migration authority.
// MarkerSHA256 pins the exact immutable journal body, so Zig never needs to
// scan retained history or depend on provider-specific LIST metadata.
type ClusterBackupAttemptHead struct {
	Version      uint32 `json:"version"`
	Generation   uint64 `json:"generation"`
	AttemptID    string `json:"attempt_id"`
	BackupID     string `json:"backup_id"`
	State        string `json:"state"`
	MarkerSHA256 string `json:"marker_sha256"`
}

const (
	clusterBackupAttemptStateActive    = "active"
	clusterBackupAttemptStateCommitted = "committed"
	clusterBackupAttemptStateFailed    = "failed"
)

func newClusterBackupAttemptID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "afba-" + hex.EncodeToString(entropy[:]), nil
}

func validateClusterBackupAttempt(attempt *ClusterBackupAttempt, expectedID string) error {
	if attempt == nil ||
		attempt.Version != clusterBackupAttemptVersion ||
		attempt.AttemptID != expectedID ||
		attempt.ExpectedTableCount <= 0 ||
		attempt.ExpectedTableCount > clusterBackupAttemptMaxTables ||
		attempt.ExpectedTableCount != len(attempt.TableNames) {
		return errors.New("invalid cluster backup attempt marker")
	}
	if err := common.ValidateBackupID(attempt.AttemptID); err != nil {
		return err
	}
	if err := common.ValidateBackupID(attempt.BackupID); err != nil {
		return err
	}
	if attempt.AttemptID == attempt.BackupID ||
		attempt.CreatedAt.IsZero() ||
		len(attempt.MetadataIDs) != attempt.ExpectedTableCount {
		return errors.New("invalid cluster backup attempt marker")
	}
	if attempt.Format != common.BackupFormatNative &&
		attempt.Format != common.BackupFormatPortable {
		return errors.New("invalid cluster backup attempt format")
	}
	tableNames := make(map[string]struct{}, len(attempt.TableNames))
	identities := map[string]struct{}{
		attempt.AttemptID: {},
		attempt.BackupID:  {},
	}
	for _, tableName := range attempt.TableNames {
		if strings.TrimSpace(tableName) == "" || len(tableName) > clusterBackupAttemptMaxNameBytes {
			return errors.New("invalid table name in cluster backup attempt")
		}
		if _, exists := tableNames[tableName]; exists {
			return fmt.Errorf("table %q is selected more than once", tableName)
		}
		tableNames[tableName] = struct{}{}
	}
	for _, metadataID := range attempt.MetadataIDs {
		if err := common.ValidateBackupID(metadataID); err != nil {
			return err
		}
		if _, exists := identities[metadataID]; exists {
			return errors.New("duplicate identifier in cluster backup attempt")
		}
		identities[metadataID] = struct{}{}
	}
	for _, artifactName := range attempt.ArtifactNames {
		if artifactName == "" ||
			len(artifactName) > clusterBackupAttemptMaxNameBytes ||
			path.Base(artifactName) != artifactName {
			return fmt.Errorf("invalid backup artifact name %q", artifactName)
		}
		if _, exists := identities[artifactName]; exists {
			return errors.New("duplicate identifier in cluster backup attempt")
		}
		identities[artifactName] = struct{}{}
	}
	return nil
}

const (
	clusterBackupMetadataVersion = 2
	clusterBackupStateComplete   = "complete"
)

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
	if meta.State != clusterBackupStateComplete {
		return fmt.Errorf("cluster backup %q is not complete", id)
	}
	if meta.ExpectedTableCount == 0 ||
		meta.ExpectedTableCount > clusterBackupAttemptMaxTables ||
		meta.ExpectedTableCount != len(meta.Tables) ||
		meta.CompletedTableCount != meta.ExpectedTableCount {
		return fmt.Errorf(
			"cluster backup %q has incomplete table coverage: expected %d, completed %d, recorded %d",
			id,
			meta.ExpectedTableCount,
			meta.CompletedTableCount,
			len(meta.Tables),
		)
	}
	tableNames := make(map[string]struct{}, len(meta.Tables))
	backupLocations := make(map[string]struct{}, len(meta.Tables))
	for _, table := range meta.Tables {
		if strings.TrimSpace(table.Name) == "" ||
			len(table.Name) > clusterBackupAttemptMaxNameBytes ||
			table.Status != "completed" ||
			strings.TrimSpace(table.BackupLocation) == "" ||
			table.Error != "" {
			return fmt.Errorf("cluster backup %q contains an incomplete table entry", id)
		}
		if _, exists := tableNames[table.Name]; exists {
			return fmt.Errorf("cluster backup %q contains duplicate table %q", id, table.Name)
		}
		if _, exists := backupLocations[table.BackupLocation]; exists {
			return fmt.Errorf("cluster backup %q contains duplicate table backup location", id)
		}
		tableNames[table.Name] = struct{}{}
		backupLocations[table.BackupLocation] = struct{}{}
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

func clusterAttemptObjectKey(prefix, attemptID string) string {
	key := path.Join(clusterBackupAttemptDir, attemptID+".json")
	if prefix != "" {
		key = path.Join(prefix, key)
	}
	return key
}

func encodeClusterBackupAttempt(attempt *ClusterBackupAttempt) ([]byte, error) {
	var body bytes.Buffer
	writer := &boundedWriter{writer: &body, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(attempt); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func writeClusterBackupAttempt(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attempt *ClusterBackupAttempt,
) ([sha256.Size]byte, error) {
	if err := validateClusterBackupAttempt(attempt, attempt.AttemptID); err != nil {
		return [sha256.Size]byte{}, err
	}
	body, err := encodeClusterBackupAttempt(attempt)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.Sum256(body)
	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		options := minio.PutObjectOptions{ContentType: "application/json"}
		options.SetMatchETagExcept("*")
		_, err = client.PutObject(
			ctx,
			s3Info.Bucket,
			clusterAttemptObjectKey(s3Info.Prefix, attempt.AttemptID),
			bytes.NewReader(body),
			int64(len(body)),
			options,
		)
		return digest, err
	}
	root := strings.TrimPrefix(resolvedLocation, "file://")
	attemptDir := filepath.Join(root, clusterBackupAttemptDir)
	if err := os.MkdirAll(attemptDir, 0o750); err != nil {
		return [sha256.Size]byte{}, err
	}
	if err := writeBytesFileAtomically(
		ctx,
		filepath.Join(attemptDir, attempt.AttemptID+".json"),
		body,
		false,
	); err != nil {
		return digest, err
	}
	return digest, nil
}

var errInvalidClusterBackupAttemptHead = errors.New("invalid cluster backup attempt head")

func validateClusterBackupAttemptHead(head *ClusterBackupAttemptHead) error {
	if head == nil ||
		head.Version != clusterBackupAttemptHeadVersion ||
		head.Generation == 0 ||
		head.MarkerSHA256 == "" {
		return errInvalidClusterBackupAttemptHead
	}
	if err := common.ValidateBackupID(head.AttemptID); err != nil {
		return fmt.Errorf("%w: invalid attempt ID", errInvalidClusterBackupAttemptHead)
	}
	if err := common.ValidateBackupID(head.BackupID); err != nil {
		return fmt.Errorf("%w: invalid backup ID", errInvalidClusterBackupAttemptHead)
	}
	switch head.State {
	case clusterBackupAttemptStateActive,
		clusterBackupAttemptStateCommitted,
		clusterBackupAttemptStateFailed:
	default:
		return fmt.Errorf("%w: invalid state", errInvalidClusterBackupAttemptHead)
	}
	digest, err := hex.DecodeString(head.MarkerSHA256)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != head.MarkerSHA256 {
		return fmt.Errorf("%w: invalid marker digest", errInvalidClusterBackupAttemptHead)
	}
	return nil
}

func encodeClusterBackupAttemptHead(head *ClusterBackupAttemptHead) ([]byte, error) {
	if err := validateClusterBackupAttemptHead(head); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	writer := &boundedWriter{writer: &body, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(head); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func decodeClusterBackupAttemptHead(body []byte) (*ClusterBackupAttemptHead, error) {
	var head ClusterBackupAttemptHead
	decoder := stdjson.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&head); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidClusterBackupAttemptHead, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing data", errInvalidClusterBackupAttemptHead)
	}
	if err := validateClusterBackupAttemptHead(&head); err != nil {
		return nil, err
	}
	return &head, nil
}

func nextClusterBackupAttemptHeadGeneration(previous *ClusterBackupAttemptHead) (uint64, error) {
	if previous == nil {
		return 1, nil
	}
	if previous.Generation == ^uint64(0) {
		return 0, errors.New("cluster backup attempt head generation exhausted")
	}
	return previous.Generation + 1, nil
}

func readClusterBackupAttemptHeadFile(pathname string) (*ClusterBackupAttemptHead, error) {
	file, err := os.Open(filepath.Clean(pathname))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()
	body, err := readBackupMetadata(file)
	if err != nil {
		return nil, err
	}
	return decodeClusterBackupAttemptHead(body)
}

// ensureZigClusterBackupAttemptHeadAbsent prevents a downgraded Go producer
// from publishing into a repository whose ordering authority has already
// migrated to Zig. A concurrent first Zig publication is still safe: Zig's
// head has precedence during restore admission and permanently fences later Go
// backup attempts.
func ensureZigClusterBackupAttemptHeadAbsent(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
) error {
	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return err
		}
		_, err = client.StatObject(
			ctx,
			s3Info.Bucket,
			path.Join(s3Info.Prefix, zigClusterBackupAttemptHeadName),
			minio.StatObjectOptions{},
		)
		if err == nil {
			return errors.New("backup repository is owned by a newer producer")
		}
		if isS3ObjectNotFound(err) {
			return nil
		}
		return err
	}
	headPath := filepath.Join(
		strings.TrimPrefix(resolvedLocation, "file://"),
		zigClusterBackupAttemptHeadName,
	)
	_, err := os.Stat(filepath.Clean(headPath))
	if err == nil {
		return errors.New("backup repository is owned by a newer producer")
	}
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func readClusterBackupAttemptHeadObject(
	ctx context.Context,
	client *minio.Client,
	bucket, objectKey string,
) (*ClusterBackupAttemptHead, string, error) {
	info, err := client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if isS3ObjectNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if info.Size > maxBackupMetadataBytes || info.ETag == "" {
		return nil, "", fmt.Errorf(
			"%w: invalid object identity",
			errInvalidClusterBackupAttemptHead,
		)
	}
	options := minio.GetObjectOptions{}
	if err := options.SetMatchETag(info.ETag); err != nil {
		return nil, "", err
	}
	object, err := client.GetObject(ctx, bucket, objectKey, options)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = object.Close() }()
	body, err := readBackupMetadata(object)
	if err != nil {
		return nil, "", err
	}
	head, err := decodeClusterBackupAttemptHead(body)
	return head, info.ETag, err
}

func clusterBackupAttemptMarkerPublicationMatches(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	expectedDigest [sha256.Size]byte,
) (bool, error) {
	var body []byte
	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return false, err
		}
		object, err := client.GetObject(
			ctx,
			s3Info.Bucket,
			clusterAttemptObjectKey(s3Info.Prefix, attemptID),
			minio.GetObjectOptions{},
		)
		if err != nil {
			if isS3ObjectNotFound(err) {
				return false, nil
			}
			return false, err
		}
		defer func() { _ = object.Close() }()
		body, err = readBackupMetadata(object)
		if err != nil {
			if isS3ObjectNotFound(err) {
				return false, nil
			}
			return false, err
		}
	} else {
		pathname := filepath.Join(
			strings.TrimPrefix(resolvedLocation, "file://"),
			clusterBackupAttemptDir,
			attemptID+".json",
		)
		file, err := os.Open(filepath.Clean(pathname))
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		defer func() { _ = file.Close() }()
		body, err = readBackupMetadata(file)
		if err != nil {
			return false, err
		}
	}
	return sha256.Sum256(body) == expectedDigest, nil
}

func clusterBackupAttemptHeadPublicationMatches(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	expected ClusterBackupAttemptHead,
) (bool, error) {
	var (
		current *ClusterBackupAttemptHead
		err     error
	)
	if s3Info != nil {
		client, clientErr := s3Info.EnsureBucket(ctx)
		if clientErr != nil {
			return false, clientErr
		}
		current, _, err = readClusterBackupAttemptHeadObject(
			ctx,
			client,
			s3Info.Bucket,
			path.Join(s3Info.Prefix, clusterBackupAttemptHeadName),
		)
	} else {
		current, err = readClusterBackupAttemptHeadFile(filepath.Join(
			strings.TrimPrefix(resolvedLocation, "file://"),
			clusterBackupAttemptHeadName,
		))
	}
	if errors.Is(err, errInvalidClusterBackupAttemptHead) {
		// The producer only publishes a validated head atomically, so malformed
		// bytes cannot be the uncertain result of this attempted publication.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return current != nil &&
		current.AttemptID == expected.AttemptID &&
		current.BackupID == expected.BackupID &&
		current.State == clusterBackupAttemptStateActive &&
		current.MarkerSHA256 == expected.MarkerSHA256, nil
}

func publishClusterBackupAttemptHead(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	head ClusterBackupAttemptHead,
) (*ClusterBackupAttemptHead, error) {
	head.Version = clusterBackupAttemptHeadVersion
	head.State = clusterBackupAttemptStateActive
	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return nil, err
		}
		objectKey := path.Join(s3Info.Prefix, clusterBackupAttemptHeadName)
		for retry := 0; retry < 16; retry++ {
			previous, etag, err := readClusterBackupAttemptHeadObject(
				ctx,
				client,
				s3Info.Bucket,
				objectKey,
			)
			if err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				return nil, err
			}
			head.Generation, err = nextClusterBackupAttemptHeadGeneration(previous)
			if err != nil {
				return nil, err
			}
			body, err := encodeClusterBackupAttemptHead(&head)
			if err != nil {
				return nil, err
			}
			options := minio.PutObjectOptions{ContentType: "application/json"}
			if previous == nil {
				options.SetMatchETagExcept("*")
			} else {
				options.SetMatchETag(etag)
			}
			if _, err := client.PutObject(
				ctx,
				s3Info.Bucket,
				objectKey,
				bytes.NewReader(body),
				int64(len(body)),
				options,
			); err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				// Preserve the observed predecessor so an ambiguous successful
				// publication can still compact the journal it superseded.
				return previous, err
			}
			return previous, nil
		}
		return nil, errors.New("cluster backup attempt head publication conflict")
	}

	root := strings.TrimPrefix(resolvedLocation, "file://")
	headPath := filepath.Join(root, clusterBackupAttemptHeadName)
	headLock := flock.New(headPath + ".publish.lock")
	locked, err := headLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errors.New("cluster backup attempt head lock unavailable")
	}
	defer func() { _ = headLock.Close() }()
	previous, err := readClusterBackupAttemptHeadFile(headPath)
	if err != nil {
		return nil, err
	}
	head.Generation, err = nextClusterBackupAttemptHeadGeneration(previous)
	if err != nil {
		return nil, err
	}
	body, err := encodeClusterBackupAttemptHead(&head)
	if err != nil {
		return nil, err
	}
	if err := writeBytesFileAtomically(ctx, headPath, body, true); err != nil {
		// Atomic rename may have completed before a directory sync failure.
		// The caller reconciles the published head and still needs this value
		// to retire the predecessor without scanning the journal directory.
		return previous, err
	}
	return previous, nil
}

func transitionClusterBackupAttemptHead(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID, targetState string,
) (bool, error) {
	if targetState != clusterBackupAttemptStateCommitted &&
		targetState != clusterBackupAttemptStateFailed {
		return false, errors.New("invalid cluster backup attempt head transition")
	}
	update := func(current *ClusterBackupAttemptHead) (*ClusterBackupAttemptHead, bool, error) {
		if current == nil || current.AttemptID != attemptID {
			return nil, false, nil
		}
		if current.State == targetState {
			return current, true, nil
		}
		if current.State != clusterBackupAttemptStateActive {
			return nil, false, nil
		}
		next := *current
		var err error
		next.Generation, err = nextClusterBackupAttemptHeadGeneration(current)
		if err != nil {
			return nil, false, err
		}
		next.State = targetState
		return &next, true, nil
	}

	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return false, err
		}
		objectKey := path.Join(s3Info.Prefix, clusterBackupAttemptHeadName)
		for retry := 0; retry < 16; retry++ {
			current, etag, err := readClusterBackupAttemptHeadObject(
				ctx,
				client,
				s3Info.Bucket,
				objectKey,
			)
			if err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				return false, err
			}
			next, owned, err := update(current)
			if err != nil || !owned {
				return owned, err
			}
			if current.State == targetState {
				return true, nil
			}
			body, err := encodeClusterBackupAttemptHead(next)
			if err != nil {
				return false, err
			}
			options := minio.PutObjectOptions{ContentType: "application/json"}
			options.SetMatchETag(etag)
			if _, err := client.PutObject(
				ctx,
				s3Info.Bucket,
				objectKey,
				bytes.NewReader(body),
				int64(len(body)),
				options,
			); err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				return false, err
			}
			return true, nil
		}
		return false, errors.New("cluster backup attempt head transition conflict")
	}

	root := strings.TrimPrefix(resolvedLocation, "file://")
	headPath := filepath.Join(root, clusterBackupAttemptHeadName)
	headLock := flock.New(headPath + ".publish.lock")
	locked, err := headLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return false, err
	}
	if !locked {
		return false, errors.New("cluster backup attempt head lock unavailable")
	}
	defer func() { _ = headLock.Close() }()
	current, err := readClusterBackupAttemptHeadFile(headPath)
	if err != nil {
		return false, err
	}
	next, owned, err := update(current)
	if err != nil || !owned {
		return owned, err
	}
	if current.State == targetState {
		return true, nil
	}
	body, err := encodeClusterBackupAttemptHead(next)
	if err != nil {
		return false, err
	}
	if err := writeBytesFileAtomically(ctx, headPath, body, true); err != nil {
		return false, err
	}
	return true, nil
}

func compactSupersededClusterBackupAttempt(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	previous *ClusterBackupAttemptHead,
	currentAttemptID string,
) error {
	if previous == nil ||
		previous.AttemptID == currentAttemptID ||
		previous.State == clusterBackupAttemptStateActive {
		return nil
	}
	return deleteClusterBackupAttempt(
		ctx,
		resolvedLocation,
		s3Info,
		previous.AttemptID,
	)
}

func compactClusterBackupAttemptIfSuperseded(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	headOwned bool,
) error {
	if headOwned {
		return nil
	}
	return deleteClusterBackupAttempt(ctx, resolvedLocation, s3Info, attemptID)
}

func readClusterBackupAttemptFile(pathname, attemptID string) (*ClusterBackupAttempt, error) {
	file, err := os.Open(filepath.Clean(pathname))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := readBackupMetadata(file)
	if err != nil {
		return nil, err
	}
	var attempt ClusterBackupAttempt
	if err := json.Unmarshal(data, &attempt); err != nil {
		return nil, err
	}
	if err := validateClusterBackupAttempt(&attempt, attemptID); err != nil {
		return nil, err
	}
	return &attempt, nil
}

func deleteClusterBackupAttempt(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
) error {
	if s3Info != nil {
		client, err := s3Info.EnsureBucket(ctx)
		if err != nil {
			return err
		}
		return client.RemoveObject(
			ctx,
			s3Info.Bucket,
			clusterAttemptObjectKey(s3Info.Prefix, attemptID),
			minio.RemoveObjectOptions{},
		)
	}
	pathname := filepath.Join(
		strings.TrimPrefix(resolvedLocation, "file://"),
		clusterBackupAttemptDir,
		attemptID+".json",
	)
	return removeFileAndSyncDirectory(ctx, pathname)
}

func reclaimStaleClusterBackupAttempt(
	metadataStore backupStore,
	resolvedLocation string,
	s3Info *common.S3Info,
	attempt *ClusterBackupAttempt,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	availabilityErr := ensureClusterMetadataAbsent(
		ctx,
		resolvedLocation,
		attempt.BackupID,
		s3Info,
	)
	switch {
	case errors.Is(availabilityErr, ErrBackupAlreadyExists):
		// Retain committed attempt journals. They are the durable ordering
		// authority used to detect a newest malformed or corrupt aggregate;
		// deleting the newest journal before a replacement is published would
		// permit an older backup to mask current health.
		return false, nil
	case availabilityErr == nil:
		claimed, err := claimExpiredClusterBackupAttemptLease(
			ctx,
			resolvedLocation,
			s3Info,
			attempt.AttemptID,
			time.Now().UTC(),
		)
		if err != nil || !claimed {
			return false, err
		}
		head, err := currentClusterBackupAttemptHead(
			ctx,
			resolvedLocation,
			s3Info,
		)
		if err != nil {
			return false, err
		}
		retainJournal := false
		if head != nil && head.AttemptID == attempt.AttemptID {
			switch head.State {
			case clusterBackupAttemptStateCommitted:
				// A committed head remains the repository's durable health
				// authority even if its aggregate was externally removed.
				// Never turn that corruption into an apparently empty slot.
				if err := deleteClusterBackupAttemptLease(
					ctx,
					resolvedLocation,
					s3Info,
					attempt.AttemptID,
				); err != nil {
					return false, err
				}
				return false, nil
			case clusterBackupAttemptStateFailed:
				retainJournal = true
			case clusterBackupAttemptStateActive:
				owned, err := transitionClusterBackupAttemptHead(
					ctx,
					resolvedLocation,
					s3Info,
					attempt.AttemptID,
					clusterBackupAttemptStateFailed,
				)
				if err != nil {
					return false, err
				}
				retainJournal = owned
			}
		}
		// Claiming the expired lease fences a paused or restarted owner before
		// removing commit records, artifacts, and finally the reservation.
		// A reclaiming lease is durable, so a later scan can safely retry an
		// interrupted cleanup without reviving the producer.
		if err := cleanupBackupAttemptContents(
			ctx,
			metadataStore,
			attempt.MetadataIDs,
			attempt.ArtifactNames,
		); err != nil {
			return false, err
		}
		if err := metadataStore.ReleaseBackupID(ctx, attempt.BackupID); err != nil {
			return false, err
		}
		if !retainJournal {
			if err := deleteClusterBackupAttempt(
				ctx,
				resolvedLocation,
				s3Info,
				attempt.AttemptID,
			); err != nil {
				return false, err
			}
		}
		if err := deleteClusterBackupAttemptLease(
			ctx,
			resolvedLocation,
			s3Info,
			attempt.AttemptID,
		); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, availabilityErr
	}
}

func currentClusterBackupAttemptHead(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
) (*ClusterBackupAttemptHead, error) {
	if s3Info != nil {
		client, err := s3Info.NewMinioClient()
		if err != nil {
			return nil, err
		}
		head, _, err := readClusterBackupAttemptHeadObject(
			ctx,
			client,
			s3Info.Bucket,
			path.Join(s3Info.Prefix, clusterBackupAttemptHeadName),
		)
		return head, err
	}
	return readClusterBackupAttemptHeadFile(filepath.Join(
		strings.TrimPrefix(resolvedLocation, "file://"),
		clusterBackupAttemptHeadName,
	))
}

func latestClusterBackupAttempt(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	metadataStore backupStore,
	scanLimit int,
	strict bool,
) (*ClusterBackupAttempt, error) {
	var latest *ClusterBackupAttempt
	scanned := 0
	reclaimed := 0
	consider := func(attempt *ClusterBackupAttempt) {
		if latest == nil ||
			attempt.CreatedAt.After(latest.CreatedAt) ||
			(attempt.CreatedAt.Equal(latest.CreatedAt) && attempt.AttemptID > latest.AttemptID) {
			latest = attempt
		}
	}
	if s3Info != nil {
		client, err := s3Info.NewMinioClient()
		if err != nil {
			return nil, err
		}
		prefix := path.Join(s3Info.Prefix, clusterBackupAttemptDir) + "/"
		scanCtx, scanCancel := context.WithCancel(ctx)
		defer scanCancel()
		objectCh := client.ListObjects(scanCtx, s3Info.Bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
			MaxKeys:   scanLimit + 1,
		})
		for object := range objectCh {
			if scanned >= scanLimit {
				scanCancel()
				if strict {
					return nil, errors.New("cluster backup attempt health scan limit exceeded")
				}
				break
			}
			if object.Err != nil {
				return nil, object.Err
			}
			base := path.Base(object.Key)
			attemptID, ok := strings.CutSuffix(base, ".json")
			if !ok {
				continue
			}
			if strings.TrimPrefix(object.Key, prefix) != base {
				if strict {
					return nil, errors.New("invalid nested cluster backup attempt marker")
				}
				continue
			}
			if err := common.ValidateBackupID(attemptID); err != nil {
				if strict {
					return nil, errors.New("invalid cluster backup attempt marker name")
				}
				continue
			}
			scanned++
			reader, err := client.GetObject(
				ctx,
				s3Info.Bucket,
				object.Key,
				minio.GetObjectOptions{},
			)
			if err != nil {
				if strict {
					return nil, err
				}
				continue
			}
			data, readErr := readBackupMetadata(reader)
			closeErr := reader.Close()
			if readErr != nil {
				if strict {
					return nil, readErr
				}
				continue
			}
			if closeErr != nil {
				if strict {
					return nil, closeErr
				}
				continue
			}
			var attempt ClusterBackupAttempt
			if err := json.Unmarshal(data, &attempt); err != nil {
				if strict {
					return nil, err
				}
				continue
			}
			if err := validateClusterBackupAttempt(&attempt, attemptID); err != nil {
				if strict {
					return nil, err
				}
				continue
			}
			if !strict && time.Since(attempt.CreatedAt) >= clusterBackupAttemptReclaimGrace {
				if reclaimed >= clusterBackupAttemptReclaimLimit {
					continue
				}
				{
					didReclaim, err := reclaimStaleClusterBackupAttempt(
						metadataStore,
						resolvedLocation,
						s3Info,
						&attempt,
					)
					if err != nil {
						return nil, err
					}
					if didReclaim {
						reclaimed++
					}
				}
				continue
			}
			consider(&attempt)
		}
		return latest, nil
	}

	attemptDir := filepath.Join(
		strings.TrimPrefix(resolvedLocation, "file://"),
		clusterBackupAttemptDir,
	)
	dir, err := os.Open(attemptDir) //#nosec G304 -- resolved backup root is policy-validated
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	const directoryBatchSize = 64
	scanComplete := false
	for !scanComplete {
		entries, readErr := dir.ReadDir(directoryBatchSize)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}
		scanComplete = errors.Is(readErr, io.EOF)
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			attemptID, ok := strings.CutSuffix(entry.Name(), ".json")
			if !ok {
				continue
			}
			if scanned >= scanLimit {
				if strict {
					return nil, errors.New("cluster backup attempt health scan limit exceeded")
				}
				return latest, nil
			}
			scanned++
			attempt, err := readClusterBackupAttemptFile(
				filepath.Join(attemptDir, entry.Name()),
				attemptID,
			)
			if err != nil {
				if strict {
					return nil, err
				}
				continue
			}
			if !strict && time.Since(attempt.CreatedAt) >= clusterBackupAttemptReclaimGrace {
				if reclaimed >= clusterBackupAttemptReclaimLimit {
					continue
				}
				{
					didReclaim, err := reclaimStaleClusterBackupAttempt(
						metadataStore,
						resolvedLocation,
						s3Info,
						attempt,
					)
					if err != nil {
						return nil, err
					}
					if didReclaim {
						reclaimed++
					}
				}
				continue
			}
			consider(attempt)
		}
	}
	return latest, nil
}

func validateClusterBackupArtifacts(
	ctx context.Context,
	metadataStore backupStore,
	meta *ClusterBackupMetadata,
) error {
	if err := validateClusterBackupMetadata(meta.BackupID, meta); err != nil {
		return err
	}
	var group errgroup.Group
	group.SetLimit(innerFanOutLimit)
	for _, tableInfo := range meta.Tables {
		tableInfo := tableInfo
		group.Go(func() error {
			metadataID := tableBackupMetadataID(tableInfo.Name, meta.BackupID)
			metadata, err := metadataStore.ReadMetadata(ctx, metadataID)
			if err != nil {
				return err
			}
			if metadata.Format != meta.Format || metadata.Table.Name != tableInfo.Name {
				return errors.New("cluster backup table metadata mismatch")
			}
			if err := validateBackupMetadataArtifactIdentities(
				ctx,
				metadataStore,
				meta.BackupID,
				metadata,
			); err != nil {
				return err
			}
			return nil
		})
	}
	return group.Wait()
}

func validateNewestClusterBackupAttempt(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	metadataStore backupStore,
	attempt *ClusterBackupAttempt,
) error {
	if attempt == nil {
		return nil
	}
	var (
		meta *ClusterBackupMetadata
		err  error
	)
	if s3Info != nil {
		meta, err = readClusterMetadataFromBlobStore(ctx, attempt.BackupID, s3Info)
	} else {
		meta, err = readClusterMetadataFromFile(ctx, resolvedLocation, attempt.BackupID)
	}
	if err != nil {
		return fmt.Errorf("newest cluster backup attempt is not committed: %w", err)
	}
	if meta.Version != clusterBackupMetadataVersion ||
		meta.ExpectedTableCount != attempt.ExpectedTableCount ||
		meta.Format != attempt.Format {
		return errors.New("newest cluster backup attempt does not match its commit")
	}
	expectedTables := make(map[string]string, len(attempt.TableNames))
	for i, tableName := range attempt.TableNames {
		expectedTables[tableName] = attempt.MetadataIDs[i]
	}
	for _, tableInfo := range meta.Tables {
		metadataID, ok := expectedTables[tableInfo.Name]
		if !ok || metadataID != tableBackupMetadataID(tableInfo.Name, attempt.BackupID) {
			return errors.New("newest cluster backup attempt table set mismatch")
		}
		delete(expectedTables, tableInfo.Name)
	}
	if len(expectedTables) != 0 {
		return errors.New("newest cluster backup attempt table set is incomplete")
	}
	if err := validateClusterBackupArtifacts(ctx, metadataStore, meta); err != nil {
		return fmt.Errorf("newest cluster backup attempt is not restorable: %w", err)
	}
	for _, artifactName := range attempt.ArtifactNames {
		if err := metadataStore.ValidateArtifact(ctx, artifactName); err != nil {
			return fmt.Errorf("newest cluster backup attempt artifact is unavailable: %w", err)
		}
	}
	return nil
}

func sanitizedBackupFailure(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "backup canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "backup deadline exceeded"
	case errors.Is(err, ErrBackupAlreadyExists):
		return "backup identifier is unavailable"
	case errors.Is(err, ErrBackupMetadataTooLarge):
		return "backup metadata exceeds the size limit"
	default:
		return "backup failed"
	}
}

// Backup backs up all tables or selected tables
func (t *TableApi) Backup(w http.ResponseWriter, r *http.Request) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeAdmin) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	var rawRequest json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&rawRequest); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}
	var req ClusterBackupRequest
	if err := json.Unmarshal(rawRequest, &req); err != nil {
		errorResponse(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}
	var requestFields map[string]json.RawMessage
	if err := json.Unmarshal(rawRequest, &requestFields); err != nil {
		errorResponse(w, "Failed to parse request", http.StatusBadRequest)
		return
	}
	if _, provided := requestFields["table_names"]; provided && len(req.TableNames) == 0 {
		errorResponse(w, "No tables to backup", http.StatusBadRequest)
		return
	}
	if err := common.ValidateBackupID(req.BackupId); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid backup ID: %v", err), http.StatusBadRequest)
		return
	}
	if len(req.TableNames) > 0 {
		if err := validateBackupTableNames(req.TableNames, clusterBackupExplicitTableLimit); err != nil {
			errorResponse(w, fmt.Sprintf("Invalid table selection: %v", err), http.StatusBadRequest)
			return
		}
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
	if err := ensureZigClusterBackupAttemptHeadAbsent(
		ctx,
		resolvedLocation,
		s3Info,
	); err != nil {
		t.logger.Warn(
			"Refusing Go backup into a repository owned by a newer producer",
			zap.Error(err),
		)
		errorResponse(
			w,
			"Backup repository requires a newer Antfly producer",
			http.StatusConflict,
		)
		return
	}
	if _, err := latestClusterBackupAttempt(
		ctx,
		resolvedLocation,
		s3Info,
		metadataStore,
		clusterBackupAttemptScanLimit,
		false,
	); err != nil {
		// Reclamation is bounded maintenance. It must not make new, unrelated
		// backup IDs unavailable when the repository itself is otherwise
		// writable.
		t.logger.Warn("Deferred stale Go backup attempt reclamation", zap.Error(err))
	}

	// Get list of tables to backup
	var tableNames []string
	if len(req.TableNames) > 0 {
		tableNames = req.TableNames
	} else {
		// Backup all tables
		tables, err := t.tm.Tables(nil, nil)
		if err != nil {
			t.logger.Error("Failed to list tables for backup", zap.String("class", sanitizedBackupFailure(err)))
			errorResponse(w, "Failed to list tables for backup", http.StatusInternalServerError)
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
	if err := validateBackupTableNames(tableNames, clusterBackupAttemptMaxTables); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid table selection: %v", err), http.StatusBadRequest)
		return
	}
	plannedTables := make([]*store.Table, len(tableNames))
	cleanupMetadataIDs := make([]string, len(tableNames))
	cleanupArtifactsByTable := make([][]string, len(tableNames))
	var allArtifactNames []string
	for i, tableName := range tableNames {
		cleanupMetadataIDs[i] = tableBackupMetadataID(tableName, req.BackupId)
		table, tableErr := t.tm.GetTable(tableName)
		if tableErr != nil {
			continue
		}
		plannedTables[i] = table
		cleanupArtifactsByTable[i] = backupArtifactNamesForFormat(
			req.BackupId,
			table,
			backupConfig.Format,
		)
		allArtifactNames = append(allArtifactNames, cleanupArtifactsByTable[i]...)
	}
	if err := ensureClusterMetadataAbsent(ctx, resolvedLocation, req.BackupId, s3Info); err != nil {
		writeBackupError(w, "Backup ID is not available", err)
		return
	}
	if err := metadataStore.ReserveBackupID(ctx, req.BackupId); err != nil {
		writeBackupError(w, "Backup ID is not available", err)
		return
	}
	attemptID, err := newClusterBackupAttemptID()
	if err != nil {
		_ = metadataStore.ReleaseBackupID(context.Background(), req.BackupId)
		errorResponse(w, "Failed to initialize backup attempt", http.StatusInternalServerError)
		return
	}
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          attemptID,
		BackupID:           req.BackupId,
		CreatedAt:          time.Now().UTC(),
		Format:             backupConfig.Format,
		ExpectedTableCount: len(tableNames),
		TableNames:         append([]string(nil), tableNames...),
		MetadataIDs:        append([]string(nil), cleanupMetadataIDs...),
		ArtifactNames:      append([]string(nil), allArtifactNames...),
	}
	markerDigest, err := writeClusterBackupAttempt(ctx, resolvedLocation, s3Info, attempt)
	if err != nil {
		reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 30*time.Second)
		published, reconcileErr := clusterBackupAttemptMarkerPublicationMatches(
			reconcileCtx,
			resolvedLocation,
			s3Info,
			attemptID,
			markerDigest,
		)
		reconcileCancel()
		if !published {
			if reconcileErr == nil {
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if releaseErr := metadataStore.ReleaseBackupID(
					releaseCtx,
					req.BackupId,
				); releaseErr != nil {
					t.logger.Error(
						"Failed to release backup reservation after definitive marker failure",
						zap.String("backup_id", req.BackupId),
						zap.Error(releaseErr),
					)
				}
				releaseCancel()
			} else {
				t.logger.Error(
					"Cluster backup attempt marker outcome remains ambiguous; retaining reservation",
					zap.String("backup_id", req.BackupId),
					zap.String("attempt_id", attemptID),
					zap.Error(reconcileErr),
				)
			}
			writeBackupError(w, "Failed to publish backup attempt marker", err)
			return
		}
		t.logger.Warn(
			"Recovered cluster backup attempt marker after an ambiguous publication response",
			zap.String("backup_id", req.BackupId),
			zap.String("attempt_id", attemptID),
			zap.Error(err),
		)
	}
	committed := false
	cleanupSafe := true
	headPublished := false
	var attemptLease *clusterBackupAttemptLeaseController
	cleanupMetadataPublished := make([]bool, len(tableNames))
	defer func() {
		attemptLease.Stop()
		if committed {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := deleteClusterBackupAttemptLease(
				cleanupCtx,
				resolvedLocation,
				s3Info,
				attemptID,
			); err != nil {
				t.logger.Warn(
					"Committed backup attempt lease cleanup deferred",
					zap.String("attempt_id", attemptID),
					zap.Error(err),
				)
			}
			cleanupCancel()
			return
		}
		if !cleanupSafe {
			t.logger.Error(
				"Cluster backup publication outcome is ambiguous; retaining fenced attempt",
				zap.String("backup_id", req.BackupId),
			)
			return
		}
		var publishedMetadataIDs []string
		for i, metadataID := range cleanupMetadataIDs {
			if cleanupMetadataPublished[i] {
				publishedMetadataIDs = append(publishedMetadataIDs, metadataID)
			}
		}
		var cleanupArtifacts []string
		for _, names := range cleanupArtifactsByTable {
			cleanupArtifacts = append(cleanupArtifacts, names...)
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if err := cleanupBackupAttemptContents(
			cleanupCtx,
			metadataStore,
			publishedMetadataIDs,
			cleanupArtifacts,
		); err != nil {
			t.logger.Error("Failed to clean abandoned cluster backup", zap.String("backup_id", req.BackupId), zap.Error(err))
			return
		}
		if err := metadataStore.ReleaseBackupID(cleanupCtx, req.BackupId); err != nil {
			// Keep the marker as the durable recovery authority until retry
			// admission is possible. A later maintenance pass may safely
			// inspect it without confusing this failed attempt with a retry.
			t.logger.Error(
				"Failed to release abandoned cluster backup reservation",
				zap.String("backup_id", req.BackupId),
				zap.String("attempt_id", attemptID),
				zap.Error(err),
			)
			return
		}
		if !headPublished {
			if err := deleteClusterBackupAttempt(
				cleanupCtx,
				resolvedLocation,
				s3Info,
				attemptID,
			); err != nil {
				t.logger.Warn(
					"Failed to remove unpublished cluster backup attempt marker",
					zap.String("attempt_id", attemptID),
					zap.Error(err),
				)
				return
			}
			if err := deleteClusterBackupAttemptLease(
				cleanupCtx,
				resolvedLocation,
				s3Info,
				attemptID,
			); err != nil {
				t.logger.Warn(
					"Unpublished backup attempt lease cleanup deferred",
					zap.String("attempt_id", attemptID),
					zap.Error(err),
				)
			}
			return
		}
		owned, transitionErr := transitionClusterBackupAttemptHead(
			cleanupCtx,
			resolvedLocation,
			s3Info,
			attemptID,
			clusterBackupAttemptStateFailed,
		)
		if transitionErr != nil {
			t.logger.Error(
				"Failed to retire abandoned cluster backup attempt head",
				zap.String("backup_id", req.BackupId),
				zap.String("attempt_id", attemptID),
				zap.Error(transitionErr),
			)
			return
		}
		if err := compactClusterBackupAttemptIfSuperseded(
			cleanupCtx,
			resolvedLocation,
			s3Info,
			attemptID,
			owned,
		); err != nil {
			// The head read proved this attempt is no longer authoritative.
			// Its artifacts and reservation are already absent, so retire the
			// otherwise-unreachable journal without any repository scan.
			t.logger.Warn(
				"Superseded abandoned backup journal compaction deferred",
				zap.String("attempt_id", attemptID),
				zap.Error(err),
			)
			return
		}
		if err := deleteClusterBackupAttemptLease(
			cleanupCtx,
			resolvedLocation,
			s3Info,
			attemptID,
		); err != nil {
			t.logger.Warn(
				"Abandoned backup attempt lease cleanup deferred",
				zap.String("attempt_id", attemptID),
				zap.Error(err),
			)
		}
	}()
	attemptLease, err = startClusterBackupAttemptLease(
		ctx,
		cancel,
		resolvedLocation,
		s3Info,
		attemptID,
	)
	if err != nil {
		writeBackupError(w, "Failed to establish backup attempt lease", err)
		return
	}
	expectedHead := ClusterBackupAttemptHead{
		Version:      clusterBackupAttemptHeadVersion,
		AttemptID:    attemptID,
		BackupID:     req.BackupId,
		MarkerSHA256: hex.EncodeToString(markerDigest[:]),
	}
	previousHead, err := publishClusterBackupAttemptHead(
		ctx,
		resolvedLocation,
		s3Info,
		expectedHead,
	)
	if err != nil {
		reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 30*time.Second)
		published, reconcileErr := clusterBackupAttemptHeadPublicationMatches(
			reconcileCtx,
			resolvedLocation,
			s3Info,
			expectedHead,
		)
		reconcileCancel()
		if !published {
			if reconcileErr != nil {
				cleanupSafe = false
				t.logger.Error(
					"Cluster backup attempt head outcome remains ambiguous; retaining fenced attempt",
					zap.String("backup_id", req.BackupId),
					zap.String("attempt_id", attemptID),
					zap.Error(reconcileErr),
				)
			}
			writeBackupError(w, "Failed to publish backup attempt head", err)
			return
		}
		t.logger.Warn(
			"Recovered cluster backup attempt head after an ambiguous publication response",
			zap.String("backup_id", req.BackupId),
			zap.String("attempt_id", attemptID),
			zap.Error(err),
		)
	}
	headPublished = true

	// Create cluster metadata
	backupFormat := backupConfig.Format
	clusterMeta := &ClusterBackupMetadata{
		Version:            clusterBackupMetadataVersion,
		State:              clusterBackupStateComplete,
		BackupID:           req.BackupId,
		Timestamp:          time.Now(),
		AntflyVersion:      multirafthttp.Version,
		Format:             backupFormat,
		ExpectedTableCount: len(tableNames),
		Tables:             make([]ClusterBackupTableInfo, len(tableNames)),
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
			table := plannedTables[i]
			if table == nil {
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  "table not found",
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:   tableName,
					Status: "failed",
					Error:  "table not found",
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
					Error:  sanitizedBackupFailure(err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      sanitizedBackupFailure(err),
				}
				return nil
			}

			artifactIntegrities, err := t.backupShardsWithIntegrity(ctx, table, backupConfig)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				t.logger.Error("Error forwarding backup", zap.String("table", tableName), zap.Error(err))
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  sanitizedBackupFailure(err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      sanitizedBackupFailure(err),
				}
				return nil // Don't fail entire backup for one table
			}

			// Write table metadata with table-specific backup ID
			if err := metadataStore.WriteMetadata(
				ctx,
				tableBackupID,
				table,
				backupFormat,
				artifactIntegrities,
			); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableBackupStatus{
					Name:   tableName,
					Status: TableBackupStatusStatusFailed,
					Error:  sanitizedBackupFailure(err),
				}
				clusterMeta.Tables[i] = ClusterBackupTableInfo{
					Name:       tableName,
					ShardCount: len(table.Shards),
					Status:     "failed",
					Error:      sanitizedBackupFailure(err),
				}
				return nil
			}
			cleanupMetadataPublished[i] = true

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

	// The cluster manifest is the final commit point. Publish it only after
	// every requested table artifact and table manifest is durable.
	if failedCount == 0 {
		_, leaseOwned, leaseErr := renewClusterBackupAttemptLease(
			ctx,
			resolvedLocation,
			s3Info,
			attemptID,
			time.Now().UTC(),
		)
		if leaseErr != nil {
			writeBackupError(w, "Failed to renew backup attempt lease", leaseErr)
			return
		}
		if !leaseOwned {
			writeBackupError(
				w,
				"Backup attempt lease was fenced",
				context.Canceled,
			)
			return
		}
		clusterMeta.CompletedTableCount = len(results)
		cleanupSafe = false
		commitCtx, commitCancel := context.WithTimeout(ctx, clusterBackupCommitTimeout)
		if strings.HasPrefix(req.Location, "s3://") {
			if err := writeClusterMetadataToBlobStore(
				commitCtx,
				req.BackupId,
				clusterMeta,
				s3Info,
			); err != nil {
				commitCancel()
				cleanupSafe = errors.Is(err, ErrBackupAlreadyExists) ||
					errors.Is(err, ErrBackupMetadataTooLarge)
				writeBackupError(w, "Failed to write cluster metadata", err)
				return
			}
		} else {
			if err := writeClusterMetadataToFile(
				commitCtx,
				resolvedLocation,
				req.BackupId,
				clusterMeta,
			); err != nil {
				commitCancel()
				cleanupSafe = errors.Is(err, ErrBackupAlreadyExists) ||
					errors.Is(err, ErrBackupMetadataTooLarge)
				writeBackupError(w, "Failed to write cluster metadata", err)
				return
			}
		}
		commitCancel()
		headOwned, headTransitionErr := transitionClusterBackupAttemptHead(
			ctx,
			resolvedLocation,
			s3Info,
			attemptID,
			clusterBackupAttemptStateCommitted,
		)
		if headTransitionErr != nil {
			// The aggregate manifest is the immutable commit point. A lost head
			// transition remains safe because active admission validates it.
			t.logger.Warn(
				"Cluster backup committed head update deferred",
				zap.String("attempt_id", attemptID),
				zap.Error(headTransitionErr),
			)
		} else if !headOwned {
			t.logger.Warn(
				"Cluster backup committed with superseded attempt head",
				zap.String("attempt_id", attemptID),
			)
		}
		committed = true
		maintenanceCtx, maintenanceCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := compactSupersededClusterBackupAttempt(
			maintenanceCtx,
			resolvedLocation,
			s3Info,
			previousHead,
			attemptID,
		); err != nil {
			t.logger.Warn(
				"Superseded cluster backup journal compaction deferred",
				zap.String("attempt_id", attemptID),
				zap.Error(err),
			)
		}
		if headTransitionErr == nil {
			// This attempt committed after another writer superseded its active
			// head. Historical restore needs only the immutable aggregate and
			// table manifests, so its unreferenced journal can be removed now.
			if err := compactClusterBackupAttemptIfSuperseded(
				maintenanceCtx,
				resolvedLocation,
				s3Info,
				attemptID,
				headOwned,
			); err != nil {
				t.logger.Warn(
					"Superseded committed backup journal compaction deferred",
					zap.String("attempt_id", attemptID),
					zap.Error(err),
				)
			}
		}
		maintenanceCancel()
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
	if len(req.TableNames) > 0 {
		if err := validateBackupTableNames(req.TableNames, clusterBackupExplicitTableLimit); err != nil {
			errorResponse(w, fmt.Sprintf("Invalid table selection: %v", err), http.StatusBadRequest)
			return
		}
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
		t.logger.Error("Failed to read cluster backup metadata", zap.String("class", sanitizedBackupFailure(err)))
		errorResponse(w, "Failed to read cluster backup metadata", http.StatusInternalServerError)
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
	if err := validateBackupTableNames(tablesToRestore, clusterBackupAttemptMaxTables); err != nil {
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
	tableArtifacts := make([][]common.BackupArtifactIntegrity, len(tablesToRestore))
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
					Error:  "failed to inspect existing table",
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
			metadata, err := metadataStore.ReadMetadata(ctx, tableBackupID)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  "failed to read backup metadata",
				}
				return nil
			}
			if metadata.Format != restoreFormat {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error: fmt.Sprintf(
						"backup format mismatch: cluster metadata has %q, table metadata has %q",
						restoreFormat,
						metadata.Format,
					),
				}
				return nil
			}

			if metadata.Table.Name != tableName {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  fmt.Sprintf("table name mismatch: expected %s, got %s", tableName, metadata.Table.Name),
				}
				return nil
			}
			if err := validateBackupMetadataArtifactIdentities(
				ctx,
				metadataStore,
				req.BackupId,
				metadata,
			); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  "backup artifact validation failed",
				}
				return nil
			}
			tableMetadata[i] = metadata.Table
			tableArtifacts[i] = append(
				[]common.BackupArtifactIntegrity(nil),
				metadata.Artifacts...,
			)
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
			}, tableArtifacts[i]); err != nil {
				results[i] = TableRestoreStatus{
					Name:   tableName,
					Status: TableRestoreStatusStatusFailed,
					Error:  "failed to restore table",
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
			t.logger.Error("Failed to initialize backup storage client", zap.String("class", sanitizedBackupFailure(err)))
			errorResponse(w, "Failed to initialize backup storage client", http.StatusInternalServerError)
			return
		}

		// List objects with cluster-metadata suffix
		objectCh := minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		})
		for object := range objectCh {
			if object.Err != nil {
				t.logger.Warn("Error listing backup objects", zap.String("class", sanitizedBackupFailure(object.Err)))
				errorResponse(w, "Failed to list backup metadata", http.StatusInternalServerError)
				return
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
					// A stale-version, corrupt, or partially uploaded
					// manifest must not make unrelated backups unavailable.
					t.logger.Warn("Skipping invalid cluster backup metadata", zap.String("class", sanitizedBackupFailure(err)))
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
			t.logger.Error("Failed to enumerate backup metadata", zap.String("class", sanitizedBackupFailure(err)))
			errorResponse(w, "Failed to enumerate backup metadata", http.StatusInternalServerError)
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
					t.logger.Warn("Skipping invalid cluster backup metadata", zap.String("class", sanitizedBackupFailure(err)))
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
