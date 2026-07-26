// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations.

package metadata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cleanupOrderBackupStore struct {
	expectedMetadata  int32
	expectedArtifacts int32
	metadataDeleted   atomic.Int32
	artifactsDeleted  atomic.Int32
	phaseViolation    atomic.Bool
}

func (*cleanupOrderBackupStore) EnsureMetadataAbsent(context.Context, string) error { return nil }
func (*cleanupOrderBackupStore) ReserveBackupID(context.Context, string) error      { return nil }
func (s *cleanupOrderBackupStore) DeleteMetadata(context.Context, string) error {
	s.metadataDeleted.Add(1)
	return nil
}
func (s *cleanupOrderBackupStore) DeleteArtifact(context.Context, string) error {
	if s.metadataDeleted.Load() != s.expectedMetadata {
		s.phaseViolation.Store(true)
	}
	s.artifactsDeleted.Add(1)
	return nil
}
func (*cleanupOrderBackupStore) ValidateArtifact(context.Context, string) error { return nil }
func (s *cleanupOrderBackupStore) ReleaseBackupID(context.Context, string) error {
	if s.artifactsDeleted.Load() != s.expectedArtifacts {
		s.phaseViolation.Store(true)
	}
	return nil
}
func (*cleanupOrderBackupStore) WriteMetadata(context.Context, string, *store.Table, common.BackupFormat) error {
	return nil
}
func (*cleanupOrderBackupStore) ReadMetadata(context.Context, string) (*store.Table, common.BackupFormat, error) {
	return nil, "", errors.New("not implemented")
}
func (*cleanupOrderBackupStore) ResolvedLocation() string { return "" }

func TestFileBackupStorePersistsFormatInVersionedEnvelope(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	table := &store.Table{Name: "documents"}

	require.NoError(t, backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		table,
		common.BackupFormatNative,
	))

	restored, format, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.NoError(t, err)
	assert.Equal(t, common.BackupFormatNative, format)
	assert.Equal(t, table.Name, restored.Name)
}

func TestFileBackupStorePublishesMetadataCreateOnly(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	first := &store.Table{Name: "first"}
	second := &store.Table{Name: "second"}

	require.NoError(t, backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		first,
		common.BackupFormatPortable,
	))
	err := backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		second,
		common.BackupFormatPortable,
	)
	require.ErrorIs(t, err, ErrBackupAlreadyExists)

	restored, _, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.NoError(t, err)
	assert.Equal(t, first.Name, restored.Name)
}

func TestFileBackupStoreReservationPermanentlyConsumesID(t *testing.T) {
	backupStore := &fileBackupStore{location: filepath.Join(t.TempDir(), "new", "backup")}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), "backup-1"),
		ErrBackupAlreadyExists,
	)
}

func TestFileBackupStoreCleanupReleasesReservationAfterArtifactsAreRemoved(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-1.tar.zst"),
		[]byte("artifact"),
		0o600,
	))

	require.NoError(t, backupStore.DeleteArtifact(context.Background(), "backup-1-1.tar.zst"))
	require.NoError(t, backupStore.DeleteMetadata(context.Background(), "backup-1"))
	require.NoError(t, backupStore.ReleaseBackupID(context.Background(), "backup-1"))
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
}

func TestBackupAttemptCleanupRemovesCommitRecordsBeforeArtifacts(t *testing.T) {
	backupStore := &cleanupOrderBackupStore{
		expectedMetadata:  2,
		expectedArtifacts: 2,
	}
	require.NoError(t, cleanupBackupAttempt(
		backupStore,
		"backup-1",
		[]string{"table-a", "table-b"},
		[]string{"artifact-a", "artifact-b"},
	))
	require.False(t, backupStore.phaseViolation.Load())
	require.Equal(t, int32(2), backupStore.metadataDeleted.Load())
	require.Equal(t, int32(2), backupStore.artifactsDeleted.Load())
}

func TestBackupAttemptContentCleanupRetainsReservationFence(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-1.afb"),
		[]byte("artifact"),
		0o600,
	))

	require.NoError(t, cleanupBackupAttemptContents(
		context.Background(),
		backupStore,
		nil,
		[]string{"backup-1-1.afb"},
	))
	require.NoFileExists(t, filepath.Join(root, "backup-1-1.afb"))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), "backup-1"),
		ErrBackupAlreadyExists,
	)
	require.NoError(t, backupStore.ReleaseBackupID(context.Background(), "backup-1"))
}

func TestFileBackupStoreDoesNotPublishAfterCancellation(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, backupStore.ReserveBackupID(ctx, "reserved"), context.Canceled)
	require.ErrorIs(
		t,
		backupStore.WriteMetadata(
			ctx,
			"metadata",
			&store.Table{Name: "documents"},
			common.BackupFormatPortable,
		),
		context.Canceled,
	)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestFileBackupStoreConcurrentPublicationHasSingleWinner(t *testing.T) {
	backupStore := &fileBackupStore{location: t.TempDir()}
	var successes atomic.Int32
	unexpected := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := backupStore.WriteMetadata(
				context.Background(),
				"backup-1",
				&store.Table{Name: "documents"},
				common.BackupFormatPortable,
			)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, ErrBackupAlreadyExists):
			default:
				unexpected <- err
			}
		}()
	}
	wg.Wait()
	close(unexpected)

	assert.Equal(t, int32(1), successes.Load())
	for err := range unexpected {
		require.NoError(t, err)
	}
}

func TestFileBackupStoreRejectsOversizedMetadata(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-metadata.json"),
		make([]byte, maxBackupMetadataBytes+1),
		0o600,
	))

	_, _, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.ErrorIs(t, err, ErrBackupMetadataTooLarge)
}

func TestTableBackupMetadataIDIsStableAndPathSafe(t *testing.T) {
	first := tableBackupMetadataID("tenant/table with spaces", "backup-1")
	assert.Equal(t, first, tableBackupMetadataID("tenant/table with spaces", "backup-1"))
	assert.NotEqual(t, first, tableBackupMetadataID("tenant/table with spaces", "backup-2"))
	assert.NotEqual(t, first, tableBackupMetadataID("tenant/table", "with spaces\x00backup-1"))
	require.NoError(t, common.ValidateBackupID(first))
	assert.Len(t, first, len("table-")+64)
}

func TestValidateBackupTableNamesRejectsAmbiguousSelections(t *testing.T) {
	require.NoError(t, validateBackupTableNames([]string{"documents", "events"}, clusterBackupExplicitTableLimit))
	require.ErrorContains(
		t,
		validateBackupTableNames([]string{"documents", "documents"}, clusterBackupExplicitTableLimit),
		"selected more than once",
	)
	require.ErrorContains(
		t,
		validateBackupTableNames([]string{"documents", " "}, clusterBackupExplicitTableLimit),
		"1 to 4096 bytes",
	)
	require.ErrorContains(
		t,
		validateBackupTableNames(
			[]string{strings.Repeat("x", clusterBackupAttemptMaxNameBytes+1)},
			clusterBackupExplicitTableLimit,
		),
		"1 to 4096 bytes",
	)
	require.ErrorContains(
		t,
		validateBackupTableNames(
			make([]string, clusterBackupExplicitTableLimit+1),
			clusterBackupExplicitTableLimit,
		),
		"at most 256 tables",
	)
	require.ErrorContains(
		t,
		validateBackupTableNames(
			make([]string, clusterBackupAttemptMaxTables+1),
			clusterBackupAttemptMaxTables,
		),
		"at most 4096 tables",
	)
}

func TestFileBackupStoreRejectsUnversionedMetadata(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-metadata.json"),
		[]byte(`{"name":"documents"}`),
		0o600,
	))

	_, _, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.Error(t, err)
	assert.ErrorContains(t, err, "unsupported backup metadata version")
}

func TestFileBackupStoreRejectsUnknownFormat(t *testing.T) {
	backupStore := &fileBackupStore{location: t.TempDir()}
	err := backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		&store.Table{Name: "documents"},
		common.BackupFormat("unknown"),
	)
	require.ErrorContains(t, err, "unsupported backup format")
}

func TestClusterBackupMetadataRequiresVersionIDAndFormat(t *testing.T) {
	valid := &ClusterBackupMetadata{
		Version:             clusterBackupMetadataVersion,
		State:               clusterBackupStateComplete,
		BackupID:            "backup-1",
		Format:              common.BackupFormatPortable,
		ExpectedTableCount:  1,
		CompletedTableCount: 1,
		Tables: []ClusterBackupTableInfo{{
			Name:           "documents",
			BackupLocation: "file:///backups/documents-metadata.json",
			Status:         "completed",
		}},
	}
	require.NoError(t, validateClusterBackupMetadata("backup-1", valid))

	invalidVersion := *valid
	invalidVersion.Version = 1
	require.ErrorContains(
		t,
		validateClusterBackupMetadata("backup-1", &invalidVersion),
		"unsupported cluster backup metadata version",
	)

	wrongID := *valid
	wrongID.BackupID = "backup-2"
	require.ErrorContains(
		t,
		validateClusterBackupMetadata("backup-1", &wrongID),
		"ID mismatch",
	)

	unknownFormat := *valid
	unknownFormat.Format = "unknown"
	require.ErrorContains(
		t,
		validateClusterBackupMetadata("backup-1", &unknownFormat),
		"unsupported cluster backup format",
	)

	incomplete := *valid
	incomplete.CompletedTableCount = 0
	require.ErrorContains(
		t,
		validateClusterBackupMetadata("backup-1", &incomplete),
		"incomplete table coverage",
	)

	failedTable := *valid
	failedTable.Tables = append([]ClusterBackupTableInfo(nil), valid.Tables...)
	failedTable.Tables[0].Status = "failed"
	require.ErrorContains(
		t,
		validateClusterBackupMetadata("backup-1", &failedTable),
		"incomplete table entry",
	)
}

func TestClusterBackupAttemptMarkersSelectNewestAttempt(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	older := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-older",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	newer := *older
	newer.AttemptID = "afba-newer"
	newer.BackupID = "backup-2"
	newer.CreatedAt = older.CreatedAt.Add(time.Second)

	require.NoError(t, writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		older,
	))
	require.NoError(t, writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		&newer,
	))
	latest, err := latestClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		backupStore,
		clusterBackupAttemptScanLimit,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, newer.AttemptID, latest.AttemptID)
	assert.Equal(t, newer.BackupID, latest.BackupID)
}

func TestNewestClusterBackupAttemptMustBeCommittedAndRestorable(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	table := &store.Table{
		Name:   "documents",
		Shards: map[types.ID]*store.ShardConfig{1: {}},
	}
	metadataID := tableBackupMetadataID(table.Name, "backup-1")
	artifactNames := backupArtifactNamesForFormat("backup-1", table, common.BackupFormatPortable)
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-newest",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{table.Name},
		MetadataIDs:        []string{metadataID},
		ArtifactNames:      artifactNames,
	}
	require.Error(t, validateNewestClusterBackupAttempt(
		context.Background(), "file://"+root, nil, backupStore, attempt,
	))
	require.NoError(t, backupStore.WriteMetadata(
		context.Background(), metadataID, table, common.BackupFormatPortable,
	))
	require.NoError(t, writeClusterMetadataToFile(
		context.Background(),
		"file://"+root,
		attempt.BackupID,
		&ClusterBackupMetadata{
			Version:             clusterBackupMetadataVersion,
			State:               clusterBackupStateComplete,
			BackupID:            attempt.BackupID,
			Format:              common.BackupFormatPortable,
			ExpectedTableCount:  1,
			CompletedTableCount: 1,
			Tables: []ClusterBackupTableInfo{{
				Name:           table.Name,
				BackupLocation: "file:///backups/" + metadataID + "-metadata.json",
				Status:         "completed",
			}},
		},
	))
	require.Error(t, validateNewestClusterBackupAttempt(
		context.Background(), "file://"+root, nil, backupStore, attempt,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifactNames[0]), []byte("artifact"), 0o600,
	))
	require.NoError(t, validateNewestClusterBackupAttempt(
		context.Background(), "file://"+root, nil, backupStore, attempt,
	))
}

func TestClusterBackupAttemptRejectsOverlappingIdentifiers(t *testing.T) {
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-attempt",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"shared-id"},
		ArtifactNames:      []string{"shared-id"},
	}
	require.ErrorContains(
		t,
		validateClusterBackupAttempt(attempt, attempt.AttemptID),
		"duplicate identifier",
	)

	attempt.ArtifactNames = []string{"artifact.afb"}
	attempt.CreatedAt = time.Time{}
	require.ErrorContains(
		t,
		validateClusterBackupAttempt(attempt, attempt.AttemptID),
		"invalid cluster backup attempt marker",
	)
}

func TestStaleUncommittedClusterBackupAttemptIsRetainedWithoutLeaseAuthority(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-1.afb"),
		[]byte("artifact"),
		0o600,
	))
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-stale",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptMaxAge - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	require.NoError(t, writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	))

	latest, err := latestClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		backupStore,
		clusterBackupAttemptScanLimit,
		false,
	)
	require.NoError(t, err)
	require.Nil(t, latest)
	require.FileExists(t, filepath.Join(root, "backup-1-1.afb"))
	require.FileExists(t, filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), "backup-1"),
		ErrBackupAlreadyExists,
	)
}

func TestStaleCommittedClusterBackupAttemptRetainsPermanentReservation(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-stale",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptMaxAge - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	require.NoError(t, writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	))
	require.NoError(t, writeClusterMetadataToFile(
		context.Background(),
		"file://"+root,
		"backup-1",
		&ClusterBackupMetadata{
			Version:             clusterBackupMetadataVersion,
			State:               clusterBackupStateComplete,
			BackupID:            "backup-1",
			Format:              common.BackupFormatPortable,
			ExpectedTableCount:  1,
			CompletedTableCount: 1,
			Tables: []ClusterBackupTableInfo{{
				Name:           "documents",
				BackupLocation: "file:///backups/documents-backup-1-metadata.json",
				Status:         "completed",
			}},
		},
	))

	latest, err := latestClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		backupStore,
		clusterBackupAttemptScanLimit,
		false,
	)
	require.NoError(t, err)
	require.Nil(t, latest)
	require.FileExists(t, filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), "backup-1"),
		ErrBackupAlreadyExists,
	)
}

func TestFileBackupStoreValidatesArtifactPresence(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.Error(t, backupStore.ValidateArtifact(context.Background(), "missing.afb"))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "present.afb"),
		[]byte("artifact"),
		0o600,
	))
	require.NoError(t, backupStore.ValidateArtifact(context.Background(), "present.afb"))
	require.Error(t, backupStore.ValidateArtifact(context.Background(), "../outside.afb"))
}
