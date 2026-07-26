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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	json "github.com/antflydb/antfly/go/pkg/libaf/json"

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
func (*cleanupOrderBackupStore) ValidateArtifactIdentity(
	context.Context,
	common.BackupArtifactIntegrity,
) error {
	return nil
}
func (s *cleanupOrderBackupStore) ReleaseBackupID(context.Context, string) error {
	if s.artifactsDeleted.Load() != s.expectedArtifacts {
		s.phaseViolation.Store(true)
	}
	return nil
}
func (*cleanupOrderBackupStore) WriteMetadata(
	context.Context,
	string,
	*store.Table,
	common.BackupFormat,
	[]common.BackupArtifactIntegrity,
) error {
	return nil
}
func (*cleanupOrderBackupStore) ReadMetadata(context.Context, string) (*backupMetadata, error) {
	return nil, errors.New("not implemented")
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
		nil,
	))

	metadata, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.NoError(t, err)
	assert.Equal(t, common.BackupFormatNative, metadata.Format)
	assert.Equal(t, table.Name, metadata.Table.Name)
}

func TestFileBackupStorePersistsPortableArtifactIntegrity(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	table := &store.Table{
		Name:   "documents",
		Shards: map[types.ID]*store.ShardConfig{1: {}},
	}
	artifacts := []common.BackupArtifactIntegrity{{
		Name:      "backup-1-1.afb",
		SizeBytes: uint64(len("artifact")),
		SHA256:    "c7c5c1d70c5dec4416ab6158afd0b223ef40c29b1dc1f97ed9428b94d4cadb1c",
	}}

	require.NoError(t, backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		table,
		common.BackupFormatPortable,
		artifacts,
	))
	body, err := os.ReadFile(filepath.Join(root, "backup-1-metadata.json"))
	require.NoError(t, err)
	var envelope backupMetadata
	require.NoError(t, json.Unmarshal(body, &envelope))
	require.Equal(t, uint32(2), envelope.Version)
	require.Equal(t, artifacts, envelope.Artifacts)

	metadata, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.NoError(t, err)
	require.Equal(t, common.BackupFormatPortable, metadata.Format)
}

func TestFileBackupStoreRejectsIncompletePortableArtifactIntegrity(t *testing.T) {
	backupStore := &fileBackupStore{location: t.TempDir()}
	table := &store.Table{
		Name:   "documents",
		Shards: map[types.ID]*store.ShardConfig{1: {}},
	}

	err := backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		table,
		common.BackupFormatPortable,
		nil,
	)
	require.ErrorContains(t, err, "do not match table shards")
}

func TestFileBackupStoreBindsPortableArtifactsToCanonicalShardNames(t *testing.T) {
	backupStore := &fileBackupStore{location: t.TempDir()}
	table := &store.Table{
		Name: "documents",
		Shards: map[types.ID]*store.ShardConfig{
			0xa: {},
			0xb: {},
		},
	}
	validDigest := strings.Repeat("0", sha256.Size*2)
	valid := []common.BackupArtifactIntegrity{
		{Name: "backup-prod-a.afb", SizeBytes: 1, SHA256: validDigest},
		{Name: "backup-prod-b.afb", SizeBytes: 1, SHA256: validDigest},
	}
	require.NoError(t, backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		table,
		common.BackupFormatPortable,
		valid,
	))

	testCases := []struct {
		name      string
		artifacts []common.BackupArtifactIntegrity
		errorText string
	}{
		{
			name: "unknown shard",
			artifacts: []common.BackupArtifactIntegrity{
				{Name: "backup-prod-a.afb", SizeBytes: 1, SHA256: validDigest},
				{Name: "backup-prod-c.afb", SizeBytes: 1, SHA256: validDigest},
			},
			errorText: "unknown shard",
		},
		{
			name: "mixed backup ids",
			artifacts: []common.BackupArtifactIntegrity{
				{Name: "backup-prod-a.afb", SizeBytes: 1, SHA256: validDigest},
				{Name: "other-b.afb", SizeBytes: 1, SHA256: validDigest},
			},
			errorText: "one backup ID",
		},
		{
			name: "duplicate shard",
			artifacts: []common.BackupArtifactIntegrity{
				{Name: "backup-prod-a.afb", SizeBytes: 1, SHA256: validDigest},
				{Name: "backup-prod-0a.afb", SizeBytes: 1, SHA256: validDigest},
			},
			errorText: "canonically named",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validatePortableArtifactIntegrities(table, testCase.artifacts)
			require.ErrorContains(t, err, testCase.errorText)
		})
	}
}

func TestFileBackupStoreRejectsPortableMetadataWithoutShards(t *testing.T) {
	err := validatePortableArtifactIntegrities(
		&store.Table{Name: "documents"},
		nil,
	)
	require.ErrorContains(t, err, "do not match table shards")
}

func TestFileBackupStoreValidatesPortableArtifactIdentity(t *testing.T) {
	root := t.TempDir()
	body := []byte("portable-artifact")
	digest := sha256.Sum256(body)
	artifact := common.BackupArtifactIntegrity{
		Name:      "backup-1-1.afb",
		SizeBytes: uint64(len(body)),
		SHA256:    hex.EncodeToString(digest[:]),
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifact.Name),
		body,
		0o600,
	))
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ValidateArtifactIdentity(
		context.Background(),
		artifact,
	))

	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifact.Name),
		body[:len(body)-1],
		0o600,
	))
	require.ErrorIs(
		t,
		backupStore.ValidateArtifactIdentity(context.Background(), artifact),
		common.ErrBackupArtifactIntegrityMismatch,
	)

	corrupt := append([]byte(nil), body...)
	corrupt[0] ^= 0xff
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifact.Name),
		corrupt,
		0o600,
	))
	require.ErrorIs(
		t,
		backupStore.ValidateArtifactIdentity(context.Background(), artifact),
		common.ErrBackupArtifactIntegrityMismatch,
	)
}

func TestPortableArtifactIdentityValidationBindsRequestedBackupID(t *testing.T) {
	root := t.TempDir()
	body := []byte("portable-artifact")
	digest := sha256.Sum256(body)
	const requestedBackupID = "backup-1"
	const artifactBackupID = "other-backup"
	shardID := types.ID(1)
	artifact := common.BackupArtifactIntegrity{
		Name: common.ShardPortableBackupFileName(
			artifactBackupID,
			shardID,
		),
		SizeBytes: uint64(len(body)),
		SHA256:    hex.EncodeToString(digest[:]),
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifact.Name),
		body,
		0o600,
	))

	err := validateBackupMetadataArtifactIdentities(
		context.Background(),
		&fileBackupStore{location: root},
		requestedBackupID,
		&backupMetadata{
			Version: backupMetadataVersion,
			Format:  common.BackupFormatPortable,
			Table: &store.Table{
				Name: "documents",
				Shards: map[types.ID]*store.ShardConfig{
					shardID: {},
				},
			},
			Artifacts: []common.BackupArtifactIntegrity{artifact},
		},
	)
	require.ErrorContains(
		t,
		err,
		common.ShardPortableBackupFileName(requestedBackupID, shardID),
	)
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
		common.BackupFormatNative,
		nil,
	))
	err := backupStore.WriteMetadata(
		context.Background(),
		"backup-1",
		second,
		common.BackupFormatNative,
		nil,
	)
	require.ErrorIs(t, err, ErrBackupAlreadyExists)

	metadata, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.NoError(t, err)
	assert.Equal(t, first.Name, metadata.Table.Name)
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
			nil,
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
				common.BackupFormatNative,
				nil,
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

	_, err := backupStore.ReadMetadata(context.Background(), "backup-1")
	require.ErrorIs(t, err, ErrBackupMetadataTooLarge)
}

func TestTableBackupMetadataIDIsStableAndPathSafe(t *testing.T) {
	first := tableBackupMetadataID("tenant/table with spaces", "backup-1")
	assert.Equal(t, first, tableBackupMetadataID("tenant/table with spaces", "backup-1"))
	assert.NotEqual(t, first, tableBackupMetadataID("tenant/table with spaces", "backup-2"))
	assert.NotEqual(t, first, tableBackupMetadataID("tenant/table", "with spaces\x00backup-1"))
	require.NoError(t, common.ValidateBackupID(first))
	assert.Len(t, first, len("table-")+64)
	assert.Equal(
		t,
		"table-77cfb73404d45d27f72ecbfb232c3fbaf6efbb64592b5ae78fca3e5c544fd3d4",
		tableBackupMetadataID("docs", "go-cluster"),
	)
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

	_, err := backupStore.ReadMetadata(context.Background(), "backup-1")
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
		nil,
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

	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		older,
	)
	require.NoError(t, err)
	_, err = writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		&newer,
	)
	require.NoError(t, err)
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

func TestClusterBackupAttemptHeadAtomicallyPinsExactMarker(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-first",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	digest, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	previous, err := publishClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		ClusterBackupAttemptHead{
			Version:      clusterBackupAttemptHeadVersion,
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(digest[:]),
		},
	)
	require.NoError(t, err)
	require.Nil(t, previous)

	readHead := func() ClusterBackupAttemptHead {
		body, readErr := os.ReadFile(filepath.Join(root, clusterBackupAttemptHeadName))
		require.NoError(t, readErr)
		var head ClusterBackupAttemptHead
		require.NoError(t, json.Unmarshal(body, &head))
		return head
	}
	firstHead := readHead()
	assert.Equal(t, attempt.AttemptID, firstHead.AttemptID)
	assert.Equal(t, attempt.BackupID, firstHead.BackupID)
	assert.Equal(t, uint64(1), firstHead.Generation)
	assert.Equal(t, clusterBackupAttemptStateActive, firstHead.State)
	assert.Equal(t, hex.EncodeToString(digest[:]), firstHead.MarkerSHA256)

	attempt.AttemptID = "afba-second"
	attempt.BackupID = "backup-2"
	secondDigest, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	previous, err = publishClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		ClusterBackupAttemptHead{
			Version:      clusterBackupAttemptHeadVersion,
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(secondDigest[:]),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, previous)
	assert.Equal(t, "afba-first", previous.AttemptID)
	secondHead := readHead()
	assert.Equal(t, attempt.AttemptID, secondHead.AttemptID)
	assert.Equal(t, attempt.BackupID, secondHead.BackupID)
	assert.Equal(t, uint64(2), secondHead.Generation)
	assert.Equal(t, clusterBackupAttemptStateActive, secondHead.State)
	assert.Equal(t, hex.EncodeToString(secondDigest[:]), secondHead.MarkerSHA256)

	owned, err := transitionClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		clusterBackupAttemptStateCommitted,
	)
	require.NoError(t, err)
	require.True(t, owned)
	committedHead := readHead()
	assert.Equal(t, uint64(3), committedHead.Generation)
	assert.Equal(t, clusterBackupAttemptStateCommitted, committedHead.State)

	owned, err = transitionClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		"afba-first",
		clusterBackupAttemptStateFailed,
	)
	require.NoError(t, err)
	require.False(t, owned)
}

func TestClusterBackupAttemptPublicationReconciliationIsExact(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-reconcile",
		BackupID:           "backup-reconcile",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-reconcile"},
		ArtifactNames:      []string{"backup-reconcile-1.afb"},
	}
	digest, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	matches, err := clusterBackupAttemptMarkerPublicationMatches(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		digest,
	)
	require.NoError(t, err)
	require.True(t, matches)

	markerPath := filepath.Join(root, clusterBackupAttemptDir, attempt.AttemptID+".json")
	require.NoError(t, os.WriteFile(markerPath, []byte("{}\n"), 0o600))
	matches, err = clusterBackupAttemptMarkerPublicationMatches(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		digest,
	)
	require.NoError(t, err)
	require.False(t, matches)

	expectedHead := ClusterBackupAttemptHead{
		Version:      clusterBackupAttemptHeadVersion,
		AttemptID:    attempt.AttemptID,
		BackupID:     attempt.BackupID,
		MarkerSHA256: hex.EncodeToString(digest[:]),
	}
	_, err = publishClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		expectedHead,
	)
	require.NoError(t, err)
	matches, err = clusterBackupAttemptHeadPublicationMatches(
		context.Background(),
		"file://"+root,
		nil,
		expectedHead,
	)
	require.NoError(t, err)
	require.True(t, matches)

	require.NoError(t, os.WriteFile(
		filepath.Join(root, clusterBackupAttemptHeadName),
		[]byte("{}\n"),
		0o600,
	))
	matches, err = clusterBackupAttemptHeadPublicationMatches(
		context.Background(),
		"file://"+root,
		nil,
		expectedHead,
	)
	require.NoError(t, err)
	require.False(t, matches)
}

func TestClusterBackupAttemptCompactsOnlyTerminalSupersededMarker(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-old-active",
		BackupID:           "backup-old",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-old"},
		ArtifactNames:      []string{"backup-old-1.afb"},
	}
	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	attempt.AttemptID = "afba-current"
	attempt.BackupID = "backup-current"
	_, err = writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)

	previous := &ClusterBackupAttemptHead{
		Version:      clusterBackupAttemptHeadVersion,
		Generation:   1,
		AttemptID:    "afba-old-active",
		BackupID:     "backup-old",
		State:        clusterBackupAttemptStateActive,
		MarkerSHA256: strings.Repeat("0", sha256.Size*2),
	}
	require.NoError(t, compactSupersededClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		previous,
		attempt.AttemptID,
	))
	oldMarker := filepath.Join(
		root,
		clusterBackupAttemptDir,
		"afba-old-active.json",
	)
	require.FileExists(t, oldMarker)

	previous.State = clusterBackupAttemptStateCommitted
	require.NoError(t, compactSupersededClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		previous,
		attempt.AttemptID,
	))
	require.NoFileExists(t, oldMarker)
	_, err = os.Stat(filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	))
	require.NoError(t, err)
}

func TestClusterBackupAttemptOwnerCompactsItsSupersededMarker(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-owner",
		BackupID:           "backup-owner",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-owner"},
		ArtifactNames:      []string{"backup-owner-1.afb"},
	}
	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	markerPath := filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	)

	require.NoError(t, compactClusterBackupAttemptIfSuperseded(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		true,
	))
	require.FileExists(t, markerPath)
	require.NoError(t, compactClusterBackupAttemptIfSuperseded(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		false,
	))
	require.NoFileExists(t, markerPath)
}

func TestClusterBackupAttemptHeadSerializesConcurrentFilePublishers(t *testing.T) {
	root := t.TempDir()
	attemptIDs := [...]string{
		"afba-concurrent-1",
		"afba-concurrent-2",
		"afba-concurrent-3",
		"afba-concurrent-4",
		"afba-concurrent-5",
		"afba-concurrent-6",
		"afba-concurrent-7",
		"afba-concurrent-8",
	}
	errs := make(chan error, len(attemptIDs))
	var wg sync.WaitGroup
	for _, attemptID := range attemptIDs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := publishClusterBackupAttemptHead(
				context.Background(),
				"file://"+root,
				nil,
				ClusterBackupAttemptHead{
					Version:      clusterBackupAttemptHeadVersion,
					AttemptID:    attemptID,
					BackupID:     "backup-" + attemptID,
					MarkerSHA256: strings.Repeat("0", sha256.Size*2),
				},
			)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	head, err := readClusterBackupAttemptHeadFile(
		filepath.Join(root, clusterBackupAttemptHeadName),
	)
	require.NoError(t, err)
	require.NotNil(t, head)
	assert.Equal(t, uint64(len(attemptIDs)), head.Generation)
	assert.Equal(t, clusterBackupAttemptStateActive, head.State)
}

func TestGoBackupProducerRejectsRepositoryOwnedByZig(t *testing.T) {
	root := t.TempDir()
	headPath := filepath.Join(root, zigClusterBackupAttemptHeadName)
	require.NoError(t, os.WriteFile(headPath, []byte(`{"version":1}`), 0o600))
	err := ensureZigClusterBackupAttemptHeadAbsent(
		context.Background(),
		"file://"+root,
		nil,
	)
	require.ErrorContains(t, err, "newer producer")
	require.NoError(t, os.Remove(headPath))
	require.NoError(t, ensureZigClusterBackupAttemptHeadAbsent(
		context.Background(),
		"file://"+root,
		nil,
	))
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
		context.Background(),
		metadataID,
		table,
		common.BackupFormatPortable,
		[]common.BackupArtifactIntegrity{{
			Name:      artifactNames[0],
			SizeBytes: uint64(len("artifact")),
			SHA256:    "c7c5c1d70c5dec4416ab6158afd0b223ef40c29b1dc1f97ed9428b94d4cadb1c",
		}},
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

func TestStaleClusterBackupAttemptWithoutLeaseIsClaimedAndReclaimed(t *testing.T) {
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
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptReclaimGrace - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)

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
	require.NoFileExists(t, filepath.Join(root, "backup-1-1.afb"))
	require.NoFileExists(t, filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	))
	require.NoFileExists(t, clusterBackupAttemptLeasePath(
		"file://"+root,
		attempt.AttemptID,
	))
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
}

func TestExpiredLeasedClusterBackupAttemptIsFencedAndReclaimed(t *testing.T) {
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
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptReclaimGrace - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	require.NoError(t, createClusterBackupAttemptLease(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		time.Now().UTC().Add(-clusterBackupAttemptLeaseDuration-time.Minute),
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
	require.NoFileExists(t, filepath.Join(root, "backup-1-1.afb"))
	require.NoFileExists(t, filepath.Join(
		root,
		clusterBackupAttemptDir,
		attempt.AttemptID+".json",
	))
	require.NoFileExists(t, clusterBackupAttemptLeasePath(
		"file://"+root,
		attempt.AttemptID,
	))
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
}

func TestClusterBackupAttemptLeaseCannotRenewAfterReclamationClaim(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	attemptID := "afba-lease"
	startedAt := time.Now().UTC()
	require.NoError(t, createClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		startedAt,
	))

	claimed, err := claimExpiredClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		startedAt.Add(clusterBackupAttemptLeaseDuration-time.Second),
	)
	require.NoError(t, err)
	require.False(t, claimed)

	expiresAt, owned, err := renewClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		startedAt.Add(time.Minute),
	)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, startedAt.Add(time.Minute+clusterBackupAttemptLeaseDuration), expiresAt)

	claimed, err = claimExpiredClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		expiresAt.Add(time.Second),
	)
	require.NoError(t, err)
	require.True(t, claimed)

	_, owned, err = renewClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		expiresAt.Add(2*time.Second),
	)
	require.NoError(t, err)
	require.False(t, owned)
}

func TestClusterBackupAttemptReclamationClaimIsExclusiveAndRecoverable(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	attemptID := "afba-exclusive-reclaim"
	startedAt := time.Now().UTC()
	require.NoError(t, createClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		startedAt,
	))

	claimAt := startedAt.Add(clusterBackupAttemptLeaseDuration + time.Second)
	claimed, err := claimExpiredClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		claimAt,
	)
	require.NoError(t, err)
	require.True(t, claimed)

	leasePath := clusterBackupAttemptLeasePath(location, attemptID)
	reclaiming, err := readClusterBackupAttemptLeaseFile(leasePath, attemptID)
	require.NoError(t, err)
	require.NotNil(t, reclaiming)
	require.Equal(t, clusterBackupAttemptLeaseStateReclaiming, reclaiming.State)
	require.Equal(t, uint64(2), reclaiming.Generation)

	claimed, err = claimExpiredClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		claimAt.Add(time.Second),
	)
	require.NoError(t, err)
	require.False(t, claimed)

	claimed, err = claimExpiredClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		reclaiming.ExpiresAt.Add(time.Second),
	)
	require.NoError(t, err)
	require.True(t, claimed)
	recovered, err := readClusterBackupAttemptLeaseFile(leasePath, attemptID)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	require.Equal(t, uint64(3), recovered.Generation)
	require.True(t, recovered.ExpiresAt.After(reclaiming.ExpiresAt))
}

func TestClusterBackupAttemptProducerReprovesLeaseBeforeCleanup(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	activeController, err := startClusterBackupAttemptLease(
		context.Background(),
		func() {},
		location,
		nil,
		"afba-active-cleanup-owner",
	)
	require.NoError(t, err)
	owned, err := activeController.StopAndAcquireCleanupWindow(context.Background())
	require.NoError(t, err)
	require.True(t, owned)

	attemptID := "afba-cleanup-owner"
	controller, err := startClusterBackupAttemptLease(
		context.Background(),
		func() {},
		location,
		nil,
		attemptID,
	)
	require.NoError(t, err)

	_, err = mutateClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attemptID,
		func(
			current *clusterBackupAttemptLeaseRecord,
		) (*clusterBackupAttemptLeaseRecord, bool, error) {
			require.NotNil(t, current)
			next := *current
			next.Generation++
			next.State = clusterBackupAttemptLeaseStateReclaiming
			next.ExpiresAt = time.Now().UTC().Add(
				clusterBackupAttemptCleanupTimeout +
					clusterBackupAttemptLeaseSafetyMargin,
			)
			return &next, true, nil
		},
	)
	require.NoError(t, err)

	owned, err = controller.StopAndAcquireCleanupWindow(context.Background())
	require.NoError(t, err)
	require.False(t, owned)
}

func TestExpiredAuthoritativeAttemptRetiresHeadBeforeKeepingJournal(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-authoritative",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptReclaimGrace - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	digest, err := writeClusterBackupAttempt(
		context.Background(),
		location,
		nil,
		attempt,
	)
	require.NoError(t, err)
	_, err = publishClusterBackupAttemptHead(
		context.Background(),
		location,
		nil,
		ClusterBackupAttemptHead{
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(digest[:]),
		},
	)
	require.NoError(t, err)
	require.NoError(t, createClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attempt.AttemptID,
		time.Now().UTC().Add(-clusterBackupAttemptLeaseDuration-time.Minute),
	))

	latest, err := latestClusterBackupAttempt(
		context.Background(),
		location,
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
	head, err := readClusterBackupAttemptHeadFile(filepath.Join(
		root,
		clusterBackupAttemptHeadName,
	))
	require.NoError(t, err)
	require.Equal(t, clusterBackupAttemptStateFailed, head.State)
	require.NoFileExists(t, clusterBackupAttemptLeasePath(location, attempt.AttemptID))
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
}

func TestExpiredCommittedHeadRetainsCorruptionAuthorityWithoutAggregate(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-committed-head",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptReclaimGrace - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	digest, err := writeClusterBackupAttempt(
		context.Background(),
		location,
		nil,
		attempt,
	)
	require.NoError(t, err)
	_, err = publishClusterBackupAttemptHead(
		context.Background(),
		location,
		nil,
		ClusterBackupAttemptHead{
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(digest[:]),
		},
	)
	require.NoError(t, err)
	owned, err := transitionClusterBackupAttemptHead(
		context.Background(),
		location,
		nil,
		attempt.AttemptID,
		clusterBackupAttemptStateCommitted,
	)
	require.NoError(t, err)
	require.True(t, owned)
	require.NoError(t, createClusterBackupAttemptLease(
		context.Background(),
		location,
		nil,
		attempt.AttemptID,
		time.Now().UTC().Add(-clusterBackupAttemptLeaseDuration-time.Minute),
	))

	latest, err := latestClusterBackupAttempt(
		context.Background(),
		location,
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
	require.NoFileExists(t, clusterBackupAttemptLeasePath(location, attempt.AttemptID))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), attempt.BackupID),
		ErrBackupAlreadyExists,
	)
	head, err := readClusterBackupAttemptHeadFile(filepath.Join(
		root,
		clusterBackupAttemptHeadName,
	))
	require.NoError(t, err)
	require.Equal(t, clusterBackupAttemptStateCommitted, head.State)
}

func TestStaleCommittedClusterBackupAttemptRetainsPermanentReservation(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(context.Background(), "backup-1"))
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-stale",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC().Add(-clusterBackupAttemptReclaimGrace - time.Minute),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	_, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
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
