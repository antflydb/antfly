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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	json "github.com/antflydb/antfly/go/pkg/libaf/json"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/minio/minio-go/v7"
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

const testReservationOwner = "afba-test-reservation-owner"

type cancelingIntegrityBackupStore struct {
	metadata       map[string]*backupMetadata
	blockedStarted chan struct{}
	startOnce      sync.Once
	canceled       atomic.Bool
}

type metadataOnlyHealthBackupStore struct {
	*fileBackupStore
	identityChecks atomic.Int32
}

func (s *metadataOnlyHealthBackupStore) ValidateArtifactIdentity(
	context.Context,
	common.BackupArtifactIntegrity,
) error {
	s.identityChecks.Add(1)
	return errors.New("health validation must not read artifact bodies")
}

func (*cancelingIntegrityBackupStore) EnsureMetadataAbsent(context.Context, string) error {
	return nil
}
func (*cancelingIntegrityBackupStore) ReserveBackupID(context.Context, string, string) error {
	return nil
}
func (*cancelingIntegrityBackupStore) BackupIDReservationOwnedBy(
	context.Context,
	string,
	string,
) (bool, error) {
	return true, nil
}
func (*cancelingIntegrityBackupStore) DeleteMetadata(context.Context, string) error {
	return nil
}
func (*cancelingIntegrityBackupStore) DeleteArtifact(context.Context, string) error {
	return nil
}
func (*cancelingIntegrityBackupStore) ValidateArtifact(context.Context, string) error {
	return nil
}
func (*cancelingIntegrityBackupStore) ValidateArtifactMetadata(
	context.Context,
	string,
	uint64,
) error {
	return nil
}
func (s *cancelingIntegrityBackupStore) ValidateArtifactIdentity(
	ctx context.Context,
	artifact common.BackupArtifactIntegrity,
) error {
	if artifact.Name == "backup-1-1.afb" {
		<-s.blockedStarted
		return errors.New("corrupt artifact")
	}
	s.startOnce.Do(func() { close(s.blockedStarted) })
	<-ctx.Done()
	s.canceled.Store(true)
	return ctx.Err()
}
func (*cancelingIntegrityBackupStore) ReleaseBackupID(
	context.Context,
	string,
	string,
) (bool, error) {
	return true, nil
}
func (*cancelingIntegrityBackupStore) WriteMetadata(
	context.Context,
	string,
	*store.Table,
	common.BackupFormat,
	[]common.BackupArtifactIntegrity,
) error {
	return nil
}
func (s *cancelingIntegrityBackupStore) ReadMetadata(
	_ context.Context,
	id string,
) (*backupMetadata, error) {
	metadata, ok := s.metadata[id]
	if !ok {
		return nil, errors.New("metadata not found")
	}
	return metadata, nil
}
func (*cancelingIntegrityBackupStore) ResolvedLocation() string { return "" }

func (*cleanupOrderBackupStore) EnsureMetadataAbsent(context.Context, string) error { return nil }
func (*cleanupOrderBackupStore) ReserveBackupID(context.Context, string, string) error {
	return nil
}
func (*cleanupOrderBackupStore) BackupIDReservationOwnedBy(
	context.Context,
	string,
	string,
) (bool, error) {
	return true, nil
}
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
func (*cleanupOrderBackupStore) ValidateArtifactMetadata(
	context.Context,
	string,
	uint64,
) error {
	return nil
}
func (*cleanupOrderBackupStore) ValidateArtifactIdentity(
	context.Context,
	common.BackupArtifactIntegrity,
) error {
	return nil
}
func (s *cleanupOrderBackupStore) ReleaseBackupID(
	context.Context,
	string,
	string,
) (bool, error) {
	if s.artifactsDeleted.Load() != s.expectedArtifacts {
		s.phaseViolation.Store(true)
	}
	return true, nil
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

func TestReadOnlyFileBackupStoreRetainsAuthorizedRepositoryRoot(t *testing.T) {
	connectionRoot := t.TempDir()
	repositoryPath := filepath.Join(connectionRoot, "repository")
	require.NoError(t, os.Mkdir(repositoryPath, 0o750))
	const backupID = "backup-1"
	const artifactName = "backup-1-1.backup"
	authorizedArtifact := []byte("authorized artifact")
	require.NoError(t, os.WriteFile(
		filepath.Join(repositoryPath, artifactName),
		authorizedArtifact,
		0o600,
	))
	originalTable := &store.Table{Name: "authorized"}
	require.NoError(t, (&fileBackupStore{location: repositoryPath}).WriteMetadata(
		context.Background(),
		backupID,
		originalTable,
		common.BackupFormatNative,
		nil,
	))
	originalCluster := &ClusterBackupMetadata{
		Version:             clusterBackupMetadataVersion,
		State:               clusterBackupStateComplete,
		BackupID:            backupID,
		Timestamp:           time.Now().UTC(),
		Format:              common.BackupFormatNative,
		ExpectedTableCount:  1,
		CompletedTableCount: 1,
		Tables: []ClusterBackupTableInfo{{
			Name:           originalTable.Name,
			BackupLocation: "file:///repository",
			Status:         "completed",
		}},
	}
	require.NoError(t, writeClusterMetadataToFile(
		context.Background(),
		"file://"+repositoryPath,
		backupID,
		originalCluster,
	))

	outsidePath := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(outsidePath, artifactName),
		[]byte("outside artifact with a different size"),
		0o600,
	))
	outsideTable := &store.Table{Name: "outside"}
	require.NoError(t, (&fileBackupStore{location: outsidePath}).WriteMetadata(
		context.Background(),
		backupID,
		outsideTable,
		common.BackupFormatNative,
		nil,
	))
	outsideCluster := *originalCluster
	outsideCluster.Tables = []ClusterBackupTableInfo{{
		Name:           outsideTable.Name,
		BackupLocation: "file:///outside",
		Status:         "completed",
	}}
	require.NoError(t, writeClusterMetadataToFile(
		context.Background(),
		"file://"+outsidePath,
		backupID,
		&outsideCluster,
	))

	var filesystemConnection common.ConnectionConfig
	require.NoError(t, filesystemConnection.UnmarshalJSON([]byte(fmt.Sprintf(`{
		"kind":"external_io",
		"capabilities":["restore.read"],
		"external_io":{"protocol":"filesystem","root":%q}
	}`, connectionRoot))))
	config := &common.Config{Connections: map[string]common.ConnectionConfig{
		"filesystem": filesystemConnection,
	}}
	metadataStore, err := newBackupStore(
		config,
		"filesystem",
		"restore.read",
		"file:///repository",
	)
	require.NoError(t, err)
	defer closeBackupStore(metadataStore)

	movedPath := filepath.Join(connectionRoot, "repository-moved")
	require.NoError(t, os.Rename(repositoryPath, movedPath))
	if err := os.Symlink(outsidePath, repositoryPath); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	metadata, err := metadataStore.ReadMetadata(context.Background(), backupID)
	require.NoError(t, err)
	require.Equal(t, originalTable.Name, metadata.Table.Name)
	clusterMetadata, err := readClusterMetadataFromBackupStore(
		context.Background(),
		"file://"+repositoryPath,
		nil,
		metadataStore,
		backupID,
	)
	require.NoError(t, err)
	require.Equal(t, originalTable.Name, clusterMetadata.Tables[0].Name)
	require.NoError(t, metadataStore.ValidateArtifactMetadata(
		context.Background(),
		artifactName,
		uint64(len(authorizedArtifact)),
	))
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
	require.NoError(t, backupStore.ValidateArtifactMetadata(
		context.Background(),
		artifact.Name,
		artifact.SizeBytes,
	))
	require.ErrorIs(t, backupStore.ValidateArtifactMetadata(
		context.Background(),
		artifact.Name,
		artifact.SizeBytes+1,
	), common.ErrBackupArtifactIntegrityMismatch)
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

func TestS3BackupStoreArtifactValidationUsesReadOnlyObjectRequests(t *testing.T) {
	body := []byte("portable-artifact")
	digest := sha256.Sum256(body)
	artifact := common.BackupArtifactIntegrity{
		Name:      "backup-1-1.afb",
		SizeBytes: uint64(len(body)),
		SHA256:    hex.EncodeToString(digest[:]),
	}
	var bucketAdmissionRequested atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket" || r.URL.Path == "/bucket/" {
			if r.Method == http.MethodHead {
				bucketAdmissionRequested.Store(true)
				http.Error(w, "bucket admission forbidden", http.StatusForbidden)
				return
			}
			// MinIO may discover the bucket region before an object request.
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(
				`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint></LocationConstraint>`,
			))
			return
		}
		if r.URL.Path != "/bucket/backups/"+artifact.Name {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"artifact-etag"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(body)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	backupStore := &s3BackupStore{s3Config: &common.S3Info{
		Endpoint:         server.URL,
		Bucket:           "bucket",
		Prefix:           "backups",
		AddressingStyle:  common.S3ExternalIoConfigAddressingStylePath,
		CredentialSource: common.AwsCredentialConfigSourceStatic,
		AccessKeyId:      "access",
		SecretAccessKey:  "secret",
	}}
	require.NoError(t, backupStore.ValidateArtifact(context.Background(), artifact.Name))
	require.NoError(t, backupStore.ValidateArtifactMetadata(
		context.Background(),
		artifact.Name,
		artifact.SizeBytes,
	))
	require.ErrorIs(t, backupStore.ValidateArtifactMetadata(
		context.Background(),
		artifact.Name,
		artifact.SizeBytes+1,
	), common.ErrBackupArtifactIntegrityMismatch)
	require.NoError(t, backupStore.ValidateArtifactIdentity(context.Background(), artifact))
	require.False(t, bucketAdmissionRequested.Load())
}

func TestS3BackupReservationReleaseIsOwnerConditional(t *testing.T) {
	var (
		mu              sync.Mutex
		reservationBody []byte
		etagGeneration  uint64
	)
	writeS3Error := func(w http.ResponseWriter, status int, code string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(
			w,
			`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code></Error>`,
			code,
		)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket" || r.URL.Path == "/bucket/" {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(
				`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint></LocationConstraint>`,
			))
			return
		}
		switch r.URL.Path {
		case "/bucket/backup-1-metadata.json":
			writeS3Error(w, http.StatusNotFound, minio.NoSuchKey)
			return
		case "/bucket/backup-1-reservation":
		default:
			http.NotFound(w, r)
			return
		}

		mu.Lock()
		defer mu.Unlock()
		currentETag := fmt.Sprintf("reservation-%d", etagGeneration)
		switch r.Method {
		case http.MethodHead:
			if reservationBody == nil {
				writeS3Error(w, http.StatusNotFound, minio.NoSuchKey)
				return
			}
			w.Header().Set("ETag", `"`+currentETag+`"`)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(reservationBody)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if reservationBody == nil {
				writeS3Error(w, http.StatusNotFound, minio.NoSuchKey)
				return
			}
			if match := strings.Trim(r.Header.Get("If-Match"), `"`); match != currentETag {
				writeS3Error(w, http.StatusPreconditionFailed, minio.PreconditionFailed)
				return
			}
			w.Header().Set("ETag", `"`+currentETag+`"`)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(reservationBody)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			_, _ = w.Write(reservationBody)
		case http.MethodPut:
			if r.Header.Get("If-None-Match") == "*" {
				if reservationBody != nil {
					writeS3Error(w, http.StatusPreconditionFailed, minio.PreconditionFailed)
					return
				}
			} else if match := strings.Trim(r.Header.Get("If-Match"), `"`); match != currentETag {
				writeS3Error(w, http.StatusPreconditionFailed, minio.PreconditionFailed)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, maxBackupReservationBytes+1))
			require.NoError(t, err)
			if lineEnd := strings.Index(string(body), "\r\n"); lineEnd > 0 {
				chunkHeader := string(body[:lineEnd])
				if sizeEnd := strings.IndexByte(chunkHeader, ';'); sizeEnd > 0 {
					decodedSize, parseErr := strconv.ParseUint(
						chunkHeader[:sizeEnd],
						16,
						64,
					)
					require.NoError(t, parseErr)
					chunkStart := lineEnd + 2
					require.LessOrEqual(t, chunkStart+int(decodedSize), len(body))
					body = body[chunkStart : chunkStart+int(decodedSize)]
				}
			}
			reservationBody = append(reservationBody[:0], body...)
			etagGeneration++
			w.Header().Set("ETag", fmt.Sprintf(`"reservation-%d"`, etagGeneration))
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	backupStore := &s3BackupStore{s3Config: &common.S3Info{
		Endpoint:           server.URL,
		Bucket:             "bucket",
		AddressingStyle:    common.S3ExternalIoConfigAddressingStylePath,
		CredentialSource:   common.AwsCredentialConfigSourceStatic,
		AccessKeyId:        "access",
		SecretAccessKey:    "secret",
		BucketProvisioning: common.S3ExternalIoConfigBucketProvisioningRequireExisting,
	}}
	const (
		backupID = "backup-1"
		oldOwner = "afba-old-attempt"
		newOwner = "afba-new-attempt"
	)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), backupID, oldOwner,
	))
	released, err := backupStore.ReleaseBackupID(
		context.Background(), backupID, oldOwner,
	)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), backupID, newOwner,
	))
	released, err = backupStore.ReleaseBackupID(
		context.Background(), backupID, oldOwner,
	)
	require.NoError(t, err)
	require.False(t, released)
	owned, err := backupStore.BackupIDReservationOwnedBy(
		context.Background(), backupID, newOwner,
	)
	require.NoError(t, err)
	require.True(t, owned)
}

func TestClusterBackupArtifactValidationCancelsSiblingTransfers(t *testing.T) {
	const digest = "0000000000000000000000000000000000000000000000000000000000000000"
	firstTable := &store.Table{
		Name:   "first",
		Shards: map[types.ID]*store.ShardConfig{1: {}},
	}
	secondTable := &store.Table{
		Name:   "second",
		Shards: map[types.ID]*store.ShardConfig{2: {}},
	}
	backupStore := &cancelingIntegrityBackupStore{
		metadata: map[string]*backupMetadata{
			tableBackupMetadataID(firstTable.Name, "backup-1"): {
				Version: backupMetadataVersion,
				Format:  common.BackupFormatPortable,
				Table:   firstTable,
				Artifacts: []common.BackupArtifactIntegrity{{
					Name: "backup-1-1.afb", SizeBytes: 1, SHA256: digest,
				}},
			},
			tableBackupMetadataID(secondTable.Name, "backup-1"): {
				Version: backupMetadataVersion,
				Format:  common.BackupFormatPortable,
				Table:   secondTable,
				Artifacts: []common.BackupArtifactIntegrity{{
					Name: "backup-1-2.afb", SizeBytes: 1, SHA256: digest,
				}},
			},
		},
		blockedStarted: make(chan struct{}),
	}
	err := validateClusterBackupArtifacts(
		context.Background(),
		backupStore,
		&ClusterBackupMetadata{
			Version:             clusterBackupMetadataVersion,
			State:               clusterBackupStateComplete,
			BackupID:            "backup-1",
			Format:              common.BackupFormatPortable,
			ExpectedTableCount:  2,
			CompletedTableCount: 2,
			Tables: []ClusterBackupTableInfo{
				{
					Name:           firstTable.Name,
					Status:         "completed",
					ShardCount:     1,
					BackupLocation: "file:///backups/first.json",
				},
				{
					Name:           secondTable.Name,
					Status:         "completed",
					ShardCount:     1,
					BackupLocation: "file:///backups/second.json",
				},
			},
		},
	)
	require.ErrorContains(t, err, "corrupt artifact")
	require.True(t, backupStore.canceled.Load())
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", testReservationOwner,
	))
	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(context.Background(), "backup-1", "afba-other-owner"),
		ErrBackupAlreadyExists,
	)
}

func TestFileBackupStoreCleanupReleasesReservationAfterArtifactsAreRemoved(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", testReservationOwner,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-1.tar.zst"),
		[]byte("artifact"),
		0o600,
	))

	require.NoError(t, backupStore.DeleteArtifact(context.Background(), "backup-1-1.tar.zst"))
	require.NoError(t, backupStore.DeleteMetadata(context.Background(), "backup-1"))
	released, err := backupStore.ReleaseBackupID(
		context.Background(), "backup-1", testReservationOwner,
	)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-retry-owner",
	))
}

func TestBackupAttemptCleanupRemovesCommitRecordsBeforeArtifacts(t *testing.T) {
	backupStore := &cleanupOrderBackupStore{
		expectedMetadata:  2,
		expectedArtifacts: 2,
	}
	require.NoError(t, cleanupBackupAttempt(
		backupStore,
		"backup-1",
		testReservationOwner,
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", testReservationOwner,
	))
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
		backupStore.ReserveBackupID(context.Background(), "backup-1", "afba-other-owner"),
		ErrBackupAlreadyExists,
	)
	released, err := backupStore.ReleaseBackupID(
		context.Background(), "backup-1", testReservationOwner,
	)
	require.NoError(t, err)
	require.True(t, released)
}

func TestStaleBackupCleanupCannotDeleteNewRetryObjects(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	const (
		backupID = "backup-1"
		oldOwner = "afba-old-attempt"
		newOwner = "afba-new-attempt"
		artifact = "backup-1-1.afb"
	)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), backupID, oldOwner,
	))
	released, err := backupStore.ReleaseBackupID(
		context.Background(), backupID, oldOwner,
	)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), backupID, newOwner,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifact),
		[]byte("new retry artifact"),
		0o600,
	))

	err = cleanupBackupAttempt(
		backupStore,
		backupID,
		oldOwner,
		nil,
		[]string{artifact},
	)
	require.ErrorContains(t, err, "no longer owns")
	require.FileExists(t, filepath.Join(root, artifact))
	owned, err := backupStore.BackupIDReservationOwnedBy(
		context.Background(), backupID, newOwner,
	)
	require.NoError(t, err)
	require.True(t, owned)
	released, err = backupStore.ReleaseBackupID(
		context.Background(), backupID, oldOwner,
	)
	require.NoError(t, err)
	require.False(t, released)
}

func TestLegacyAnonymousReservationFailsClosed(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "backup-1-reservation"),
		[]byte("reserved\n"),
		0o600,
	))
	owned, err := backupStore.BackupIDReservationOwnedBy(
		context.Background(), "backup-1", testReservationOwner,
	)
	require.NoError(t, err)
	require.False(t, owned)
	released, err := backupStore.ReleaseBackupID(
		context.Background(), "backup-1", testReservationOwner,
	)
	require.NoError(t, err)
	require.False(t, released)
	require.ErrorIs(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", testReservationOwner,
	), ErrBackupAlreadyExists)
}

func TestFileBackupStoreDoesNotPublishAfterCancellation(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(
		t,
		backupStore.ReserveBackupID(ctx, "reserved", testReservationOwner),
		context.Canceled,
	)
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

	markerDigest, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	_, err = publishClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		ClusterBackupAttemptHead{
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(markerDigest[:]),
		},
	)
	require.NoError(t, err)
	healthStore := &metadataOnlyHealthBackupStore{fileBackupStore: backupStore}
	validatedMeta, err := validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		healthStore,
	)
	require.NoError(t, err)
	require.NotNil(t, validatedMeta)
	require.Equal(t, attempt.BackupID, validatedMeta.BackupID)
	require.Zero(t, healthStore.identityChecks.Load())
	owned, err := transitionClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		clusterBackupAttemptStateCommitted,
	)
	require.NoError(t, err)
	require.True(t, owned)
	validatedMeta, err = validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		healthStore,
	)
	require.NoError(t, err)
	require.NotNil(t, validatedMeta)
	require.Equal(t, attempt.BackupID, validatedMeta.BackupID)
	require.Zero(t, healthStore.identityChecks.Load())

	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifactNames[0]),
		[]byte("artifacU"),
		0o600,
	))
	validatedMeta, err = validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		healthStore,
	)
	require.NoError(t, err)
	require.NotNil(t, validatedMeta)
	require.Equal(t, attempt.BackupID, validatedMeta.BackupID)
	require.Zero(t, healthStore.identityChecks.Load())

	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifactNames[0]),
		[]byte("short"),
		0o600,
	))
	_, err = validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		healthStore,
	)
	require.ErrorIs(t, err, common.ErrBackupArtifactIntegrityMismatch)
}

func TestNewestClusterBackupRepositoryRejectsMarkerWithoutHead(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-headless",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC(),
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
	validatedMeta, err := validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		&fileBackupStore{location: root},
	)
	require.Nil(t, validatedMeta)
	require.ErrorContains(t, err, "without an authoritative head")
}

func TestNewestClusterBackupRepositoryRejectsFailedHead(t *testing.T) {
	root := t.TempDir()
	attempt := &ClusterBackupAttempt{
		Version:            clusterBackupAttemptVersion,
		AttemptID:          "afba-failed",
		BackupID:           "backup-1",
		CreatedAt:          time.Now().UTC(),
		Format:             common.BackupFormatPortable,
		ExpectedTableCount: 1,
		TableNames:         []string{"documents"},
		MetadataIDs:        []string{"documents-backup-1"},
		ArtifactNames:      []string{"backup-1-1.afb"},
	}
	markerDigest, err := writeClusterBackupAttempt(
		context.Background(),
		"file://"+root,
		nil,
		attempt,
	)
	require.NoError(t, err)
	_, err = publishClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		ClusterBackupAttemptHead{
			AttemptID:    attempt.AttemptID,
			BackupID:     attempt.BackupID,
			MarkerSHA256: hex.EncodeToString(markerDigest[:]),
		},
	)
	require.NoError(t, err)
	owned, err := transitionClusterBackupAttemptHead(
		context.Background(),
		"file://"+root,
		nil,
		attempt.AttemptID,
		clusterBackupAttemptStateFailed,
	)
	require.NoError(t, err)
	require.True(t, owned)
	validatedMeta, err := validateNewestClusterBackupRepositoryMetadata(
		context.Background(),
		"file://"+root,
		nil,
		&fileBackupStore{location: root},
	)
	require.Nil(t, validatedMeta)
	require.ErrorContains(t, err, clusterBackupAttemptStateFailed)
}

func TestClusterMetadataObjectBackupIDIsRepositoryScoped(t *testing.T) {
	testCases := []struct {
		name      string
		prefix    string
		objectKey string
		backupID  string
		ok        bool
	}{
		{
			name:      "direct child",
			prefix:    "tenant/repository",
			objectKey: "tenant/repository/backup-1-cluster-metadata.json",
			backupID:  "backup-1",
			ok:        true,
		},
		{
			name:      "trailing slash",
			prefix:    "tenant/repository/",
			objectKey: "tenant/repository/backup-1-cluster-metadata.json",
			backupID:  "backup-1",
			ok:        true,
		},
		{
			name:      "empty prefix direct child",
			objectKey: "backup-1-cluster-metadata.json",
			backupID:  "backup-1",
			ok:        true,
		},
		{
			name:      "sibling byte prefix",
			prefix:    "tenant/repository",
			objectKey: "tenant/repository-other/backup-1-cluster-metadata.json",
		},
		{
			name:      "nested child",
			prefix:    "tenant/repository",
			objectKey: "tenant/repository/nested/backup-1-cluster-metadata.json",
		},
		{
			name:      "invalid backup ID",
			prefix:    "tenant/repository",
			objectKey: "tenant/repository/../-cluster-metadata.json",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			backupID, ok := clusterMetadataBackupIDFromObjectKey(
				testCase.prefix,
				testCase.objectKey,
			)
			require.Equal(t, testCase.ok, ok)
			require.Equal(t, testCase.backupID, backupID)
		})
	}
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-stale",
	))
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-retry",
	))
}

func TestStaleClusterBackupAttemptCannotReclaimNewRetry(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), attempt.BackupID, attempt.AttemptID,
	))
	released, err := backupStore.ReleaseBackupID(
		context.Background(), attempt.BackupID, attempt.AttemptID,
	)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), attempt.BackupID, "afba-retry",
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, attempt.ArtifactNames[0]),
		[]byte("retry artifact"),
		0o600,
	))
	_, err = writeClusterBackupAttempt(
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
	require.FileExists(t, filepath.Join(root, attempt.ArtifactNames[0]))
	owned, err := backupStore.BackupIDReservationOwnedBy(
		context.Background(), attempt.BackupID, "afba-retry",
	)
	require.NoError(t, err)
	require.True(t, owned)
}

func TestExpiredLeasedClusterBackupAttemptIsFencedAndReclaimed(t *testing.T) {
	root := t.TempDir()
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-stale",
	))
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-retry",
	))
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

func TestStaleClusterBackupAttemptReclamationHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reclaimed, err := reclaimStaleClusterBackupAttempt(
		ctx,
		&cleanupOrderBackupStore{},
		"file://"+t.TempDir(),
		nil,
		&ClusterBackupAttempt{
			Version:            clusterBackupAttemptVersion,
			AttemptID:          "afba-canceled-reclaim",
			BackupID:           "backup-1",
			CreatedAt:          time.Now().UTC(),
			Format:             common.BackupFormatPortable,
			ExpectedTableCount: 1,
			TableNames:         []string{"documents"},
			MetadataIDs:        []string{"documents-backup-1"},
			ArtifactNames:      []string{"backup-1-1.afb"},
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, reclaimed)
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-authoritative",
	))
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-retry",
	))
}

func TestExpiredCommittedHeadRetainsCorruptionAuthorityWithoutAggregate(t *testing.T) {
	root := t.TempDir()
	location := "file://" + root
	backupStore := &fileBackupStore{location: root}
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-committed-head",
	))
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
		backupStore.ReserveBackupID(
			context.Background(), attempt.BackupID, "afba-retry",
		),
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
	require.NoError(t, backupStore.ReserveBackupID(
		context.Background(), "backup-1", "afba-stale",
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
		backupStore.ReserveBackupID(
			context.Background(), "backup-1", "afba-retry",
		),
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
