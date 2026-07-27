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

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/schema"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/snapstore"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	blevesearch "github.com/blevesearch/bleve/v2/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func signalOnCall(call *mock.Call) <-chan struct{} {
	done := make(chan struct{})
	call.Run(func(args mock.Arguments) {
		close(done)
	})
	return done
}

var _ ShardIface = (*MockShard)(nil)

const bleveCompositeSortSentinel = "\U0010ffff\U0010ffff\U0010ffff"

// MockShard is a mock type for the Shard interface
type MockShard struct {
	mock.Mock

	raftNode *MockRaftNode // Add this if Shard methods interact with raftNode directly
}

func (m *MockShard) Backup(ctx context.Context, backup common.BackupConfig) error {
	args := m.Called(ctx, backup.Location, backup.BackupID)
	return args.Error(0)
}

func (m *MockShard) ExportPortable(ctx context.Context, w io.Writer) error {
	args := m.Called(ctx, w)
	return args.Error(0)
}

func (m *MockShard) ImportPortable(ctx context.Context, r io.Reader) error {
	args := m.Called(ctx, r)
	return args.Error(0)
}

func (m *MockShard) PrepareSplit(ctx context.Context, splitKey []byte) error {
	args := m.Called(ctx, splitKey)
	return args.Error(0)
}

func (m *MockShard) Split(ctx context.Context, newShardID uint64, splitKey []byte) error {
	args := m.Called(ctx, newShardID, splitKey)
	return args.Error(0)
}

func (m *MockShard) FinalizeSplit(ctx context.Context, newRangeEnd []byte) error {
	args := m.Called(ctx, newRangeEnd)
	return args.Error(0)
}

func (m *MockShard) RollbackSplit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockShard) SetMergeState(ctx context.Context, state *db.MergeState) error {
	args := m.Called(ctx, state)
	return args.Error(0)
}

func (m *MockShard) FinalizeMerge(ctx context.Context, byteRange [2][]byte) error {
	args := m.Called(ctx, byteRange)
	return args.Error(0)
}

func (m *MockShard) AddIndex(
	ctx context.Context,
	config indexes.IndexConfig,
) error {
	args := m.Called(ctx, config)
	return args.Error(0)
}

func (m *MockShard) DropIndex(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockShard) SetRange(ctx context.Context, byteRange [2][]byte) error {
	args := m.Called(ctx, byteRange)
	return args.Error(0)
}

func (m *MockShard) UpdateSchema(ctx context.Context, schema *schema.TableSchema) error {
	args := m.Called(ctx, schema)
	return args.Error(0)
}

func (m *MockShard) FindMedianKey() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockShard) Search(ctx context.Context, query []byte) ([]byte, error) {
	args := m.Called(ctx, query)
	res, _ := args.Get(0).([]byte)
	return res, args.Error(1)
}

func (m *MockShard) SearchTyped(ctx context.Context, req *indexes.RemoteIndexSearchRequest) (*indexes.RemoteIndexSearchResult, error) {
	args := m.Called(ctx, req)
	res, _ := args.Get(0).(*indexes.RemoteIndexSearchResult)
	return res, args.Error(1)
}

func (m *MockShard) Delete(ctx context.Context, key []byte) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockShard) Lookup(ctx context.Context, key string) (map[string]any, error) {
	args := m.Called(ctx, key)
	res, _ := args.Get(0).(map[string]any)
	return res, args.Error(1)
}

func (m *MockShard) GetTimestamp(key string) (uint64, error) {
	args := m.Called(key)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockShard) ExportRangeChunk(
	ctx context.Context,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	args := m.Called(ctx, startKey, endKey, afterKey, limit)
	res, _ := args.Get(0).([][2][]byte)
	nextKey, _ := args.Get(1).([]byte)
	return res, nextKey, args.Bool(2), args.Error(3)
}

func (m *MockShard) ListMergeDeltaEntriesAfter(afterSeq uint64) ([]*db.MergeDeltaEntry, error) {
	args := m.Called(afterSeq)
	res, _ := args.Get(0).([]*db.MergeDeltaEntry)
	return res, args.Error(1)
}

func (m *MockShard) Batch(ctx context.Context, batch *db.BatchOp, proposeOnly bool) error {
	args := m.Called(ctx, batch, proposeOnly)
	return args.Error(0)
}

func (m *MockShard) ApplyMergeChunk(ctx context.Context, batch *db.BatchOp) error {
	args := m.Called(ctx, batch)
	return args.Error(0)
}

func (m *MockShard) Scan(
	ctx context.Context,
	fromKey []byte,
	toKey []byte,
	opts db.ScanOptions,
) (*db.ScanResult, error) {
	args := m.Called(ctx, fromKey, toKey, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.ScanResult), args.Error(1)
}

func (m *MockShard) ProposeConfChange(
	ctx context.Context,
	cc raftpb.ConfChange,
) error { // Assuming raftpb is imported or not needed for this test
	m.Called(ctx, cc)
	return nil
}

func (m *MockShard) ApplyConfChange(
	ctx context.Context,
	cc raftpb.ConfChange,
) error {
	args := m.Called(ctx, cc)
	return args.Error(0)
}

func (s *MockShard) IsIDRemoved(id uint64) bool {
	return s.raftNode.IsIDRemoved(id)
}

func (s *MockShard) TransferLeadership(ctx context.Context, target types.ID) {
	s.raftNode.TransferLeadership(ctx, target)
}

func (m *MockShard) SyncWriteOp(ctx context.Context, op *db.Op) error {
	args := m.Called(ctx, op)
	return args.Error(0)
}

func (m *MockShard) GetTransactionStatus(ctx context.Context, txnID []byte) (int32, error) {
	args := m.Called(ctx, txnID)
	return args.Get(0).(int32), args.Error(1)
}

func (m *MockShard) GetCommitVersion(ctx context.Context, txnID []byte) (uint64, error) {
	args := m.Called(ctx, txnID)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockShard) ListTxnRecords(ctx context.Context) ([]db.TxnRecord, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]db.TxnRecord), args.Error(1)
}

func (m *MockShard) ListTxnIntents(ctx context.Context) ([]db.TxnIntent, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]db.TxnIntent), args.Error(1)
}

func (m *MockShard) GetEdges(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error) {
	args := m.Called(ctx, indexName, key, edgeType, direction)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]indexes.Edge), args.Error(1)
}

func (m *MockShard) TraverseEdges(ctx context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error) {
	args := m.Called(ctx, indexName, startKey, rules)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*indexes.TraversalResult), args.Error(1)
}

func (m *MockShard) GetNeighbors(ctx context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error) {
	args := m.Called(ctx, indexName, key, edgeType, direction)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*indexes.TraversalResult), args.Error(1)
}

func (m *MockShard) FindShortestPath(ctx context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error) {
	args := m.Called(ctx, indexName, source, target, edgeTypes, direction, weightMode, maxDepth, minWeight, maxWeight)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*indexes.Path), args.Error(1)
}

// MockRaftNode is a mock type for the raftNode field within Shard (if accessed directly)
// For IsIDRemoved and TransferLeadership
type MockRaftNode struct {
	mock.Mock
}

func (m *MockRaftNode) IsIDRemoved(id uint64) bool {
	args := m.Called(id)
	return args.Bool(0)
}

func (m *MockRaftNode) TransferLeadership(ctx context.Context, target types.ID) {
	m.Called(ctx, target)
}

func (m *MockRaftNode) Shutdown() {
	m.Called()
}

// Ensure MockShard has the raftNode field if methods like IsIDRemoved or TransferLeadership are called on shard.raftNode
// If these methods are part of an interface that Shard implements, then MockShard should implement them.
// For handleBackup, we don't directly need MockRaftNode, but it's good to have for other tests.
// So, we'll attach a new MockRaftNode to MockShard instances if necessary for tests that use those methods.
// For Backup, it's not used.

// MockStore is a mock type for the Store
type MockStore struct {
	mock.Mock

	logger *zap.Logger
	nodeID types.ID
}

func (m *MockStore) Shard(id types.ID) (ShardIface, bool) {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil, args.Bool(1)
	}
	return args.Get(0).(ShardIface), args.Bool(1)
}

func (m *MockStore) Status() *StoreStatus {
	args := m.Called()
	return args.Get(0).(*StoreStatus)
}

func (m *MockStore) StopRaftGroup(shardID types.ID) error {
	args := m.Called(shardID)
	return args.Error(0)
}

func (m *MockStore) StartRaftGroup(
	shardID types.ID,
	peers []common.Peer,
	join bool,
	config *ShardStartConfig,
) error {
	args := m.Called(shardID, peers, join, config)
	// Some tests use .Return() with no explicit value for a nil error.
	if len(args) == 0 {
		return nil
	}
	return args.Error(0)
}

func (m *MockStore) ID() types.ID {
	return m.nodeID
}

func (m *MockStore) Scan(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	opts db.ScanOptions,
) (*db.ScanResult, error) {
	args := m.Called(ctx, shardID, fromKey, toKey, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.ScanResult), args.Error(1)
}

func TestHandleBackup_FileLocation_Success(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	baseDir := t.TempDir()
	mockStore := &MockStore{
		logger: logger,
		nodeID: types.ID(1),
	} // nodeID needed for StoreAPI internally for some ops

	api := &StoreAPI{
		logger: logger,
		store:  mockStore,
		antflyConfig: &common.Config{
			Storage: common.StorageConfig{
				Local: common.LocalStorageConfig{
					BaseDir: baseDir,
				},
			},
		},
	}
	mux := api.setupRoutes() // Important to initialize mux

	shardID := types.ID(123)
	backupID := "test-backup-1"
	tempDir := filepath.Join(baseDir, "backup-location") // local file backup roots must be under baseDir
	require.NoError(t, os.MkdirAll(tempDir, 0o755))

	backupLocation := "file://" + tempDir
	// expectedBackupFileName := fmt.Sprintf("%s-%s.tar.sz", shardID, backupID)
	// fullBackupFilePath := filepath.Join(tempDir, expectedBackupFileName)

	// Simulate the backup file being created by the Shard.Backup method
	mockShard.On("Backup", mock.Anything, "", fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(nil).
		Run(func(args mock.Arguments) {
			snapDir := common.SnapDir(baseDir, shardID, mockStore.ID())
			filePath := filepath.Join(snapDir, fmt.Sprintf("%s-%s.tar.zst", backupID, shardID))
			// Create a dummy backup file that would be created by shard.Backup
			os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
			err := os.WriteFile(filePath, []byte("backup data"), 0o644)
			require.NoError(t, err)
		})
	mockStore.On("Shard", shardID).Return(mockShard, true)

	backupReq := common.BackupConfig{
		BackupID: backupID,
		Location: backupLocation,
		Format:   common.BackupFormatNative,
	}
	body, err := json.Marshal(backupReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "handler returned wrong status code")

	// Verify headers for file download
	assert.Equal(t, "attachment; filename=file.dat", rr.Header().Get("Content-Disposition"))
	assert.Equal(t, "application/octet-stream", rr.Header().Get("Content-Type"))

	// Verify file content streamed in response
	responseBody, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	assert.Equal(t, "backup data", string(responseBody), "handler returned unexpected body content")
	assert.Equal(t, strconv.Itoa(len("backup data")), rr.Header().Get("Content-Length"))

	mockShard.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

func TestHandleBackup_DefaultsToPortable(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	baseDir := t.TempDir()
	mockStore := &MockStore{
		logger: logger,
		nodeID: types.ID(1),
	}
	api := &StoreAPI{
		logger: logger,
		store:  mockStore,
		antflyConfig: &common.Config{
			Storage: common.StorageConfig{
				Local: common.LocalStorageConfig{
					BaseDir: baseDir,
				},
			},
		},
	}

	shardID := types.ID(123)
	backupID := "test-default-portable"
	tempDir := filepath.Join(baseDir, "portable-backup-location")
	require.NoError(t, os.MkdirAll(tempDir, 0o755))
	backupLocation := "file://" + tempDir

	mockShard.On("ExportPortable", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			_, err := args.Get(1).(io.Writer).Write([]byte("portable backup data"))
			require.NoError(t, err)
		})
	mockStore.On("Shard", shardID).Return(mockShard, true)

	backupReq := common.BackupConfig{
		BackupID: backupID,
		Location: backupLocation,
	}
	body, err := json.Marshal(backupReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.setupRoutes().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "attachment; filename="+common.ShardPortableBackupFileName(backupID, shardID), rr.Header().Get("Content-Disposition"))
	assert.Equal(t, "portable backup data", rr.Body.String())
	assert.Equal(t, common.ShardPortableBackupFileName(backupID, shardID), rr.Header().Get(common.BackupArtifactNameHeader))
	assert.Equal(t, strconv.Itoa(len("portable backup data")), rr.Header().Get(common.BackupArtifactSizeHeader))
	assert.Equal(
		t,
		"789146fe47e143e9e33e1d83942d0d019baf0f6a4465df201892035461cd1550",
		rr.Header().Get(common.BackupArtifactSHA256Header),
	)

	// The store streams from a temporary durable file. The coordinator owns
	// publication into the authorized destination and the store must not retain
	// a second copy after the response completes.
	entries, err := os.ReadDir(common.SnapDir(baseDir, shardID, mockStore.ID()))
	require.NoError(t, err)
	require.Empty(t, entries)
	_, err = os.Stat(filepath.Join(tempDir, common.ShardPortableBackupFileName(backupID, shardID)))
	require.ErrorIs(t, err, os.ErrNotExist)

	mockShard.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

func TestHandleBackup_MissingShardID(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := &MockStore{logger: logger}
	sapi := &StoreAPI{logger: logger, store: mockStore}
	api := sapi.setupRoutes()

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", strings.NewReader(`{}`))
	// Missing X-Raft-Shard-ID
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to get shard ID")
}

func TestHandleBackup_InvalidShardID(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := &MockStore{logger: logger}
	sapi := &StoreAPI{logger: logger, store: mockStore}
	api := sapi.setupRoutes()

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", strings.NewReader(`{}`))
	req.Header.Set("X-Raft-Shard-Id", "invalid-id")
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to get shard ID")
}

func TestHandleBackup_ShardNotFound(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := &MockStore{logger: logger}
	sapi := &StoreAPI{logger: logger, store: mockStore}
	api := sapi.setupRoutes()

	shardID := types.ID(404)
	mockStore.On("Shard", shardID).Return(nil, false) // Shard not found

	req := httptest.NewRequest(
		http.MethodPost,
		"/shard/backup",
		strings.NewReader(`{"backup_id": "test"}`),
	)
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Contains(t, rr.Body.String(), "Shard not found")
	mockStore.AssertExpectations(t)
}

func TestHandleBackup_BadRequestBody(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	mockStore := &MockStore{logger: logger}
	sapi := &StoreAPI{logger: logger, store: mockStore}
	api := sapi.setupRoutes()

	shardID := types.ID(123)
	mockStore.On("Shard", shardID).Return(mockShard, true) // Shard exists

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", strings.NewReader(`{invalid-json`))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to read request body")
	mockStore.AssertExpectations(t) // Shard should be called to validate existence
}

func TestHandleBackup_ShardBackupFails(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	mockStore := &MockStore{logger: logger}
	baseDir := t.TempDir()
	sapi := &StoreAPI{logger: logger, store: mockStore, antflyConfig: &common.Config{Storage: common.StorageConfig{Local: common.LocalStorageConfig{BaseDir: baseDir}}}}
	api := sapi.setupRoutes()

	shardID := types.ID(123)
	backupID := "test-backup-fail"
	tempDir := filepath.Join(baseDir, "backup-fail-location")
	require.NoError(t, os.MkdirAll(tempDir, 0o755))
	backupLocation := "file://" + tempDir

	mockStore.On("Shard", shardID).Return(mockShard, true)
	mockShard.On("Backup", mock.Anything, "", fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(errors.New("shard backup failed"))

	backupReq := common.BackupConfig{
		BackupID: backupID,
		Location: backupLocation,
		Format:   common.BackupFormatNative,
	}
	body, err := json.Marshal(backupReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(
		t,
		rr.Body.String(),
		"Failed to backup: shard backup failed",
	) // Original code uses "Failed to backup"

	mockShard.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

func TestHandleBackup_FileLocation_BackupFileNotCreated(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	mockStore := &MockStore{logger: logger}
	baseDir := t.TempDir()
	api := &StoreAPI{logger: logger, store: mockStore, antflyConfig: &common.Config{Storage: common.StorageConfig{Local: common.LocalStorageConfig{BaseDir: baseDir}}}}

	shardID := types.ID(789)
	backupID := "test-backup-nofile"
	tempDir := filepath.Join(baseDir, "backup-missing-location")
	require.NoError(t, os.MkdirAll(tempDir, 0o755))
	backupLocation := "file://" + tempDir

	// Simulate Shard.Backup succeeding but somehow not creating the file (or it gets deleted)
	mockShard.On("Backup", mock.Anything, "", fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(nil)
	mockStore.On("Shard", shardID).Return(mockShard, true)

	backupReq := common.BackupConfig{
		BackupID: backupID,
		Location: backupLocation,
		Format:   common.BackupFormatNative,
	}
	body, err := json.Marshal(backupReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.setupRoutes().ServeHTTP(rr, req)

	// If the file isn't found, os.Open will fail.
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to open backup file")

	mockShard.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

func TestHandleBackup_NonFileLocation_Success(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	mockStore := &MockStore{logger: logger}

	api := &StoreAPI{logger: logger, store: mockStore}

	shardID := types.ID(101)
	backupID := "test-backup-s3"
	s3Location := "s3://mybucket/backups/" // Example S3 location

	mockStore.On("Shard", shardID).Return(mockShard, true)
	// For non-"file://" locations, Backup is called, but no file streaming is attempted.
	mockShard.On("Backup", mock.Anything, s3Location, fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(nil)

	backupReq := common.BackupConfig{
		BackupID: backupID,
		Location: s3Location,
		Format:   common.BackupFormatNative,
	}
	body, err := json.Marshal(backupReq)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.setupRoutes().ServeHTTP(rr, req)

	assert.Equal(
		t,
		http.StatusOK,
		rr.Code,
		"handler returned wrong status code for non-file location",
	)
	// No file content should be in the body for S3 or other non-file locations handled this way
	assert.Empty(t, rr.Body.String(), "handler returned unexpected body for non-file location")

	mockShard.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

// Minimal raftpb.ConfChange for MockShard ProposeConfChange
type ConfChange struct {
	Type    uint32 // Placeholder for actual ConfChangeType
	NodeID  uint64
	Context []byte
}

const (
	// Placeholder values for ConfChangeType if needed by mocks
	ConfChangeAddNode    = 0
	ConfChangeRemoveNode = 1
)

// Ensure MockShard.raftNode is initialized in tests that need it.
// For example, in TestHandleIsIDRemoved and TestHandleTransferLeadership, you would set:
// mockShard.raftNode = new(MockRaftNode)
// and then set expectations on mockShard.raftNode.
// For handleBackup, this field is not directly used by the handler via the shard mock.

func setupStoreAPI(t *testing.T, storeNodeID types.ID) (http.Handler, *MockStore, string) {
	logger := zaptest.NewLogger(t)
	mockStore := &MockStore{logger: logger, nodeID: storeNodeID}
	baseDir := t.TempDir()
	var filesystemConnection common.ConnectionConfig
	require.NoError(t, filesystemConnection.UnmarshalJSON([]byte(fmt.Sprintf(`{
		"kind":"external_io",
		"capabilities":["backup.write","restore.read"],
		"external_io":{"protocol":"filesystem","root":%q}
	}`, baseDir))))
	api := &StoreAPI{
		logger: logger,
		store:  mockStore,
		antflyConfig: &common.Config{
			Connections: map[string]common.ConnectionConfig{
				"filesystem": filesystemConnection,
			},
			Storage: common.StorageConfig{
				Local: common.LocalStorageConfig{
					BaseDir: baseDir,
				},
			},
		},
		startingShards: make(map[types.ID]struct{}),
	}
	return api.setupRoutes(), mockStore, baseDir
}

func TestHandleSearch_PreservesCompositeSortCursorAndTokens(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	shardID := types.ID(301)
	mockShard := &MockShard{}

	reqBody := indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "tags"}},
			Limit:   2,
			SearchAfter: []any{
				"bravo",
			},
		},
	}
	jsonBody, err := json.Marshal(reqBody)
	require.NoError(t, err)

	respBytes, err := json.Marshal(indexes.RemoteIndexSearchResult{
		BleveSearchResult: &bleve.SearchResult{
			Hits: []*blevesearch.DocumentMatch{
				{ID: "doc3", Sort: []string{"charlie"}},
			},
		},
	})
	require.NoError(t, err)

	mockStore.On("Shard", shardID).Return(mockShard, true)
	mockShard.On("Search", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var forwarded indexes.RemoteIndexSearchRequest
		require.NoError(t, json.Unmarshal(args.Get(1).([]byte), &forwarded))
		require.Equal(t, []any{"bravo"}, forwarded.BlevePagingOpts.SearchAfter)
		require.Equal(t, "tags", forwarded.BlevePagingOpts.OrderBy[0].Field)
	}).Return(respBytes, nil)

	req := httptest.NewRequest(http.MethodPost, "/search", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.BleveSearchResult)
	require.Len(t, resp.BleveSearchResult.Hits, 1)
	assert.Equal(t, "doc3", resp.BleveSearchResult.Hits[0].ID)
	assert.Equal(t, []string{"charlie"}, resp.BleveSearchResult.Hits[0].Sort)
	mockStore.AssertExpectations(t)
	mockShard.AssertExpectations(t)
}

func TestHandleSearch_PreservesSyntheticCompositeSortSentinel(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	shardID := types.ID(302)
	mockShard := &MockShard{}

	reqBody := indexes.RemoteIndexSearchRequest{
		Limit: 10,
		BlevePagingOpts: indexes.FullTextPagingOptions{
			OrderBy: []indexes.SortField{{Field: "mixed_scalar"}},
			Limit:   2,
			SearchAfter: []any{
				bleveCompositeSortSentinel,
			},
		},
	}
	jsonBody, err := json.Marshal(reqBody)
	require.NoError(t, err)

	respBytes, err := json.Marshal(indexes.RemoteIndexSearchResult{
		BleveSearchResult: &bleve.SearchResult{
			Hits: []*blevesearch.DocumentMatch{
				{ID: "doc1", Sort: []string{bleveCompositeSortSentinel}},
				{ID: "doc2", Sort: []string{bleveCompositeSortSentinel}},
			},
		},
	})
	require.NoError(t, err)

	mockStore.On("Shard", shardID).Return(mockShard, true)
	mockShard.On("Search", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var forwarded indexes.RemoteIndexSearchRequest
		require.NoError(t, json.Unmarshal(args.Get(1).([]byte), &forwarded))
		require.Equal(t, []any{bleveCompositeSortSentinel}, forwarded.BlevePagingOpts.SearchAfter)
	}).Return(respBytes, nil)

	req := httptest.NewRequest(http.MethodPost, "/search", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp indexes.RemoteIndexSearchResult
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.BleveSearchResult)
	require.Len(t, resp.BleveSearchResult.Hits, 2)
	for _, hit := range resp.BleveSearchResult.Hits {
		assert.Equal(t, []string{bleveCompositeSortSentinel}, hit.Sort)
	}
	mockStore.AssertExpectations(t)
	mockShard.AssertExpectations(t)
}

func TestHandleStartShard_Success_JSON(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1)) // Store Node ID 1
	newShardID := types.ID(100)

	reqBody := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("a"), []byte("b")},
		},
		Peers: []common.Peer{{ID: 1}},
		Join:  false,
	}
	jsonBody, _ := json.Marshal(reqBody)

	mockStore.On("Shard", newShardID).Return(nil, false) // Shard does not exist
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, reqBody.Peers, reqBody.Join, mock.MatchedBy(func(config *ShardStartConfig) bool {
		return config.InitWithDBArchive == "" &&
			assert.ObjectsAreEqual(config.ShardConfig, reqBody.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	<-started
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_RejectsDuplicateInFlightStart(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(106)

	reqBody := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("m"), []byte("n")},
		},
		Peers: []common.Peer{{ID: 7}},
	}
	jsonBody, _ := json.Marshal(reqBody)

	started := make(chan struct{})
	unblock := make(chan struct{})

	mockStore.On("Shard", newShardID).Return(nil, false).Once()
	mockStore.On("StartRaftGroup", newShardID, reqBody.Peers, reqBody.Join, mock.Anything).
		Run(func(args mock.Arguments) {
			close(started)
			<-unblock
		}).
		Return(nil).
		Once()

	firstReq := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	firstReq.Header.Set("X-Raft-Shard-Id", newShardID.String())
	firstReq.Header.Set("Content-Type", "application/json")

	firstResp := httptest.NewRecorder()
	api.ServeHTTP(firstResp, firstReq)
	assert.Equal(t, http.StatusOK, firstResp.Code)

	<-started

	secondReq := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	secondReq.Header.Set("X-Raft-Shard-Id", newShardID.String())
	secondReq.Header.Set("Content-Type", "application/json")

	secondResp := httptest.NewRecorder()
	api.ServeHTTP(secondResp, secondReq)
	assert.Equal(t, http.StatusBadRequest, secondResp.Code)
	assert.Contains(t, secondResp.Body.String(), "Shard already exists")

	close(unblock)
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Success_Multipart_PayloadOnly(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(101)

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{ByteRange: types.Range{[]byte("c"), []byte("d")}},
		Peers:       []common.Peer{{ID: 2}},
		Join:        true,
	}
	payloadBytes, _ := json.Marshal(startReq)

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	writer.WriteField("payload", string(payloadBytes))
	err := writer.Close() // Close writer before using buffer
	require.NoError(t, err)

	mockStore.On("Shard", newShardID).Return(nil, false)
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, startReq.Peers, startReq.Join, mock.MatchedBy(func(ssc *ShardStartConfig) bool {
		return ssc.InitWithDBArchive == "" &&
			assert.ObjectsAreEqual(ssc.ShardConfig, startReq.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", &b)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	<-started
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Success_Multipart_WithFile(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(102)
	backupID := "backup123"
	expectedArchiveName := fmt.Sprintf("%s-%s.tar.zst", backupID, newShardID)

	snapDir := common.SnapDir(baseDir, newShardID, mockStore.ID())

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("e"), []byte("f")},
			RestoreConfig: &common.BackupConfig{
				BackupID: backupID,
				Format:   common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 3}},
	}
	payloadBytes, _ := json.Marshal(startReq)

	var reqBodyBuf bytes.Buffer
	writer := multipart.NewWriter(&reqBodyBuf)
	writer.WriteField("payload", string(payloadBytes))
	fileWriter, _ := writer.CreateFormFile("backup_file", expectedArchiveName)
	dummyData := []byte("dummy backup data")
	n, err := fileWriter.Write(dummyData)
	require.NoError(t, err)
	require.Equal(t, len(dummyData), n)
	err = writer.Close()
	require.NoError(t, err)

	mockStore.On("Shard", newShardID).Return(nil, false)
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, startReq.Peers, startReq.Join, mock.MatchedBy(func(ssc *ShardStartConfig) bool {
		expectedFilePath := filepath.Join(snapDir, expectedArchiveName)
		_, err := os.Stat(expectedFilePath)
		assert.NoError(t, err, "Backup file should exist at %s", expectedFilePath)
		content, err := os.ReadFile(expectedFilePath)
		assert.NoError(t, err)
		assert.Equal(t, dummyData, content)

		return ssc.InitWithDBArchive == strings.TrimSuffix(expectedArchiveName, ".tar.zst") &&
			assert.ObjectsAreEqual(ssc.ShardConfig, startReq.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", &reqBodyBuf)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	<-started
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_MultipartRejectsFilenameFormatMismatch(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(0x301)
	backupID := "native-declaration"
	portableFileName := common.ShardPortableBackupFileName(backupID, newShardID)
	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("e"), []byte("f")},
			RestoreConfig: &common.BackupConfig{
				BackupID: backupID,
				Format:   common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 3}},
	}
	payloadBytes, err := json.Marshal(startReq)
	require.NoError(t, err)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("payload", string(payloadBytes)))
	fileWriter, err := writer.CreateFormFile("backup_file", portableFileName)
	require.NoError(t, err)
	_, err = fileWriter.Write([]byte("unverified portable payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	mockStore.On("Shard", newShardID).Return(nil, false)
	req := httptest.NewRequest(http.MethodPost, "/shard", &body)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()
	api.ServeHTTP(response, req)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "does not match canonical native backup file")
	mockStore.AssertNotCalled(t, "StartRaftGroup")
	require.NoFileExists(t, filepath.Join(
		common.SnapDir(baseDir, newShardID, mockStore.ID()),
		portableFileName,
	))
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_PortableMultipartVerifiesArtifactIntegrity(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		corrupt    bool
		statusCode int
	}{
		{name: "valid", statusCode: http.StatusOK},
		{name: "corrupt", corrupt: true, statusCode: http.StatusInternalServerError},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
			newShardID := types.ID(0x302)
			backupID := "portable-backup"
			fileName := common.ShardPortableBackupFileName(backupID, newShardID)
			payload := []byte("portable backup data")
			digest := sha256.Sum256(payload)
			declaredDigest := digest
			if testCase.corrupt {
				declaredDigest[0] ^= 0xff
			}
			startReq := ShardStartRequest{
				ShardConfig: ShardConfig{
					ByteRange: types.Range{[]byte("e"), []byte("f")},
					RestoreConfig: &common.BackupConfig{
						BackupID: backupID,
						Format:   common.BackupFormatPortable,
						Artifact: &common.BackupArtifactIntegrity{
							Name:      fileName,
							SizeBytes: uint64(len(payload)),
							SHA256:    hex.EncodeToString(declaredDigest[:]),
						},
					},
				},
				Peers: []common.Peer{{ID: 3}},
			}
			payloadBytes, err := json.Marshal(startReq)
			require.NoError(t, err)
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			require.NoError(t, writer.WriteField("payload", string(payloadBytes)))
			fileWriter, err := writer.CreateFormFile("backup_file", fileName)
			require.NoError(t, err)
			_, err = fileWriter.Write(payload)
			require.NoError(t, err)
			require.NoError(t, writer.Close())

			mockStore.On("Shard", newShardID).Return(nil, false)
			var started <-chan struct{}
			if !testCase.corrupt {
				signal := signalOnCall(mockStore.On(
					"StartRaftGroup",
					newShardID,
					startReq.Peers,
					startReq.Join,
					mock.MatchedBy(func(config *ShardStartConfig) bool {
						return config.InitWithDBArchive == fileName
					}),
				).Return())
				started = signal
			}

			req := httptest.NewRequest(http.MethodPost, "/shard", &body)
			req.Header.Set("X-Raft-Shard-Id", newShardID.String())
			req.Header.Set("Content-Type", writer.FormDataContentType())
			response := httptest.NewRecorder()
			api.ServeHTTP(response, req)
			require.Equal(t, testCase.statusCode, response.Code)
			if started != nil {
				<-started
				require.FileExists(t, filepath.Join(
					common.SnapDir(baseDir, newShardID, mockStore.ID()),
					fileName,
				))
			} else {
				mockStore.AssertNotCalled(t, "StartRaftGroup")
			}
			mockStore.AssertExpectations(t)
		})
	}
}

func TestDownloadFromS3VerifiesBeforeAtomicPublication(t *testing.T) {
	validBody := []byte("portable-artifact")
	corruptBody := append([]byte(nil), validBody...)
	corruptBody[0] ^= 0xff
	currentBody := validBody
	digest := sha256.Sum256(validBody)
	artifact := &common.BackupArtifactIntegrity{
		Name:      "backup-1-1.afb",
		SizeBytes: uint64(len(validBody)),
		SHA256:    hex.EncodeToString(digest[:]),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bucket" || r.URL.Path == "/bucket/" {
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
		w.Header().Set("Content-Length", strconv.Itoa(len(currentBody)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.Header.Get("If-Match") != `"artifact-etag"` {
				http.Error(w, "missing object identity precondition", http.StatusPreconditionFailed)
				return
			}
			_, _ = w.Write(currentBody)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	s3Info := &common.S3Info{
		Endpoint:           server.URL,
		Bucket:             "bucket",
		Prefix:             "backups",
		AddressingStyle:    common.S3ExternalIoConfigAddressingStylePath,
		CredentialSource:   common.AwsCredentialConfigSourceStatic,
		AccessKeyId:        "access",
		SecretAccessKey:    "secret",
		BucketProvisioning: common.S3ExternalIoConfigBucketProvisioningRequireExisting,
	}
	destPath := filepath.Join(t.TempDir(), artifact.Name)
	require.NoError(t, downloadFromS3(
		context.Background(),
		zaptest.NewLogger(t),
		artifact.Name,
		destPath,
		s3Info,
		artifact,
	))
	require.FileExists(t, destPath)
	published, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, validBody, published)

	currentBody = corruptBody
	require.ErrorIs(t, downloadFromS3(
		context.Background(),
		zaptest.NewLogger(t),
		artifact.Name,
		destPath,
		s3Info,
		artifact,
	), common.ErrBackupArtifactIntegrityMismatch)
	published, err = os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, validBody, published)
}

func TestOpenLocalRestoreArtifactIsRootedAndDescriptorValidated(t *testing.T) {
	root := t.TempDir()
	const artifactName = "backup-1-1.afb"
	require.NoError(t, os.WriteFile(
		filepath.Join(root, artifactName),
		[]byte("portable artifact"),
		0o600,
	))
	restoreRoot, err := os.OpenRoot(root)
	require.NoError(t, err)
	defer func() { _ = restoreRoot.Close() }()
	file, err := openLocalRestoreArtifact(restoreRoot, artifactName)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	for _, invalidName := range []string{
		"../" + artifactName,
		"nested/" + artifactName,
		`nested\` + artifactName,
		filepath.Join(string(filepath.Separator), artifactName),
	} {
		file, err = openLocalRestoreArtifact(restoreRoot, invalidName)
		require.Nil(t, file)
		require.Error(t, err)
	}

	outside := filepath.Join(t.TempDir(), artifactName)
	require.NoError(t, os.WriteFile(outside, []byte("outside artifact"), 0o600))
	require.NoError(t, os.Remove(filepath.Join(root, artifactName)))
	if err := os.Symlink(outside, filepath.Join(root, artifactName)); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
	file, err = openLocalRestoreArtifact(restoreRoot, artifactName)
	if file != nil {
		_ = file.Close()
	}
	require.Error(t, err)
}

func TestCopyLocalBackupToSnapDirVerifiesBeforePublishing(t *testing.T) {
	const backupID = "portable-local"
	shardID := types.ID(0x302)
	nodeID := types.ID(1)
	sourceDir := t.TempDir()
	payload := []byte("portable local backup")
	fileName := common.ShardPortableBackupFileName(backupID, shardID)
	require.NoError(t, os.WriteFile(
		filepath.Join(sourceDir, fileName),
		payload,
		0o600,
	))
	digest := sha256.Sum256(payload)
	config := &common.BackupConfig{
		BackupID: backupID,
		Format:   common.BackupFormatPortable,
		Artifact: &common.BackupArtifactIntegrity{
			Name:      fileName,
			SizeBytes: uint64(len(payload)),
			SHA256:    hex.EncodeToString(digest[:]),
		},
	}
	snapStore, err := snapstore.NewLocalSnapStore(t.TempDir(), shardID, nodeID)
	require.NoError(t, err)

	archiveName, err := copyLocalBackupToSnapDir(
		context.Background(),
		zaptest.NewLogger(t),
		snapStore,
		shardID,
		sourceDir,
		config,
	)
	require.NoError(t, err)
	require.Equal(t, fileName, archiveName)
	stored, err := snapStore.Get(context.Background(), fileName)
	require.NoError(t, err)
	defer func() { _ = stored.Close() }()
	storedPayload, err := io.ReadAll(stored)
	require.NoError(t, err)
	require.Equal(t, payload, storedPayload)

	corruptStore, err := snapstore.NewLocalSnapStore(t.TempDir(), shardID, nodeID)
	require.NoError(t, err)
	corruptConfig := *config
	corruptArtifact := *config.Artifact
	corruptArtifact.SHA256 = strings.Repeat("0", sha256.Size*2)
	corruptConfig.Artifact = &corruptArtifact
	_, err = copyLocalBackupToSnapDir(
		context.Background(),
		zaptest.NewLogger(t),
		corruptStore,
		shardID,
		sourceDir,
		&corruptConfig,
	)
	require.ErrorIs(t, err, common.ErrBackupArtifactIntegrityMismatch)
	exists, err := corruptStore.Exists(context.Background(), fileName)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestHandleStartShard_RejectsS3RestoreWithoutAuthorizedConnection(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(103)
	backupID := "s3backup"
	s3BucketURL := "s3://invalid-s3-endpoint-for-test.localdomain"
	restoreLocation := s3BucketURL + "/backups"

	snapDir := common.SnapDir(common.RootAntflyDir, newShardID, mockStore.ID())
	os.RemoveAll(snapDir)
	t.Cleanup(func() { os.RemoveAll(snapDir) })

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("g"), []byte("h")},
			RestoreConfig: &common.BackupConfig{
				BackupID: backupID,
				Location: restoreLocation,
				Format:   common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 4}},
	}
	jsonBody, _ := json.Marshal(startReq)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(
		t,
		http.StatusBadRequest,
		rr.Code,
		"Expected an unauthorized S3 location to fail before network access",
	)
	assert.Contains(t, rr.Body.String(), "authorizing S3 restore")
	assert.Contains(t, rr.Body.String(), "connection is required")
	mockStore.AssertNotCalled(
		t,
		"StartRaftGroup",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_RejectsLocalRestoreWithoutNamedConnection(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(0x103)
	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("g"), []byte("h")},
			RestoreConfig: &common.BackupConfig{
				BackupID: "local-backup",
				Location: "file:///backups",
				Format:   common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 4}},
	}
	body, err := json.Marshal(startReq)
	require.NoError(t, err)
	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, req)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "named filesystem connection")
	mockStore.AssertNotCalled(t, "StartRaftGroup")
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Success_RestoreConfig_File(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(104)
	backupID := "filebackup"
	expectedArchiveName := fmt.Sprintf("%s-%s.tar.zst", backupID, newShardID)

	tempDir := filepath.Join(baseDir, "restore-source-root")
	srcBackupDir := filepath.Join(tempDir, "source_backups")
	err := os.MkdirAll(srcBackupDir, os.ModePerm)
	require.NoError(t, err)

	srcBackupFilePath := filepath.Join(srcBackupDir, expectedArchiveName)
	err = os.WriteFile(srcBackupFilePath, []byte("local backup data"), 0o644)
	require.NoError(t, err)

	restoreLocation := "file:///restore-source-root/source_backups"

	snapDir := common.SnapDir(baseDir, newShardID, mockStore.ID())

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("i"), []byte("j")},
			RestoreConfig: &common.BackupConfig{
				BackupID:   backupID,
				Connection: "filesystem",
				Location:   restoreLocation,
				Format:     common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 5}},
	}
	jsonBody, _ := json.Marshal(startReq)

	mockStore.On("Shard", newShardID).Return(nil, false)
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, startReq.Peers, startReq.Join, mock.MatchedBy(func(ssc *ShardStartConfig) bool {
		destPath := filepath.Join(snapDir, expectedArchiveName)
		_, statErr := os.Stat(destPath)
		assert.NoError(t, statErr, "Copied backup file should exist")
		content, _ := os.ReadFile(destPath)
		assert.Equal(t, "local backup data", string(content))
		return ssc.InitWithDBArchive == strings.TrimSuffix(expectedArchiveName, ".tar.zst") &&
			assert.ObjectsAreEqual(ssc.ShardConfig, startReq.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	<-started
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Success_SplitStart(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(105)
	expectedArchiveName := common.SplitArchive(newShardID)

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{ByteRange: types.Range{[]byte("k"), []byte("l")}},
		Peers:       []common.Peer{{ID: 6}},
		SplitStart:  true,
	}
	jsonBody, _ := json.Marshal(startReq)

	mockStore.On("Shard", newShardID).Return(nil, false)
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, startReq.Peers, startReq.Join, mock.MatchedBy(func(ssc *ShardStartConfig) bool {
		return ssc.InitWithDBArchive == strings.TrimSuffix(expectedArchiveName, ".tar.zst") &&
			assert.ObjectsAreEqual(ssc.ShardConfig, startReq.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	<-started
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Multipart_FileWithoutRestoreConfig(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(106)

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{ByteRange: types.Range{[]byte("m"), []byte("n")}},
		Peers:       []common.Peer{{ID: 7}},
	}
	payloadBytes, _ := json.Marshal(startReq)

	var reqBodyBuf bytes.Buffer
	writer := multipart.NewWriter(&reqBodyBuf)
	writer.WriteField("payload", string(payloadBytes))
	fileWriter, _ := writer.CreateFormFile("backup_file", "unwanted.tar.zst")
	_, _ = fileWriter.Write([]byte("this data should be ignored"))
	err := writer.Close()
	require.NoError(t, err)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", &reqBodyBuf)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "RestoreConfig is required when uploading a backup file")
	mockStore.AssertExpectations(t)
}

// --- Failure Cases ---

func TestHandleStartShard_Failure_MethodNotAllowed(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	req := httptest.NewRequest(http.MethodGet, "/shard", nil)
	req.Header.Set("X-Raft-Shard-Id", "1")
	mockStore.On("Shard", types.ID(1)).Return(nil, false)

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestHandleStartShard_Failure_MissingShardIDHeader(t *testing.T) {
	api, _, _ := setupStoreAPI(t, types.ID(1))
	req := httptest.NewRequest(http.MethodPost, "/shard", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to get shard ID")
}

func TestHandleStartShard_Failure_InvalidShardIDHeader(t *testing.T) {
	api, _, _ := setupStoreAPI(t, types.ID(1))
	req := httptest.NewRequest(http.MethodPost, "/shard", strings.NewReader("{}"))
	req.Header.Set("X-Raft-Shard-Id", "not-a-valid-id")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to get shard ID")
}

func TestHandleStartShard_Failure_ZeroShardIDHeader(t *testing.T) {
	api, _, _ := setupStoreAPI(t, types.ID(1))
	req := httptest.NewRequest(http.MethodPost, "/shard", strings.NewReader("{}"))
	req.Header.Set("X-Raft-Shard-Id", "0")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Shard ID cannot be 0")
}

func TestHandleStartShard_Failure_ShardAlreadyExists(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	existingShardID := types.ID(200)

	mockStore.On("Shard", existingShardID).Return(new(MockShard), true) // Shard exists

	reqBody := ShardStartRequest{
		ShardConfig: ShardConfig{ByteRange: types.Range{[]byte("a"), []byte("b")}},
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", existingShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Shard already exists")
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Failure_InvalidNodeID(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(0)) // Store Node ID is 0 (invalid)
	newShardID := types.ID(201)

	reqBody := ShardStartRequest{
		ShardConfig: ShardConfig{ByteRange: types.Range{[]byte("a"), []byte("b")}},
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "Failed to determine current node ID")
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Failure_BadJSONRequest(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(202)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", strings.NewReader("this is not json"))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "reading request body")
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Failure_BadMultipartRequest_Payload(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(203)

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	writer.WriteField("payload", "not valid json")
	err := writer.Close()
	require.NoError(t, err)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", &b)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "unmarshaling payload")
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Failure_RestoreFromFile_SrcNotFound(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(204)
	backupID := "filenotfoundbackup"

	require.NoError(t, os.Mkdir(
		filepath.Join(baseDir, "non_existent_source_backups"),
		0o750,
	))
	restoreLocation := "file:///non_existent_source_backups"

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("o"), []byte("p")},
			// FIXME (ajr) Why doesn't the below line work with the DeepEqual of assert.ObjectsAreEqual?
			// ByteRange:     types.Range{[]byte{0x00}, []byte{0xFF}},
			RestoreConfig: &common.BackupConfig{
				BackupID:   backupID,
				Connection: "filesystem",
				Location:   restoreLocation,
				Format:     common.BackupFormatNative,
			},
		},
		Peers: []common.Peer{{ID: 1}},
	}
	jsonBody, _ := json.Marshal(startReq)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "local restore artifact")
	mockStore.AssertNotCalled(
		t,
		"StartRaftGroup",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_Failure_Multipart_CreateSnapDirFails(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(205)
	backupID := "backup_snap_fail"

	snapDirBase := common.SnapDir(baseDir, newShardID, mockStore.ID())
	err := os.MkdirAll(filepath.Dir(snapDirBase), os.ModePerm)
	require.NoError(t, err)
	f, err := os.Create(snapDirBase) // Create a FILE where a DIR is expected by MkdirAll
	require.NoError(t, err)
	f.Close()

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			RestoreConfig: &common.BackupConfig{
				BackupID: backupID,
				Format:   common.BackupFormatNative,
			},
		},
	}
	payloadBytes, _ := json.Marshal(startReq)

	var reqBodyBuf bytes.Buffer
	writer := multipart.NewWriter(&reqBodyBuf)
	writer.WriteField("payload", string(payloadBytes))
	_, err = writer.CreateFormFile(
		"backup_file",
		common.ShardBackupFileName(backupID, newShardID),
	)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", &reqBodyBuf)
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "creating restore directory")
	mockStore.AssertNotCalled(
		t,
		"StartRaftGroup",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
	mockStore.AssertExpectations(t)
}

func TestHandleStartShard_RejectsTraversalBackupID(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(310)
	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			RestoreConfig: &common.BackupConfig{BackupID: "../../tmp/owned", Location: "s3://bucket/backups"},
		},
		Peers: []common.Peer{{ID: 1}},
	}
	body, err := json.Marshal(startReq)
	require.NoError(t, err)
	mockStore.On("Shard", newShardID).Return(nil, false)

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "invalid backup ID")
	mockStore.AssertNotCalled(t, "StartRaftGroup", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBackup_RejectsLocalLocationOutsideBaseDir(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	mockStore := &MockStore{logger: logger, nodeID: types.ID(1)}
	api := (&StoreAPI{
		logger: logger,
		store:  mockStore,
		antflyConfig: &common.Config{Storage: common.StorageConfig{Local: common.LocalStorageConfig{
			BaseDir: baseDir,
		}}},
	}).setupRoutes()

	shardID := types.ID(311)
	mockStore.On("Shard", shardID).Return(mockShard, true)
	body, err := json.Marshal(common.BackupConfig{BackupID: "safe-id", Location: "file://" + outsideDir, Format: common.BackupFormatNative})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "must be under antfly base directory")
	mockShard.AssertNotCalled(t, "Backup", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBackup_AllowsConfiguredLocalBackupDir(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	baseDir := t.TempDir()
	allowedDir := t.TempDir()
	t.Setenv(allowedFileBackupDirsEnv, allowedDir)

	mockStore := &MockStore{logger: logger, nodeID: types.ID(1)}
	api := (&StoreAPI{
		logger: logger,
		store:  mockStore,
		antflyConfig: &common.Config{Storage: common.StorageConfig{Local: common.LocalStorageConfig{
			BaseDir: baseDir,
		}}},
	}).setupRoutes()

	shardID := types.ID(313)
	mockStore.On("Shard", shardID).Return(mockShard, true)
	mockShard.On("Backup", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		backupName := args.String(2) + ".tar.zst"
		snapDir := common.SnapDir(baseDir, shardID, mockStore.ID())
		require.NoError(t, os.MkdirAll(snapDir, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, backupName), []byte("backup"), 0o600))
	}).Return(nil)

	body, err := json.Marshal(common.BackupConfig{BackupID: "safe-id", Location: "file://" + allowedDir, Format: common.BackupFormatNative})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	mockShard.AssertCalled(t, "Backup", mock.Anything, mock.Anything, mock.Anything)
}

func TestHandleBackup_RejectsTraversalBackupID(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockShard := new(MockShard)
	mockStore := &MockStore{logger: logger, nodeID: types.ID(1)}
	api := (&StoreAPI{logger: logger, store: mockStore}).setupRoutes()

	shardID := types.ID(312)
	mockStore.On("Shard", shardID).Return(mockShard, true)
	body, err := json.Marshal(common.BackupConfig{BackupID: "../../owned", Location: "s3://bucket/backups", Format: common.BackupFormatNative})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/shard/backup", bytes.NewReader(body))
	req.Header.Set("X-Raft-Shard-Id", shardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "Invalid backup ID")
	mockShard.AssertNotCalled(t, "Backup", mock.Anything, mock.Anything, mock.Anything)
}
