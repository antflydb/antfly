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

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
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

// MockShard is a mock type for the Shard interface
type MockShard struct {
	mock.Mock

	raftNode *MockRaftNode // Add this if Shard methods interact with raftNode directly
}

func (m *MockShard) Backup(ctx context.Context, location, backupID string) error {
	args := m.Called(ctx, location, backupID)
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

func (m *MockShard) Delete(ctx context.Context, key []byte) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

func (m *MockShard) Lookup(ctx context.Context, key string) (map[string]any, error) {
	args := m.Called(key)
	return args.Get(0).(map[string]any), args.Error(0)
}

func (m *MockShard) GetTimestamp(key string) (uint64, error) {
	args := m.Called(key)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockShard) Batch(ctx context.Context, batch *db.BatchOp, proposeOnly bool) error {
	args := m.Called(ctx, batch, proposeOnly)
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
	cc raftpb.ConfChange,
) error { // Assuming raftpb is imported or not needed for this test
	m.Called(cc)
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

func (m *MockStore) StopRaftGroup(shardID types.ID) {
	m.Called(shardID)
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
	tempDir := t.TempDir() // Create a temporary directory for the backup file

	backupLocation := "file://" + tempDir
	// expectedBackupFileName := fmt.Sprintf("%s-%s.tar.sz", shardID, backupID)
	// fullBackupFilePath := filepath.Join(tempDir, expectedBackupFileName)

	// Simulate the backup file being created by the Shard.Backup method
	mockShard.On("Backup", mock.Anything, backupLocation, fmt.Sprintf("%s-%s", backupID, shardID)).
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
	sapi := &StoreAPI{logger: logger, store: mockStore}
	api := sapi.setupRoutes()

	shardID := types.ID(123)
	backupID := "test-backup-fail"
	tempDir := t.TempDir()
	backupLocation := "file://" + tempDir

	mockStore.On("Shard", shardID).Return(mockShard, true)
	mockShard.On("Backup", mock.Anything, backupLocation, fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(errors.New("shard backup failed"))

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
	api := &StoreAPI{logger: logger, store: mockStore}

	shardID := types.ID(789)
	backupID := "test-backup-nofile"
	tempDir := t.TempDir()
	backupLocation := "file://" + tempDir

	// Simulate Shard.Backup succeeding but somehow not creating the file (or it gets deleted)
	mockShard.On("Backup", mock.Anything, backupLocation, fmt.Sprintf("%s-%s", backupID, shardID)).
		Return(nil)
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
		startingShards: make(map[types.ID]struct{}),
	}
	return api.setupRoutes(), mockStore, baseDir
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
			ByteRange:     types.Range{[]byte("e"), []byte("f")},
			RestoreConfig: &common.BackupConfig{BackupID: backupID},
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

func TestHandleStartShard_Failure_RestoreConfig_S3_CannotCreateClientOrDownloadFails(t *testing.T) {
	api, mockStore, _ := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(103)
	backupID := "s3backup"
	// Using a clearly invalid hostname to ensure NewMinioClient or FGetObject fails.
	s3BucketURL := "s3://invalid-s3-endpoint-for-test.localdomain"
	restoreLocation := s3BucketURL + "/backups"

	snapDir := common.SnapDir(common.RootAntflyDir, newShardID, mockStore.ID())
	os.RemoveAll(snapDir)
	t.Cleanup(func() { os.RemoveAll(snapDir) })

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange:     types.Range{[]byte("g"), []byte("h")},
			RestoreConfig: &common.BackupConfig{BackupID: backupID, Location: restoreLocation},
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
		http.StatusInternalServerError,
		rr.Code,
		"Expected server error due to S3 download failure",
	)
	assert.Contains(t, rr.Body.String(), "downloading s3 backup")
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

func TestHandleStartShard_Success_RestoreConfig_File(t *testing.T) {
	api, mockStore, baseDir := setupStoreAPI(t, types.ID(1))
	newShardID := types.ID(104)
	backupID := "filebackup"
	expectedArchiveName := fmt.Sprintf("%s-%s.tar.zst", backupID, newShardID)

	tempDir := t.TempDir()
	srcBackupDir := filepath.Join(tempDir, "source_backups")
	err := os.MkdirAll(srcBackupDir, os.ModePerm)
	require.NoError(t, err)

	srcBackupFilePath := filepath.Join(srcBackupDir, expectedArchiveName)
	err = os.WriteFile(srcBackupFilePath, []byte("local backup data"), 0o644)
	require.NoError(t, err)

	restoreLocation := "file://" + srcBackupDir

	snapDir := common.SnapDir(baseDir, newShardID, mockStore.ID())

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange:     types.Range{[]byte("i"), []byte("j")},
			RestoreConfig: &common.BackupConfig{BackupID: backupID, Location: restoreLocation},
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

	nonExistentSrcDir := filepath.Join(t.TempDir(), "non_existent_source_backups")
	restoreLocation := "file://" + nonExistentSrcDir
	expectedArchiveName := fmt.Sprintf("%s-%s.tar.zst", backupID, newShardID)

	startReq := ShardStartRequest{
		ShardConfig: ShardConfig{
			ByteRange: types.Range{[]byte("o"), []byte("p")},
			// FIXME (ajr) Why doesn't the below line work with the DeepEqual of assert.ObjectsAreEqual?
			// ByteRange:     types.Range{[]byte{0x00}, []byte{0xFF}},
			RestoreConfig: &common.BackupConfig{BackupID: backupID, Location: restoreLocation},
		},
		Peers: []common.Peer{{ID: 1}},
	}
	jsonBody, _ := json.Marshal(startReq)

	mockStore.On("Shard", newShardID).Return(nil, false)
	started := signalOnCall(mockStore.On("StartRaftGroup", newShardID, startReq.Peers, startReq.Join, mock.MatchedBy(func(ssc *ShardStartConfig) bool {
		snapDir := common.SnapDir(baseDir, newShardID, mockStore.ID())
		destPath := filepath.Join(snapDir, expectedArchiveName)
		_, statErr := os.Stat(destPath)
		assert.Error(t, statErr, "Backup file should NOT have been copied as source doesn't exist")
		return ssc.InitWithDBArchive == "" &&
			assert.ObjectsAreEqual(ssc.ShardConfig, startReq.ShardConfig)
	})).
		Return())

	req := httptest.NewRequest(http.MethodPost, "/shard", bytes.NewReader(jsonBody))
	req.Header.Set("X-Raft-Shard-Id", newShardID.String())
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code) // Handler proceeds with warning, Pebble will fail later
	<-started
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
			RestoreConfig: &common.BackupConfig{BackupID: backupID},
		},
	}
	payloadBytes, _ := json.Marshal(startReq)

	var reqBodyBuf bytes.Buffer
	writer := multipart.NewWriter(&reqBodyBuf)
	writer.WriteField("payload", string(payloadBytes))
	_, err = writer.CreateFormFile("backup_file", "backup_snap_fail-205.tar.zst")
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
	assert.Contains(t, rr.Body.String(), "creating snapshot directory")
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
