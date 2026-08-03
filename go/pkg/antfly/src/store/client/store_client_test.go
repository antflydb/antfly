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

package sdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/storeutils"
	json "github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestStoreClientBackupUsesResolvedLocationWithoutSerializingIt(t *testing.T) {
	root := t.TempDir()
	var received common.BackupConfig
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
			headers := make(http.Header)
			headers.Set(common.BackupArtifactNameHeader, "backup-1-2a.afb")
			headers.Set(common.BackupArtifactSizeHeader, "15")
			headers.Set(
				common.BackupArtifactSHA256Header,
				"45e76a9340241da1ad38b6eca1188b0d971de588021b63f6c4b7b4a8fe61fb67",
			)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("portable backup")),
				Header:     headers,
			}, nil
		}),
	}
	client := NewStoreClient(httpClient, types.ID(1), "http://store.test")
	config := common.BackupConfig{
		BackupID:         "backup-1",
		Connection:       "filesystem",
		Location:         "file:///logical",
		ResolvedLocation: "file://" + root,
		Format:           common.BackupFormatPortable,
	}

	integrity, err := client.Backup(context.Background(), types.ID(42), config)
	require.NoError(t, err)
	require.NotNil(t, integrity)
	assert.Equal(t, uint64(len("portable backup")), integrity.SizeBytes)
	assert.Equal(t, config.Location, received.Location)
	assert.Empty(t, received.ResolvedLocation)
	content, err := os.ReadFile(filepath.Join(
		root,
		common.ShardPortableBackupFileName(config.BackupID, types.ID(42)),
	))
	require.NoError(t, err)
	assert.Equal(t, "portable backup", string(content))

	_, err = client.Backup(context.Background(), types.ID(42), config)
	require.Error(t, err)
	assert.True(t, errors.Is(err, common.ErrBackupAlreadyExists))
}

func TestStoreClientBackupRejectsUnresolvedManagedFilesystemLocation(t *testing.T) {
	client := NewStoreClient(http.DefaultClient, types.ID(1), "http://store.test")
	_, err := client.Backup(context.Background(), types.ID(42), common.BackupConfig{
		BackupID:   "backup-1",
		Connection: "filesystem",
		Location:   "file:///logical",
		Format:     common.BackupFormatPortable,
	})
	require.ErrorContains(t, err, "coordinator-resolved")
}

func TestStoreClientBatch_PropagatesTimestamp(t *testing.T) {
	var received db.BatchOp
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(body, &received))
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewStoreClient(httpClient, types.ID(1), "http://store.test")
	timestamp := uint64(123456789)
	ctx := storeutils.WithTimestamp(context.Background(), timestamp)

	err := sc.Batch(
		ctx,
		types.ID(42),
		[][2][]byte{{[]byte("doc-1"), []byte(`{"name":"test"}`)}},
		nil,
		nil,
		db.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	assert.Equal(t, timestamp, received.GetTimestamp())
}

func TestStoreClientBackup_FileLocationDoesNotTruncateStreamSource(t *testing.T) {
	backupDir := t.TempDir()
	backupID := "backup"
	shardID := types.ID(42)
	backupFileName := common.ShardPortableBackupFileName(backupID, shardID)
	backupPath := filepath.Join(backupDir, backupFileName)
	expected := []byte("streamed portable backup")
	require.NoError(t, os.WriteFile(backupPath, expected, 0o600))

	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, err := os.Open(backupPath)
			require.NoError(t, err)
			stat, err := body.Stat()
			require.NoError(t, err)
			return &http.Response{
				StatusCode:    http.StatusOK,
				Body:          body,
				Header:        make(http.Header),
				ContentLength: stat.Size(),
			}, nil
		}),
	}

	sc := NewStoreClient(httpClient, types.ID(1), "http://store.test")
	require.NoError(t, sc.Backup(
		context.Background(),
		shardID,
		"file://"+backupDir,
		backupID,
		common.BackupFormatPortable,
	))

	actual, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	matches, err := filepath.Glob(filepath.Join(backupDir, "."+backupFileName+".tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}
