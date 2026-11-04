// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompletedPurgePersistsRemovalAfterProfileWriteFailure(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	original := cloneProfile(s.profile)
	path := s.profilePath()
	backup := filepath.Join(s.directory, "profile-backup.json")
	require.NoError(t, os.Rename(path, backup))
	require.NoError(t, os.Mkdir(path, 0700))
	sourceID := s.profile.Sources[0].ID
	response := serviceCall(s, "DELETE", "/v1/sources/"+sourceID+"?purge=true", nil)
	require.Equal(t, 500, response.Code)
	require.Empty(t, s.profile.Sources, "tombstone already revoked in-memory source")
	// While the write fault persists, completion must fail and retain its intent.
	failed := serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, failed.Code)
	var failedJob SyncJob
	require.NoError(t, json.Unmarshal(failed.Body.Bytes(), &failedJob))
	require.Equal(t, "failed", waitServiceJob(t, s, failedJob.ID).State)
	queue, err := s.readPurges()
	require.NoError(t, err)
	require.Len(t, queue.Sources, 1)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(backup, path))
	response = serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, response.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	job = waitServiceJob(t, s, job.ID)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 1, job.PurgedSourceCount)
	require.NoError(t, s.Close())
	reopened, err := NewService(ServiceOptions{Profile: original, StateDir: s.directory, PublicURL: s.baseURL, AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: s.factory})
	require.NoError(t, err)
	defer reopened.Close()
	require.Empty(t, reopened.profile.Sources)
	queue, err = reopened.readPurges()
	require.NoError(t, err)
	require.Empty(t, queue.Sources)
	require.Equal(t, "completed", startServiceJob(t, reopened).State)
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Empty(t, backend.records, "completed purge must not reimport deleted rows on restart")
}
