/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestBackupReturnsTypedAmbiguousOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"backup_outcome_ambiguous","error":"backup outcome is ambiguous","message":"inspect the retained generation","retryable":false,"backup_id":"daily","artifact_backup_id":"generation-7"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	err = client.Backup(context.Background(), "docs", "daily", "s3://archive/daily", "archive-writer")
	var ambiguous *BackupOutcomeAmbiguousError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("Backup error = %T %v, want *BackupOutcomeAmbiguousError", err, err)
	}
	if ambiguous.StatusCode != http.StatusConflict ||
		ambiguous.Code != "backup_outcome_ambiguous" ||
		ambiguous.Message != "inspect the retained generation" ||
		ambiguous.Retryable ||
		ambiguous.BackupID != "daily" ||
		ambiguous.ArtifactBackupID != "generation-7" {
		t.Fatalf("ambiguous outcome = %#v", ambiguous)
	}
}

func TestClusterBackupPreservesAmbiguousReconciliationIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"backup_id":"nightly","status":"ambiguous","tables":[{"name":"docs","status":"ambiguous","error":"inspect the retained generation","code":"backup_outcome_ambiguous","retryable":false,"backup_id":"attempt-t-0","artifact_backup_id":"attempt-a-0"}]}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.ClusterBackup(context.Background(), "nightly", "s3://archive/nightly", "archive-writer", nil)
	if err != nil {
		t.Fatalf("ClusterBackup: %v", err)
	}
	if result.Status != "ambiguous" || len(result.Tables) != 1 {
		t.Fatalf("cluster outcome = %#v", result)
	}
	table := result.Tables[0]
	if table.Error != "inspect the retained generation" ||
		table.Code != "backup_outcome_ambiguous" ||
		table.Retryable ||
		table.BackupID != "attempt-t-0" ||
		table.ArtifactBackupID != "attempt-a-0" {
		t.Fatalf("table outcome = %#v", table)
	}
}
