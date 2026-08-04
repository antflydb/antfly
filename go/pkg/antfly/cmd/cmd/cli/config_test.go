/*
Copyright 2026 The Antfly Authors

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

package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestClusterBackupReturnsErrorForPartialResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"backup_id":"daily",
			"status":"partial",
			"tables":[
				{"name":"documents","status":"completed"},
				{"name":"missing","status":"failed","error":"not found"}
			]
		}`))
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatalf("NewAntflyClient: %v", err)
	}
	err = client.ClusterBackup(
		context.Background(),
		"daily",
		"file:///backups",
		"local-backups",
		[]string{"documents", "missing"},
	)
	if err == nil || !strings.Contains(err.Error(), "did not complete: status partial") {
		t.Fatalf("ClusterBackup error = %v", err)
	}
}

func TestResolveURLUsesEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("ANTFLY_URL", "https://platform.antfly.io/cloud/v1/instance")
	cmd := &cobra.Command{}
	cmd.Flags().String("url", "http://localhost:8080", "")

	if got := resolveURL(cmd); got != "https://platform.antfly.io/cloud/v1/instance" {
		t.Fatalf("resolveURL = %q", got)
	}
}

func TestResolveURLPrefersFlag(t *testing.T) {
	t.Setenv("ANTFLY_URL", "https://platform.antfly.io/cloud/v1/env")
	cmd := &cobra.Command{}
	cmd.Flags().String("url", "http://localhost:8080", "")
	if err := cmd.Flags().Set("url", "https://platform.antfly.io/cloud/v1/flag"); err != nil {
		t.Fatalf("set url flag: %v", err)
	}

	if got := resolveURL(cmd); got != "https://platform.antfly.io/cloud/v1/flag" {
		t.Fatalf("resolveURL = %q", got)
	}
}

func TestResolveTokenUsesEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("ANTFLY_TOKEN", "env-token")
	cmd := &cobra.Command{}
	cmd.Flags().String("token", "", "")

	if got := resolveToken(cmd); got != "env-token" {
		t.Fatalf("resolveToken = %q", got)
	}
}

func TestResolveTokenPrefersFlag(t *testing.T) {
	t.Setenv("ANTFLY_TOKEN", "env-token")
	cmd := &cobra.Command{}
	cmd.Flags().String("token", "", "")
	if err := cmd.Flags().Set("token", "flag-token"); err != nil {
		t.Fatalf("set token flag: %v", err)
	}

	if got := resolveToken(cmd); got != "flag-token" {
		t.Fatalf("resolveToken = %q", got)
	}
}

func TestBackupListJSONWritesStdout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/db/v1/backups" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("location"); got != "s3://bucket/backups" {
			t.Fatalf("location query = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"backups":[{"backup_id":"daily-20260618","timestamp":"2026-06-18T18:00:00Z","tables":["_backup_verify"],"antfly_version":"v0.2.0"}]}`))
	}))
	defer server.Close()

	client, err := NewAntflyClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatalf("NewAntflyClient: %v", err)
	}
	oldClient := antflyClient
	antflyClient = client
	defer func() { antflyClient = oldClient }()

	cmd := newBackupCmd()
	cmd.PreRunE = nil
	cmd.SetArgs([]string{"--list", "--location", "s3://bucket/backups", "--connection", "archive", "-o", "json"})

	oldStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	defer func() { os.Stdout = oldStdout }()

	if err := cmd.Execute(); err != nil {
		t.Fatalf("backup --list -o json: %v", err)
	}
	if err := writePipe.Close(); err != nil {
		t.Fatalf("close stdout pipe: %v", err)
	}

	var out bytes.Buffer
	if _, err := out.ReadFrom(readPipe); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if !strings.Contains(out.String(), `"backup_id": "daily-20260618"`) {
		t.Fatalf("stdout did not contain backup JSON: %s", out.String())
	}
}

func TestBackupListRejectsJSONL(t *testing.T) {
	cmd := newBackupCmd()
	cmd.PreRunE = nil
	cmd.SetArgs([]string{"--list", "--location", "s3://bucket/backups", "--connection", "archive", "-o", "jsonl"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("backup --list -o jsonl succeeded, want error")
	}
	if !strings.Contains(err.Error(), "supports output formats table and json") {
		t.Fatalf("backup --list -o jsonl error = %q", err)
	}
}
