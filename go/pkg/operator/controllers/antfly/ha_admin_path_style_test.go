package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
)

const pathStylePrimaryStatus = `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`

// pathStyleServer answers primary status under exactly one spelling of the
// hot-standby admin API and records every path it saw.
func pathStyleServer(t *testing.T, servedPrefix string) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, servedPrefix+"/") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer operator-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(pathStylePrimaryStatus))
	}))
	t.Cleanup(server.Close)
	return server, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), paths...)
	}
}

func TestHAAdminPathStyleAutoUsesCanonicalPathsOnNewServers(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")
	adminsdk.ResetNegotiatedPathStyles()
	server, seen := pathStyleServer(t, adminsdk.StandbyPath)

	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client(), HAAdminPathStyle: adminsdk.PathStyleAuto}
	client, err := reconciler.haAdminSDKClient(&antflyv1.AntflyCluster{}, server.URL)
	if err != nil {
		t.Fatalf("haAdminSDKClient: %v", err)
	}
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse: %v", err)
	}
	if got := seen(); len(got) != 1 || got[0] != adminsdk.StandbyPrimaryStatusPath {
		t.Fatalf("paths = %v, want a single canonical request", got)
	}
}

func TestHAAdminPathStyleAutoFallsBackToLegacyPathsOnOldServers(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")
	adminsdk.ResetNegotiatedPathStyles()
	server, seen := pathStyleServer(t, adminsdk.HAPath)

	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client(), HAAdminPathStyle: adminsdk.PathStyleAuto}
	for i := 0; i < 2; i++ {
		// A fresh client per reconcile, as the controller does.
		client, err := reconciler.haAdminSDKClient(&antflyv1.AntflyCluster{}, server.URL)
		if err != nil {
			t.Fatalf("haAdminSDKClient: %v", err)
		}
		if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
			t.Fatalf("PrimaryStatusResponse %d: %v", i, err)
		}
	}
	got := seen()
	want := []string{adminsdk.StandbyPrimaryStatusPath, adminsdk.HAPrimaryStatusPath, adminsdk.HAPrimaryStatusPath}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("paths = %v, want one canonical probe then legacy only: %v", got, want)
	}
}

func TestHAAdminPathStyleZeroValueKeepsLegacyPaths(t *testing.T) {
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")
	server, seen := pathStyleServer(t, adminsdk.HAPath)

	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client()}
	client, err := reconciler.haAdminSDKClient(&antflyv1.AntflyCluster{}, server.URL)
	if err != nil {
		t.Fatalf("haAdminSDKClient: %v", err)
	}
	if _, err := client.PrimaryStatusResponse(context.Background(), nil); err != nil {
		t.Fatalf("PrimaryStatusResponse: %v", err)
	}
	if got := seen(); len(got) != 1 || got[0] != adminsdk.HAPrimaryStatusPath {
		t.Fatalf("paths = %v, want a single legacy request", got)
	}
}

func TestParseHAAdminPathStyle(t *testing.T) {
	cases := map[string]adminsdk.PathStyle{"auto": adminsdk.PathStyleAuto, " Legacy ": adminsdk.PathStyleLegacy, "canonical": adminsdk.PathStyleCanonical}
	for input, want := range cases {
		got, err := ParseHAAdminPathStyle(input)
		if err != nil || got != want {
			t.Fatalf("ParseHAAdminPathStyle(%q) = %v, %v; want %v", input, got, err, want)
		}
	}
	if _, err := ParseHAAdminPathStyle("v2"); err == nil {
		t.Fatal("ParseHAAdminPathStyle accepted an unknown style")
	}
}
