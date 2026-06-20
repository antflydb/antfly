// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo && antflylite_capi

package antflylite

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLiteCAPI(t *testing.T) {
	if got := ABIVersion(); got != 1 {
		t.Fatalf("ABI version = %d, want 1", got)
	}

	path := filepath.Join(t.TempDir(), "go-smoke.aflite")
	db, err := OpenWithOptions(path, OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
		NoSync:  true,
	})
	if err != nil {
		t.Fatalf("open Lite database: %v", err)
	}
	defer db.Close()

	err = db.Batch([]WriteIntent{{
		Key:   "doc:go-smoke",
		Value: []byte(`{"title":"go api lite"}`),
	}}, 1)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}

	lookup, err := db.LookupJSON("doc:go-smoke")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !bytes.Contains(lookup, []byte("go api lite")) {
		t.Fatalf("lookup JSON %q did not contain written document", lookup)
	}

	status, err := db.StatusJSON()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !bytes.Contains(status, []byte("aflite")) || !bytes.Contains(status, []byte("native_single_file")) {
		t.Fatalf("status JSON %q did not describe native aflite storage", status)
	}

	typedStatus, err := db.Status()
	if err != nil {
		t.Fatalf("typed status: %v", err)
	}
	if typedStatus.Storage.Format != "aflite" || typedStatus.Storage.Engine != "native_single_file" {
		t.Fatalf("typed status storage = %#v", typedStatus.Storage)
	}
	if typedStatus.Inference.Mode != "caller_supplied_or_disabled" {
		t.Fatalf("typed status inference mode = %q", typedStatus.Inference.Mode)
	}
	if typedStatus.Inference.Configured || typedStatus.Inference.RemoteProviderConfigured || typedStatus.Inference.LocalRuntimeConfigured {
		t.Fatalf("fresh Lite database should not report configured inference: %#v", typedStatus.Inference)
	}
	if !typedStatus.Inference.CallerSuppliedArtifacts || !typedStatus.Inference.NoInferenceConfiguredOK {
		t.Fatalf("fresh Lite database should accept caller-supplied or deferred inference: %#v", typedStatus.Inference)
	}

	caps, err := db.CapabilitiesJSON()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !bytes.Contains(caps, []byte("inference")) {
		t.Fatalf("capabilities JSON %q did not include inference fields", caps)
	}

	typedCaps, err := db.Capabilities()
	if err != nil {
		t.Fatalf("typed capabilities: %v", err)
	}
	if typedCaps.InferenceMode != "caller_supplied_or_disabled" || !typedCaps.CallerSuppliedArtifacts || !typedCaps.NoInferenceConfiguredOK {
		t.Fatalf("typed capabilities inference fields = %#v", typedCaps)
	}
	if typedCaps.RaftReplication || typedCaps.ClusterPlacement || typedCaps.DistributedTransactionCoordination {
		t.Fatalf("typed capabilities should not advertise distributed features: %#v", typedCaps)
	}

	checkReport, err := db.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !checkReport.Valid || checkReport.FileSize == 0 || checkReport.CompactSize == 0 || checkReport.Issue != nil {
		t.Fatalf("unexpected check report: %#v", checkReport)
	}

	snapshotPath := filepath.Join(t.TempDir(), "go-snapshot.aflite")
	snapshotReport, err := db.CopyStableSnapshot(snapshotPath, false)
	if err != nil {
		t.Fatalf("copy stable snapshot: %v", err)
	}
	if snapshotReport.SnapshotSize == 0 || snapshotReport.PageCount == 0 {
		t.Fatalf("unexpected snapshot report: %#v", snapshotReport)
	}
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file: %v", err)
	}

	vacuumReport, err := db.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if vacuumReport.BeforeSize == 0 || vacuumReport.AfterSize == 0 {
		t.Fatalf("unexpected vacuum report: %#v", vacuumReport)
	}

	backupPath := filepath.Join(t.TempDir(), "go-backup.afb")
	if err := db.BackupToFile(backupPath); err != nil {
		t.Fatalf("backup to file: %v", err)
	}
	if info, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file: %v", err)
	} else if info.Size() == 0 {
		t.Fatalf("backup file is empty: %s", backupPath)
	}

	restoredPath := filepath.Join(t.TempDir(), "go-restored.aflite")
	if err := RestoreBackupFile(restoredPath, backupPath, false); err != nil {
		t.Fatalf("restore backup file: %v", err)
	}
	restored, err := OpenReadonly(restoredPath)
	if err != nil {
		t.Fatalf("open restored Lite database: %v", err)
	}
	restoredLookup, err := restored.LookupJSON("doc:go-smoke")
	if err != nil {
		t.Fatalf("lookup restored document: %v", err)
	}
	if !bytes.Contains(restoredLookup, []byte("go api lite")) {
		t.Fatalf("restored lookup JSON %q did not contain written document", restoredLookup)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("close restored Lite database: %v", err)
	}

	backupBytes, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup file: %v", err)
	}
	restoredFromBytesPath := filepath.Join(t.TempDir(), "go-restored-bytes.aflite")
	if err := RestoreBackup(restoredFromBytesPath, backupBytes, false); err != nil {
		t.Fatalf("restore backup bytes: %v", err)
	}
	if err := RestoreBackup(restoredFromBytesPath, backupBytes, false); err == nil {
		t.Fatalf("restore without replace unexpectedly overwrote target")
	}
	malformedRestorePath := filepath.Join(t.TempDir(), "go-malformed-restore.aflite")
	if err := RestoreBackup(malformedRestorePath, []byte("not an afb"), false); err == nil {
		t.Fatalf("malformed restore unexpectedly succeeded")
	}
	if _, err := os.Stat(malformedRestorePath); !os.IsNotExist(err) {
		t.Fatalf("malformed restore left target behind: %v", err)
	}

	hostedPath := filepath.Join(t.TempDir(), "go-hosted.aflite")
	hosted, err := OpenHosted(hostedPath)
	if err != nil {
		t.Fatalf("open hosted Lite database: %v", err)
	}
	defer hosted.Close()

	hostedCaps, err := hosted.Capabilities()
	if err != nil {
		t.Fatalf("hosted capabilities: %v", err)
	}
	if !hostedCaps.HostedProfile || !hostedCaps.ManualMaintenance {
		t.Fatalf("hosted capabilities should report manual maintenance: %#v", hostedCaps)
	}
	if hostedCaps.BackgroundEnrichmentRuntime || hostedCaps.TTLCleanupRuntime || hostedCaps.TransactionRecoveryRuntime {
		t.Fatalf("hosted capabilities should not report background runtimes: %#v", hostedCaps)
	}

	hostedStatus, err := hosted.Status()
	if err != nil {
		t.Fatalf("hosted status: %v", err)
	}
	if !hostedStatus.Capabilities.HostedProfile || !hostedStatus.Capabilities.ManualMaintenance {
		t.Fatalf("hosted status should include hosted capabilities: %#v", hostedStatus.Capabilities)
	}
}
