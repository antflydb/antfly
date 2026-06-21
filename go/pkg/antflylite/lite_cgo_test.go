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

func TestErrorCodeMetadataMatchesCABI(t *testing.T) {
	cases := []ErrorCode{
		OK,
		InvalidArgument,
		NotFound,
		VersionConflict,
		IntentConflict,
		TxnNotFound,
		Busy,
		Internal,
		ErrorCode(127),
	}
	for _, code := range cases {
		if got, want := code.Name(), cABIErrorCodeName(code); got != want {
			t.Fatalf("error code %d name = %q, C ABI = %q", code, got, want)
		}
		if got, want := code.Description(), cABIErrorCodeDescription(code); got != want {
			t.Fatalf("error code %d description = %q, C ABI = %q", code, got, want)
		}
	}
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func TestLiteOpenModeConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-open-modes.aflite")

	writer, err := Open(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()

	if _, err := Open(path); err != Busy {
		t.Fatalf("second writer error = %v, want %v", err, Busy)
	}

	readonly, err := OpenReadonly(path)
	if err != nil {
		t.Fatalf("open readonly while writer exists: %v", err)
	}
	if _, err := readonly.Status(); err != nil {
		readonly.Close()
		t.Fatalf("readonly status: %v", err)
	}
	if err := readonly.Close(); err != nil {
		t.Fatalf("close readonly: %v", err)
	}

	statusOnly, err := OpenStatusOnly(path)
	if err != nil {
		t.Fatalf("open status-only while writer exists: %v", err)
	}
	if _, err := statusOnly.Status(); err != nil {
		statusOnly.Close()
		t.Fatalf("status-only status: %v", err)
	}
	if err := statusOnly.Close(); err != nil {
		t.Fatalf("close status-only: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen writer after close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened writer: %v", err)
	}
}

func TestLiteCAPI(t *testing.T) {
	if got := ABIVersion(); got != SupportedABIVersion {
		t.Fatalf("ABI version = %d, want %d", got, SupportedABIVersion)
	}
	if got, want := OpenOptionsSize(), compiledOpenOptionsSize(); got != want {
		t.Fatalf("open options size = %d, compiled header size = %d", got, want)
	}
	if err := ValidateABI(); err != nil {
		t.Fatalf("validate ABI: %v", err)
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

	txnID := TxnID{0x67, 0x6f, 0x2d, 0x6c, 0x69, 0x74, 0x65, 0x2d, 0x74, 0x78, 0x6e, 0x2d, 0, 0, 0, 1}
	if err := db.BeginTransaction(txnID, 3, nil); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := db.WriteTransaction(txnID, []WriteIntent{{
		Key:   "doc:go-txn",
		Value: []byte(`{"title":"go transaction"}`),
	}}); err != nil {
		t.Fatalf("write transaction: %v", err)
	}
	if err := db.ResolveTransaction(txnID, TxnCommitted, 4); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	txnStatus, err := db.TransactionStatus(txnID)
	if err != nil {
		t.Fatalf("transaction status: %v", err)
	}
	if txnStatus != TxnCommitted {
		t.Fatalf("transaction status = %d, want committed", txnStatus)
	}
	commitVersion, err := db.CommitVersion(txnID)
	if err != nil {
		t.Fatalf("transaction commit version: %v", err)
	}
	if commitVersion != 4 {
		t.Fatalf("commit version = %d, want 4", commitVersion)
	}
	txnLookup, err := db.LookupJSON("doc:go-txn")
	if err != nil {
		t.Fatalf("lookup transaction document: %v", err)
	}
	if !bytes.Contains(txnLookup, []byte("go transaction")) {
		t.Fatalf("transaction lookup JSON %q did not contain committed document", txnLookup)
	}

	schema := []byte(`{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","required":["title"]}}}}`)
	if err := db.SetSchemaJSON(schema); err != nil {
		t.Fatalf("set schema: %v", err)
	}
	gotSchema, err := db.SchemaJSON()
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !bytes.Contains(gotSchema, []byte(`"required":["title"]`)) {
		t.Fatalf("schema JSON %q did not contain configured schema", gotSchema)
	}

	enrichment := []byte(`{"name":"body_chunks_v1","kind":"chunk","field":"body","chunk_size":8,"chunk_overlap":2}`)
	if err := db.AddEnrichmentJSON(enrichment); err != nil {
		t.Fatalf("add enrichment: %v", err)
	}
	enrichments, err := db.EnrichmentsJSON()
	if err != nil {
		t.Fatalf("list enrichments: %v", err)
	}
	if !bytes.Contains(enrichments, []byte("body_chunks_v1")) {
		t.Fatalf("enrichments JSON %q did not contain configured enrichment", enrichments)
	}

	index := []byte(`{"name":"ft_body_v1","kind":"full_text","config_json":"{}"}`)
	if err := db.AddIndexJSON(index); err != nil {
		t.Fatalf("add index: %v", err)
	}
	indexes, err := db.IndexesJSON()
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	if !bytes.Contains(indexes, []byte("ft_body_v1")) {
		t.Fatalf("indexes JSON %q did not contain configured index", indexes)
	}

	denseIndex := []byte(`{"name":"dv_embedding_v1","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}"}`)
	if err := db.AddIndexJSON(denseIndex); err != nil {
		t.Fatalf("add dense index: %v", err)
	}
	indexes, err = db.IndexesJSON()
	if err != nil {
		t.Fatalf("list indexes after dense index: %v", err)
	}
	if !bytes.Contains(indexes, []byte("dv_embedding_v1")) {
		t.Fatalf("indexes JSON %q did not contain configured dense index", indexes)
	}

	sparseIndex := []byte(`{"name":"sv_embedding_v1","kind":"sparse_vector","config_json":"{\"field\":\"sparse_embedding\",\"external\":true}"}`)
	if err := db.AddIndexJSON(sparseIndex); err != nil {
		t.Fatalf("add sparse index: %v", err)
	}
	graphIndex := []byte(`{"name":"gr_links_v1","kind":"graph","config_json":"{}"}`)
	if err := db.AddIndexJSON(graphIndex); err != nil {
		t.Fatalf("add graph index: %v", err)
	}
	indexes, err = db.IndexesJSON()
	if err != nil {
		t.Fatalf("list indexes after retrieval indexes: %v", err)
	}
	if !bytes.Contains(indexes, []byte("sv_embedding_v1")) || !bytes.Contains(indexes, []byte("gr_links_v1")) {
		t.Fatalf("indexes JSON %q did not contain configured sparse and graph indexes", indexes)
	}

	err = db.Batch([]WriteIntent{
		{
			Key:   "doc:go-search",
			Value: []byte(`{"title":"searchable","body":"go binding full text search hybrid alpha","_embeddings":{"dv_embedding_v1":[1.0,0.0],"sv_embedding_v1":{"indices":[7,42],"values":[1.5,0.5]}},"_edges":{"gr_links_v1":{"links":[{"target":"doc:go-related","weight":1.0}]}}}`),
		},
		{
			Key:   "doc:go-related",
			Value: []byte(`{"title":"related","body":"go graph target"}`),
		},
	}, 2)
	if err != nil {
		t.Fatalf("batch searchable document: %v", err)
	}
	idleStatus, err := db.RunUntilIdleStatus()
	if err != nil {
		t.Fatalf("run until idle status: %v", err)
	}
	if idleStatus.DerivedTargetSequence == 0 || !idleStatus.HasAsyncIndexes || len(idleStatus.TextMerge) == 0 {
		t.Fatalf("run-until-idle status missing readiness fields: %#v", idleStatus)
	}
	pending, err := db.PendingWorkStats()
	if err != nil {
		t.Fatalf("pending work stats: %v", err)
	}
	if pending.DerivedTargetSequence == 0 || !pending.HasAsyncIndexes || len(pending.Enrichment) == 0 {
		t.Fatalf("pending work status missing readiness fields: %#v", pending)
	}
	pendingJSON, err := db.PendingWorkStatsJSON()
	if err != nil {
		t.Fatalf("pending work stats json: %v", err)
	}
	if !bytes.Contains(pendingJSON, []byte("has_async_indexes")) {
		t.Fatalf("pending work JSON %q did not include async index status", pendingJSON)
	}
	replayed, err := db.ReplayGeneratedEnrichments()
	if err != nil {
		t.Fatalf("replay generated enrichments: %v", err)
	}
	if replayed.Replayed != 0 {
		t.Fatalf("fresh Go smoke database replayed generated enrichments = %#v, want 0", replayed)
	}
	scan, err := db.ScanJSON([]byte(`{"from":"doc:go-","to":"doc:go~","include_documents":true,"limit":10}`))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !bytes.Contains(scan, []byte("go binding full text search")) {
		t.Fatalf("scan JSON %q did not contain searchable document", scan)
	}
	fullTextQuery := []byte(`{"mode":"full_text","index_name":"ft_body_v1","text_query_type":"match","field":"body","text":"binding full text","limit":5}`)
	denseQuery := []byte(`{"embeddings":{"dv_embedding_v1":[1.0,0.0]},"indexes":["dv_embedding_v1"],"limit":1}`)
	sparseQuery := []byte(`{"embeddings":{"sv_embedding_v1":{"indices":[7,42],"values":[1.5,0.5]}},"indexes":["sv_embedding_v1"],"limit":1}`)
	graphQuery := []byte(`{"graph_searches":{"neighbors":{"type":"neighbors","index_name":"gr_links_v1","start_nodes":{"keys":["doc:go-search"]},"params":{"edge_types":["links"]}}},"limit":10}`)
	hybridQuery := []byte(`{"full_text_search":{"match":{"field":"body","text":"hybrid alpha"}},"embeddings":{"dv_embedding_v1":[1.0,0.0]},"indexes":["dv_embedding_v1"],"merge_config":{"strategy":"rrf"},"limit":3}`)
	assertSearchContains := func(handle *DB, label string, request []byte, want string) {
		t.Helper()
		result, err := handle.SearchJSON(request)
		if err != nil {
			t.Fatalf("%s search: %v", label, err)
		}
		if !bytes.Contains(result, []byte(want)) {
			t.Fatalf("%s search JSON %q did not contain %q", label, result, want)
		}
	}
	assertSearchContains(db, "full-text", fullTextQuery, "go binding full text search")
	assertSearchContains(db, "dense", denseQuery, "doc:go-search")
	assertSearchContains(db, "sparse", sparseQuery, "doc:go-search")
	assertSearchContains(db, "graph", graphQuery, "doc:go-related")
	assertSearchContains(db, "hybrid", hybridQuery, "doc:go-search")

	if deleted, err := db.DeleteIndex("missing-index"); err != nil {
		t.Fatalf("delete missing index: %v", err)
	} else if deleted {
		t.Fatalf("delete missing index reported deleted")
	}
	if deleted, err := db.DeleteEnrichment("chunk", "missing-enrichment"); err != nil {
		t.Fatalf("delete missing enrichment: %v", err)
	} else if deleted {
		t.Fatalf("delete missing enrichment reported deleted")
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
	if typedStatus.Storage.PrimaryLayout != "native_document_pages" ||
		typedStatus.Storage.IndexLayout != "lsm_logical_files_in_native_index_catalog" ||
		typedStatus.Storage.CompatibilityFallback {
		t.Fatalf("typed status storage layout = %#v", typedStatus.Storage)
	}
	if typedStatus.Inference.Mode != InferenceModeCallerSuppliedOrDisabled {
		t.Fatalf("typed status inference mode = %q", typedStatus.Inference.Mode)
	}
	if !containsString(typedStatus.Inference.AvailableModes, InferenceModeCallerSuppliedArtifacts) ||
		!containsString(typedStatus.Inference.AvailableModes, InferenceModeDisabledDeferred) {
		t.Fatalf("typed status inference available modes = %#v", typedStatus.Inference.AvailableModes)
	}
	if typedStatus.Inference.Configured || typedStatus.Inference.RemoteProviderConfigured || typedStatus.Inference.LocalRuntimeConfigured {
		t.Fatalf("fresh Lite database should not report configured inference: %#v", typedStatus.Inference)
	}
	if !typedStatus.Inference.CallerSuppliedArtifacts || !typedStatus.Inference.NoInferenceConfiguredOK {
		t.Fatalf("fresh Lite database should accept caller-supplied or deferred inference: %#v", typedStatus.Inference)
	}
	if typedStatus.PendingWork.DerivedTargetSequence == 0 || !typedStatus.PendingWork.HasAsyncIndexes {
		t.Fatalf("typed status pending work = %#v", typedStatus.PendingWork)
	}

	remotePath := filepath.Join(t.TempDir(), "go-remote-inference.aflite")
	remoteDB, err := OpenWithOptions(remotePath, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("open remote inference Lite database: %v", err)
	}
	remoteStatus, err := remoteDB.Status()
	if err != nil {
		t.Fatalf("remote inference status: %v", err)
	}
	if err := remoteDB.Close(); err != nil {
		t.Fatalf("close remote inference Lite database: %v", err)
	}
	if remoteStatus.Inference.Mode != InferenceModeRemoteProvider ||
		!remoteStatus.Inference.Configured ||
		!remoteStatus.Inference.RemoteProviderConfigured ||
		remoteStatus.Inference.LocalRuntimeConfigured {
		t.Fatalf("remote inference status = %#v", remoteStatus.Inference)
	}
	if remoteStatus.Capabilities.InferenceMode != InferenceModeRemoteProvider ||
		!remoteStatus.Capabilities.RemoteInferenceProviders ||
		remoteStatus.Capabilities.LocalInferenceRuntime {
		t.Fatalf("remote inference status capabilities = %#v", remoteStatus.Capabilities)
	}

	localPath := filepath.Join(t.TempDir(), "go-local-inference.aflite")
	localDB, err := OpenWithOptions(localPath, OpenOptions{
		Mode:                   OpenModeWriter,
		Profile:                ProfileNative,
		LocalRuntimeConfigured: true,
	})
	if err != nil {
		t.Fatalf("open local inference Lite database: %v", err)
	}
	localStatus, err := localDB.Status()
	if err != nil {
		t.Fatalf("local inference status: %v", err)
	}
	localCaps, err := localDB.Capabilities()
	if err != nil {
		t.Fatalf("local inference capabilities: %v", err)
	}
	if err := localDB.Close(); err != nil {
		t.Fatalf("close local inference Lite database: %v", err)
	}
	if localStatus.Inference.Mode != InferenceModeLocalEmbedded ||
		!localStatus.Inference.Configured ||
		localStatus.Inference.RemoteProviderConfigured ||
		!localStatus.Inference.LocalRuntimeConfigured ||
		!localStatus.Inference.LocalRuntimeAvailable {
		t.Fatalf("local inference status = %#v", localStatus.Inference)
	}
	if localStatus.Capabilities.InferenceMode != InferenceModeLocalEmbedded ||
		!localStatus.Capabilities.LocalInferenceRuntime ||
		!containsString(localStatus.Capabilities.AvailableInferenceModes, InferenceModeLocalEmbedded) {
		t.Fatalf("local inference status capabilities = %#v", localStatus.Capabilities)
	}
	if localCaps.InferenceMode != InferenceModeLocalEmbedded ||
		!localCaps.LocalInferenceRuntime ||
		!containsString(localCaps.AvailableInferenceModes, InferenceModeLocalEmbedded) {
		t.Fatalf("local inference capabilities = %#v", localCaps)
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
	if typedCaps.InferenceMode != InferenceModeCallerSuppliedOrDisabled || !typedCaps.CallerSuppliedArtifacts || !typedCaps.NoInferenceConfiguredOK {
		t.Fatalf("typed capabilities inference fields = %#v", typedCaps)
	}
	if !typedCaps.CallerSuppliedEmbeddings || !typedCaps.TextSearch || !typedCaps.DenseVectorSearch ||
		!typedCaps.SparseVectorSearch || !typedCaps.HybridSearch || !typedCaps.GraphSearch {
		t.Fatalf("typed capabilities retrieval fields = %#v", typedCaps)
	}
	if !containsString(typedCaps.SupportedInferenceModes, InferenceModeLocalEmbedded) ||
		!containsString(typedCaps.AvailableInferenceModes, InferenceModeCallerSuppliedArtifacts) ||
		!containsString(typedCaps.AvailableInferenceModes, InferenceModeDisabledDeferred) {
		t.Fatalf("typed capabilities inference modes = supported=%#v available=%#v", typedCaps.SupportedInferenceModes, typedCaps.AvailableInferenceModes)
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
	assertSearchContains(restored, "restored full-text", fullTextQuery, "go binding full text search")
	assertSearchContains(restored, "restored dense", denseQuery, "doc:go-search")
	assertSearchContains(restored, "restored sparse", sparseQuery, "doc:go-search")
	assertSearchContains(restored, "restored graph", graphQuery, "doc:go-related")
	assertSearchContains(restored, "restored hybrid", hybridQuery, "doc:go-search")
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

	importedPath := filepath.Join(t.TempDir(), "go-imported.aflite")
	imported, err := Open(importedPath)
	if err != nil {
		t.Fatalf("open imported Lite database: %v", err)
	}
	if err := imported.ImportBackup(backupBytes); err != nil {
		t.Fatalf("import backup bytes: %v", err)
	}
	assertSearchContains(imported, "imported full-text", fullTextQuery, "go binding full text search")
	assertSearchContains(imported, "imported dense", denseQuery, "doc:go-search")
	assertSearchContains(imported, "imported sparse", sparseQuery, "doc:go-search")
	assertSearchContains(imported, "imported graph", graphQuery, "doc:go-related")
	assertSearchContains(imported, "imported hybrid", hybridQuery, "doc:go-search")
	if err := imported.Close(); err != nil {
		t.Fatalf("close imported Lite database: %v", err)
	}

	malformedRestorePath := filepath.Join(t.TempDir(), "go-malformed-restore.aflite")
	if err := RestoreBackup(malformedRestorePath, []byte("not an afb"), false); err == nil {
		t.Fatalf("malformed restore unexpectedly succeeded")
	}
	if _, err := os.Stat(malformedRestorePath); !os.IsNotExist(err) {
		t.Fatalf("malformed restore left target behind: %v", err)
	}

	ttlPath := filepath.Join(t.TempDir(), "go-ttl.aflite")
	ttlDB, err := OpenWithOptions(ttlPath, OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
		NoSync:  true,
		MapSize: 64 * 1024 * 1024,
		TTLCleanup: &TTLCleanupOptions{
			Enabled:       true,
			LeaseOwned:    true,
			OwnerID:       "go-lite-ttl",
			LeaseTTLMS:    100,
			IntervalMS:    10,
			BatchSize:     4,
			GracePeriodNS: 1,
		},
	})
	if err != nil {
		t.Fatalf("open Lite database with TTL cleanup options: %v", err)
	}
	ttlStats, err := ttlDB.StatsJSON()
	if err != nil {
		t.Fatalf("ttl stats: %v", err)
	}
	if !bytes.Contains(ttlStats, []byte(`"ttl_cleanup"`)) || !bytes.Contains(ttlStats, []byte(`"enabled":true`)) {
		t.Fatalf("ttl stats JSON %q did not include enabled TTL cleanup", ttlStats)
	}
	if err := ttlDB.Close(); err != nil {
		t.Fatalf("close TTL Lite database: %v", err)
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
	if !containsString(hostedCaps.AvailableInferenceModes, InferenceModeManualMaintenance) {
		t.Fatalf("hosted capabilities should advertise manual maintenance inference mode: %#v", hostedCaps.AvailableInferenceModes)
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

	hostedTTLPath := filepath.Join(t.TempDir(), "go-hosted-ttl.aflite")
	hostedWithTTL, err := OpenWithOptions(hostedTTLPath, OpenOptions{
		Mode:       OpenModeWriter,
		Profile:    ProfileHosted,
		TTLCleanup: &TTLCleanupOptions{Enabled: true},
	})
	if err == nil {
		defer hostedWithTTL.Close()
		t.Fatalf("hosted Lite database unexpectedly accepted TTL cleanup options")
	}
	if err != InvalidArgument {
		t.Fatalf("hosted TTL open error = %v, want %v", err, InvalidArgument)
	}
}
