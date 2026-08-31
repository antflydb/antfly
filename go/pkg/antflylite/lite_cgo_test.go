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

func TestBundledCABIHeaderMatchesSourceTree(t *testing.T) {
	bundled, err := os.ReadFile(filepath.Join("include", "antfly.h"))
	if err != nil {
		t.Fatalf("read bundled C ABI header: %v", err)
	}

	sourcePath := filepath.Join("..", "..", "..", "zig", "pkg", "antfly", "include", "antfly.h")
	source, err := os.ReadFile(sourcePath)
	if os.IsNotExist(err) {
		t.Skip("source-tree C ABI header is not present in this module checkout")
	}
	if err != nil {
		t.Fatalf("read source-tree C ABI header: %v", err)
	}
	if !bytes.Equal(bundled, source) {
		t.Fatalf("bundled C ABI header is out of sync with %s", sourcePath)
	}
}

func TestErrorCodeMetadataMatchesCABI(t *testing.T) {
	cases := []ErrorCode{
		OK,
		InvalidArgument,
		NotFound,
		VersionConflict,
		IntentConflict,
		TxnNotFound,
		Busy,
		OutcomeUnknown,
		Unsupported,
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

func TestLiteOpenReadOnlyMissingDoesNotCreate(t *testing.T) {
	for name, open := range map[string]func(string) (*DB, error){
		"writer":      Open,
		"readonly":    OpenReadonly,
		"status-only": OpenStatusOnly,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name+".aflite")
			db, err := open(path)
			if err != NotFound {
				if db != nil {
					db.Close()
				}
				t.Fatalf("open missing %s database error = %v, want %v", name, err, NotFound)
			}
			if db != nil {
				db.Close()
				t.Fatalf("open missing %s database returned a handle", name)
			}
			if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
				t.Fatalf("missing %s open created or exposed file: stat err = %v", name, statErr)
			}
		})
	}
}

func TestLiteOpenModeConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-open-modes.aflite")

	writer, err := Create(path)
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
	if err := readonly.Batch([]WriteIntent{{
		Key:   "doc:readonly-write",
		Value: []byte(`{"title":"readonly write"}`),
	}}, 2); err != InvalidArgument {
		readonly.Close()
		t.Fatalf("readonly batch error = %v, want %v", err, InvalidArgument)
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
	if err := statusOnly.Batch([]WriteIntent{{
		Key:   "doc:status-only-write",
		Value: []byte(`{"title":"status only write"}`),
	}}, 3); err != InvalidArgument {
		statusOnly.Close()
		t.Fatalf("status-only batch error = %v, want %v", err, InvalidArgument)
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

func TestLiteHostedPauseResumeGeneratedEnrichment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-hosted-resume.aflite")

	hosted, err := CreateHosted(path)
	if err != nil {
		t.Fatalf("open hosted Lite database: %v", err)
	}
	hostedCaps, err := hosted.Capabilities()
	if err != nil {
		hosted.Close()
		t.Fatalf("hosted capabilities: %v", err)
	}
	if !hostedCaps.ManualMaintenance || hostedCaps.BackgroundEnrichmentRuntime {
		hosted.Close()
		t.Fatalf("hosted capabilities should expose manual maintenance without background enrichment: %#v", hostedCaps)
	}
	if err := hosted.AddEnrichmentJSON([]byte(`{"name":"resume_chunks_v1","kind":"chunk","field":"body","chunk_size":24,"chunk_overlap":0}`)); err != nil {
		hosted.Close()
		t.Fatalf("hosted add chunk enrichment: %v", err)
	}
	if err := hosted.AddIndexJSON([]byte(`{"name":"resume_ft_body","kind":"full_text","config_json":"{\"chunk_name\":\"resume_chunks_v1\"}"}`)); err != nil {
		hosted.Close()
		t.Fatalf("hosted add full-text index: %v", err)
	}
	batchOut, err := hosted.BatchJSON([]byte(`{"inserts":{"doc:go-resume":{"title":"paused","body":"go manual maintenance pause resume phrase"}},"sync_level":"write"}`))
	if err != nil {
		hosted.Close()
		t.Fatalf("hosted batch source document: %v", err)
	}
	if !bytes.Contains(batchOut, []byte(`"inserted":1`)) {
		hosted.Close()
		t.Fatalf("hosted batch source document response = %s, want inserted count", batchOut)
	}
	hostedLookup, err := hosted.LookupJSON("doc:go-resume")
	if err != nil {
		hosted.Close()
		t.Fatalf("hosted lookup source document after batch: %v", err)
	}
	if !bytes.Contains(hostedLookup, []byte("pause resume phrase")) {
		hosted.Close()
		t.Fatalf("hosted lookup source document = %s, want body text", hostedLookup)
	}
	pendingBefore, err := hosted.PendingWorkStats()
	if err != nil {
		hosted.Close()
		t.Fatalf("hosted pending work: %v", err)
	}
	if !pendingBefore.HasAsyncIndexes {
		hosted.Close()
		t.Fatalf("hosted pending work should expose async index debt: %#v", pendingBefore)
	}
	if err := hosted.Close(); err != nil {
		t.Fatalf("close hosted Lite database: %v", err)
	}

	resumed, err := OpenWithOptions(path, OpenOptions{
		Mode:                      OpenModeWriter,
		Profile:                   ProfileNative,
		GeneratedEnrichmentReplay: true,
	})
	if err != nil {
		t.Fatalf("open native Lite database after hosted pause: %v", err)
	}
	defer resumed.Close()

	resumedLookup, err := resumed.LookupJSON("doc:go-resume")
	if err != nil {
		enrichments, _ := resumed.EnrichmentsJSON()
		indexes, _ := resumed.IndexesJSON()
		t.Fatalf("resumed lookup source document after hosted close: %v; enrichments=%s indexes=%s", err, enrichments, indexes)
	}
	if !bytes.Contains(resumedLookup, []byte("pause resume phrase")) {
		t.Fatalf("resumed lookup source document = %s, want body text", resumedLookup)
	}

	replayed, err := resumed.ReplayGeneratedEnrichments()
	if err != nil {
		t.Fatalf("replay generated enrichments: %v", err)
	}
	if replayed.Replayed == 0 {
		enrichments, _ := resumed.EnrichmentsJSON()
		indexes, _ := resumed.IndexesJSON()
		t.Fatalf("replay generated enrichments = %#v, want nonzero replay after hosted pause; enrichments=%s indexes=%s", replayed, enrichments, indexes)
	}
	idle, err := resumed.RunUntilIdleStatus()
	if err != nil {
		t.Fatalf("run until idle after replay: %v", err)
	}
	if !idle.HasAsyncIndexes || idle.DerivedTargetSequence == 0 {
		t.Fatalf("post-replay idle status missing index readiness fields: %#v", idle)
	}
	result, err := resumed.SearchJSON([]byte(`{"full_text_search":{"match":{"field":"body","text":"resume phrase"}},"limit":1}`))
	if err != nil {
		t.Fatalf("search resumed full-text index: %v", err)
	}
	if !bytes.Contains(result, []byte("doc:go-resume")) {
		stats, _ := resumed.StatsJSON()
		t.Fatalf("resumed full-text search JSON %q did not contain restored document; stats=%s", result, stats)
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
	db, err := CreateWithOptions(path, OpenOptions{
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
	for _, field := range []string{
		"portable_import_publication_in_progress",
		"portable_import_recovery_required",
		"portable_runtime_activation_pending",
	} {
		if !bytes.Contains(pendingJSON, []byte(field)) {
			t.Fatalf("pending work JSON %q did not include restore readiness field %q", pendingJSON, field)
		}
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
	graphQuery := []byte(`{"graph_queries":{"neighbors":{"index":"gr_links_v1","traverse":{"start":{"keys":["doc:go-search"]},"edge_types":["links"],"max_depth":1}}},"limit":10}`)
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
		typedStatus.Storage.ReplayLayout != "native_replay_lanes_in_document_catalog" ||
		typedStatus.Storage.IndexLayout != "native_index_catalog_pages" {
		t.Fatalf("typed status storage layout = %#v", typedStatus.Storage)
	}
	if typedStatus.Storage.IndexNamespace == nil || *typedStatus.Storage.IndexNamespace != "__antfly_lite" {
		t.Fatalf("typed status index namespace = %#v", typedStatus.Storage.IndexNamespace)
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
	remoteDB, err := CreateWithOptions(remotePath, OpenOptions{
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
		!remoteStatus.Capabilities.RemoteInferenceProviders {
		t.Fatalf("remote inference status capabilities = %#v", remoteStatus.Capabilities)
	}

	localPath := filepath.Join(t.TempDir(), "go-local-inference.aflite")
	localDB, err := CreateWithOptions(localPath, OpenOptions{
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
	localRuntimeAvailable := localCaps.LocalInferenceRuntime
	expectedLocalMode := InferenceModeCallerSuppliedOrDisabled
	if localRuntimeAvailable {
		expectedLocalMode = InferenceModeLocalEmbedded
	}
	if localStatus.Inference.Mode != expectedLocalMode ||
		localStatus.Inference.Configured != localRuntimeAvailable ||
		localStatus.Inference.RemoteProviderConfigured ||
		!localStatus.Inference.LocalRuntimeConfigured ||
		localStatus.Inference.LocalRuntimeAvailable != localRuntimeAvailable {
		t.Fatalf("local inference status = %#v", localStatus.Inference)
	}
	if localStatus.Capabilities.InferenceMode != expectedLocalMode ||
		localStatus.Capabilities.LocalInferenceRuntime != localRuntimeAvailable ||
		containsString(localStatus.Capabilities.AvailableInferenceModes, InferenceModeLocalEmbedded) != localRuntimeAvailable {
		t.Fatalf("local inference status capabilities = %#v", localStatus.Capabilities)
	}
	if localCaps.InferenceMode != expectedLocalMode ||
		localCaps.LocalInferenceRuntime != localRuntimeAvailable ||
		containsString(localCaps.AvailableInferenceModes, InferenceModeLocalEmbedded) != localRuntimeAvailable {
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
	if typedCaps.DistributedShardOwnership ||
		typedCaps.RaftReplication ||
		typedCaps.ClusterPlacement ||
		typedCaps.CrossNodeJoins ||
		typedCaps.RemoteShardFanout ||
		typedCaps.DistributedTransactionCoordination ||
		typedCaps.ClusterHeartbeatStatusAggregation ||
		typedCaps.ServerSideAutoscaling ||
		typedCaps.KubernetesOperator ||
		typedCaps.ObjectStoragePrimary {
		t.Fatalf("typed capabilities should not advertise distributed features: %#v", typedCaps)
	}

	checkReport, err := db.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !checkReport.Valid || checkReport.FileSize == 0 || checkReport.CompactSize == 0 || checkReport.Issue != nil {
		t.Fatalf("unexpected check report: %#v", checkReport)
	}

	badPath := filepath.Join(t.TempDir(), "go-truncated.aflite")
	if err := os.WriteFile(badPath, []byte("short native lite header"), 0o600); err != nil {
		t.Fatalf("write truncated lite file: %v", err)
	}
	badReport, err := CheckFile(badPath)
	if err != nil {
		t.Fatalf("check truncated lite file: %v", err)
	}
	if badReport.Valid || badReport.Issue == nil || *badReport.Issue != "truncated_header" {
		t.Fatalf("truncated file check report = %#v", badReport)
	}

	pinnedSnapshotPath := filepath.Join(t.TempDir(), "go-pinned-snapshot.aflite")
	if err := db.Batch([]WriteIntent{{
		Key:   "doc:go-pinned",
		Value: []byte(`{"title":"pinned-before"}`),
	}}, 9_100); err != nil {
		t.Fatalf("seed pinned snapshot document: %v", err)
	}
	pinnedReader, err := OpenReadonly(path)
	if err != nil {
		t.Fatalf("open pinned snapshot reader: %v", err)
	}
	if err := db.Batch([]WriteIntent{{
		Key:   "doc:go-pinned",
		Value: []byte(`{"title":"pinned-after-a"}`),
	}}, 9_200); err != nil {
		pinnedReader.Close()
		t.Fatalf("advance pinned snapshot document a: %v", err)
	}
	if err := db.Batch([]WriteIntent{{
		Key:   "doc:go-pinned",
		Value: []byte(`{"title":"pinned-after-b"}`),
	}}, 9_300); err != nil {
		pinnedReader.Close()
		t.Fatalf("advance pinned snapshot document b: %v", err)
	}
	pinnedSnapshotReport, err := pinnedReader.CopyStableSnapshot(pinnedSnapshotPath, false)
	if closeErr := pinnedReader.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("copy pinned reader snapshot: %v", err)
	}
	if pinnedSnapshotReport.TailBytes == 0 {
		t.Fatalf("pinned snapshot report did not observe writer tail: %#v", pinnedSnapshotReport)
	}
	pinnedSnapshotCheck, err := CheckFile(pinnedSnapshotPath)
	if err != nil {
		t.Fatalf("check pinned snapshot: %v", err)
	}
	if !pinnedSnapshotCheck.Valid || pinnedSnapshotCheck.TailBytes != 0 {
		t.Fatalf("pinned snapshot check = %#v", pinnedSnapshotCheck)
	}
	pinnedSnapshot, err := OpenReadonly(pinnedSnapshotPath)
	if err != nil {
		t.Fatalf("open pinned snapshot: %v", err)
	}
	pinnedSnapshotLookup, err := pinnedSnapshot.LookupJSON("doc:go-pinned")
	if closeErr := pinnedSnapshot.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("lookup pinned snapshot: %v", err)
	}
	if !bytes.Contains(pinnedSnapshotLookup, []byte("pinned-before")) || bytes.Contains(pinnedSnapshotLookup, []byte("pinned-after")) {
		t.Fatalf("pinned snapshot lookup JSON %q did not preserve reader checkpoint", pinnedSnapshotLookup)
	}
	pinnedWriterLookup, err := db.LookupJSON("doc:go-pinned")
	if err != nil {
		t.Fatalf("lookup pinned writer: %v", err)
	}
	if !bytes.Contains(pinnedWriterLookup, []byte("pinned-after-b")) {
		t.Fatalf("writer lookup JSON %q did not contain latest pinned value", pinnedWriterLookup)
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
	if _, err := db.CopyStableSnapshot(filepath.Join(t.TempDir(), "go-snapshot.afb"), false); err != InvalidArgument {
		t.Fatalf("handle snapshot to .afb error = %v, want %v", err, InvalidArgument)
	}

	snapshotFilePath := filepath.Join(t.TempDir(), "go-snapshot-file.aflite")
	snapshotFileReport, err := CopyStableSnapshotFile(path, snapshotFilePath, false)
	if err != nil {
		t.Fatalf("copy stable snapshot file: %v", err)
	}
	if snapshotFileReport.SnapshotSize == 0 || snapshotFileReport.PageCount == 0 {
		t.Fatalf("unexpected snapshot file report: %#v", snapshotFileReport)
	}
	snapshotFile, err := OpenReadonly(snapshotFilePath)
	if err != nil {
		t.Fatalf("open copied stable snapshot file: %v", err)
	}
	snapshotFileLookup, err := snapshotFile.LookupJSON("doc:go-smoke")
	if closeErr := snapshotFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("lookup copied stable snapshot file: %v", err)
	}
	if !bytes.Contains(snapshotFileLookup, []byte("go api lite")) {
		t.Fatalf("snapshot file lookup JSON %q did not contain written document", snapshotFileLookup)
	}
	if _, err := CopyStableSnapshotFile(path, snapshotFilePath, false); err == nil {
		t.Fatalf("snapshot file without replace unexpectedly overwrote target")
	}
	if _, err := CopyStableSnapshotFile(path, filepath.Join(t.TempDir(), "go-snapshot-file.afb"), false); err != InvalidArgument {
		t.Fatalf("snapshot file to .afb error = %v, want %v", err, InvalidArgument)
	}

	compactReport, err := db.Compact()
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !compactReport.Compacted || compactReport.Vacuum.BeforeSize == 0 || compactReport.Vacuum.AfterSize == 0 {
		t.Fatalf("unexpected compact report: %#v", compactReport)
	}

	vacuumReport, err := db.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if vacuumReport.BeforeSize == 0 || vacuumReport.AfterSize == 0 {
		t.Fatalf("unexpected vacuum report: %#v", vacuumReport)
	}

	openTxnID := TxnID{0x67, 0x6f, 0x2d, 0x6c, 0x69, 0x74, 0x65, 0x2d, 0x6f, 0x70, 0x65, 0x6e, 0, 0, 0, 2}
	if err := db.BeginTransaction(openTxnID, 9_400, nil); err != nil {
		t.Fatalf("begin open transaction before backup: %v", err)
	}
	if err := db.WriteTransaction(openTxnID, []WriteIntent{{
		Key:   "doc:go-pending-backup",
		Value: []byte(`{"title":"pending backup write"}`),
	}}); err != nil {
		t.Fatalf("write open transaction before backup: %v", err)
	}
	openTxnSnapshotPath := filepath.Join(t.TempDir(), "go-open-txn-snapshot.aflite")
	if _, err := CopyStableSnapshotFile(path, openTxnSnapshotPath, false); err != nil {
		t.Fatalf("snapshot with open transaction: %v", err)
	}
	openTxnSnapshot, err := OpenReadonly(openTxnSnapshotPath)
	if err != nil {
		t.Fatalf("open snapshot with open transaction: %v", err)
	}
	openTxnSnapshotCommittedLookup, err := openTxnSnapshot.LookupJSON("doc:go-smoke")
	if err != nil {
		openTxnSnapshot.Close()
		t.Fatalf("lookup committed doc from open-transaction snapshot: %v", err)
	}
	if !bytes.Contains(openTxnSnapshotCommittedLookup, []byte("go api lite")) {
		openTxnSnapshot.Close()
		t.Fatalf("open-transaction snapshot lookup JSON %q did not contain committed document", openTxnSnapshotCommittedLookup)
	}
	openTxnSnapshotPendingLookup, snapshotPendingErr := openTxnSnapshot.LookupJSON("doc:go-pending-backup")
	if snapshotPendingErr != nil && snapshotPendingErr != NotFound {
		openTxnSnapshot.Close()
		t.Fatalf("lookup pending doc from open-transaction snapshot: %v", snapshotPendingErr)
	}
	if closeErr := openTxnSnapshot.Close(); closeErr != nil {
		t.Fatalf("close open-transaction snapshot: %v", closeErr)
	}
	if snapshotPendingErr == nil && bytes.Contains(openTxnSnapshotPendingLookup, []byte("pending backup write")) {
		t.Fatalf("open-transaction snapshot included unresolved write: %q", openTxnSnapshotPendingLookup)
	}
	openTxnBackupPath := filepath.Join(t.TempDir(), "go-open-txn-backup.afb")
	if err := db.BackupToFile(openTxnBackupPath); err != nil {
		t.Fatalf("backup with open transaction: %v", err)
	}
	openTxnRestoredPath := filepath.Join(t.TempDir(), "go-open-txn-restored.aflite")
	if err := RestoreBackupFile(openTxnRestoredPath, openTxnBackupPath, false); err != nil {
		t.Fatalf("restore backup with open transaction: %v", err)
	}
	openTxnRestored, err := OpenReadonly(openTxnRestoredPath)
	if err != nil {
		t.Fatalf("open restored backup with open transaction: %v", err)
	}
	openTxnCommittedLookup, err := openTxnRestored.LookupJSON("doc:go-smoke")
	if err != nil {
		openTxnRestored.Close()
		t.Fatalf("lookup committed doc from open-transaction backup: %v", err)
	}
	if !bytes.Contains(openTxnCommittedLookup, []byte("go api lite")) {
		openTxnRestored.Close()
		t.Fatalf("open-transaction backup lookup JSON %q did not contain committed document", openTxnCommittedLookup)
	}
	openTxnPendingLookup, pendingErr := openTxnRestored.LookupJSON("doc:go-pending-backup")
	if pendingErr != nil && pendingErr != NotFound {
		openTxnRestored.Close()
		t.Fatalf("lookup pending doc from open-transaction backup: %v", pendingErr)
	}
	if closeErr := openTxnRestored.Close(); closeErr != nil {
		t.Fatalf("close open-transaction backup restore: %v", closeErr)
	}
	if pendingErr == nil && bytes.Contains(openTxnPendingLookup, []byte("pending backup write")) {
		t.Fatalf("open-transaction backup restored unresolved write: %q", openTxnPendingLookup)
	}
	if err := db.ResolveTransaction(openTxnID, TxnAborted, 0); err != nil {
		t.Fatalf("abort open transaction after backup: %v", err)
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

	exportPath := filepath.Join(t.TempDir(), "go-export.afb")
	if err := db.ExportToFile(exportPath); err != nil {
		t.Fatalf("export to file: %v", err)
	}
	if info, err := os.Stat(exportPath); err != nil {
		t.Fatalf("export file: %v", err)
	} else if info.Size() == 0 {
		t.Fatalf("export file is empty: %s", exportPath)
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

	restoredAliasPath := filepath.Join(t.TempDir(), "go-restored-alias.aflite")
	if err := Restore(restoredAliasPath, backupBytes, false); err != nil {
		t.Fatalf("restore alias bytes: %v", err)
	}
	if err := Restore(restoredAliasPath, backupBytes, false); err == nil {
		t.Fatalf("restore alias without replace unexpectedly overwrote target")
	}

	restoredAliasFilePath := filepath.Join(t.TempDir(), "go-restored-alias-file.aflite")
	if err := RestoreFile(restoredAliasFilePath, exportPath, false); err != nil {
		t.Fatalf("restore alias file: %v", err)
	}

	lockedRestorePath := filepath.Join(t.TempDir(), "go-locked-restore.aflite")
	lockedRestore, err := Create(lockedRestorePath)
	if err != nil {
		t.Fatalf("open locked restore target: %v", err)
	}
	if err := lockedRestore.Batch([]WriteIntent{{
		Key:   "doc:locked-restore-target",
		Value: []byte(`{"title":"locked restore target survives"}`),
	}}, 7); err != nil {
		t.Fatalf("write locked restore target: %v", err)
	}
	if err := RestoreBackup(lockedRestorePath, backupBytes, true); err != Busy {
		t.Fatalf("restore into active writer = %v, want %v", err, Busy)
	}
	lockedLookup, err := lockedRestore.LookupJSON("doc:locked-restore-target")
	if err != nil {
		t.Fatalf("lookup locked restore target: %v", err)
	}
	if !bytes.Contains(lockedLookup, []byte("locked restore target survives")) {
		t.Fatalf("locked restore target lookup JSON %q did not contain original document", lockedLookup)
	}
	if _, err := os.Stat(lockedRestorePath + ".restore-tmp.aflite"); !os.IsNotExist(err) {
		t.Fatalf("locked restore left temp file behind: %v", err)
	}
	if err := lockedRestore.Close(); err != nil {
		t.Fatalf("close locked restore target: %v", err)
	}

	importedPath := filepath.Join(t.TempDir(), "go-imported.aflite")
	imported, err := Create(importedPath)
	if err != nil {
		t.Fatalf("open imported Lite database: %v", err)
	}
	exportBytes, err := db.Export()
	if err != nil {
		t.Fatalf("export bytes: %v", err)
	}
	if len(exportBytes) == 0 {
		t.Fatalf("export bytes are empty")
	}
	if err := imported.Import(exportBytes); err != nil {
		t.Fatalf("import bytes: %v", err)
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
	ttlDB, err := CreateWithOptions(ttlPath, OpenOptions{
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
	hosted, err := CreateHosted(hostedPath)
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

	if err := hosted.AddIndexJSON(index); err != nil {
		t.Fatalf("hosted add full-text index: %v", err)
	}
	if err := hosted.Batch([]WriteIntent{{
		Key:   "doc:go-hosted-search",
		Value: []byte(`{"title":"hosted","body":"go hosted manual maintenance search"}`),
	}}, 9); err != nil {
		t.Fatalf("hosted batch searchable document: %v", err)
	}
	hostedIdleStatus, err := hosted.RunUntilIdleStatus()
	if err != nil {
		t.Fatalf("hosted run until idle status: %v", err)
	}
	if hostedIdleStatus.DerivedTargetSequence == 0 || !hostedIdleStatus.HasAsyncIndexes || len(hostedIdleStatus.TextMerge) == 0 {
		t.Fatalf("hosted run-until-idle status missing readiness fields: %#v", hostedIdleStatus)
	}
	hostedFullTextQuery := []byte(`{"full_text_search":{"match":{"field":"body","text":"hosted manual maintenance"}},"limit":1}`)
	assertSearchContains(hosted, "hosted full-text", hostedFullTextQuery, "go hosted manual maintenance search")

	hostedStatus, err := hosted.Status()
	if err != nil {
		t.Fatalf("hosted status: %v", err)
	}
	if !hostedStatus.Capabilities.HostedProfile || !hostedStatus.Capabilities.ManualMaintenance {
		t.Fatalf("hosted status should include hosted capabilities: %#v", hostedStatus.Capabilities)
	}

	hostedTTLPath := filepath.Join(t.TempDir(), "go-hosted-ttl.aflite")
	hostedWithTTL, err := CreateWithOptions(hostedTTLPath, OpenOptions{
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
