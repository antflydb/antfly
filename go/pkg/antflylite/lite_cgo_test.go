// Copyright 2026 Antfly, Inc.
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

//go:build cgo && antflylite_capi

package antflylite

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestLiteCAPIProvisionsDefaultFullTextIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-default-index.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
		NoSync:  true,
	})
	if err != nil {
		t.Fatalf("create Lite database: %v", err)
	}
	defer db.Close()

	// Creating a Lite database provisions the default full-text index,
	// matching the server's behavior on table create. No `lite index
	// create`/AddIndexJSON call is needed for basic text search to work.
	indexes, err := db.IndexesJSON()
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	if !bytes.Contains(indexes, []byte("full_text_index_v0")) {
		t.Fatalf("indexes JSON %q did not contain the default full text index", indexes)
	}

	if err := db.Batch([]WriteIntent{{
		Key:   "doc:default-index",
		Value: []byte(`{"title":"searchable","body":"hybrid alpha without an explicit index"}`),
	}}, 1); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle status: %v", err)
	}

	hybridQuery := []byte(`{"full_text_search":{"match":{"field":"body","text":"hybrid alpha"}},"limit":3}`)
	result, err := db.SearchJSON(hybridQuery)
	if err != nil {
		t.Fatalf("hybrid full text search with no index name: %v", err)
	}
	if !bytes.Contains(result, []byte("doc:default-index")) {
		t.Fatalf("hybrid search JSON %q did not contain the searchable document", result)
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

// fakeRemoteEmbeddingVector deterministically derives a fake embedding from
// the exact text an antfly-provider embedder would send, so a test can both
// serve embed responses from a fake inference server and independently
// reconstruct the vector a query for the same text should match against.
func fakeRemoteEmbeddingVector(text string, dims int) []float64 {
	vec := make([]float64, dims)
	for d := 0; d < dims; d++ {
		h := fnv.New32a()
		_, _ = h.Write([]byte(fmt.Sprintf("%s|%d", text, d)))
		vec[d] = float64(h.Sum32()%1000) / 1000.0
	}
	return vec
}

// newFakeAntflyEmbedServer starts an httptest server implementing the
// minimal antfly inference embed endpoint (POST .../embed, request
// {"model":...,"input":[...]}, response
// {"object":"list","data":[{"object":"embedding","index":...,"embedding":[...]}]})
// that managed_embedder.zig's antfly-provider client calls for an embedder
// config carrying its own api_url. It also answers the GET capability-probe
// path with 404, which the client tolerates by falling back to a
// conservative default (see remote_capabilities.zig's discoverOnce and
// managed_embedder.zig's densePartLeaseForEntry). Returns the server and a
// counter of embed requests received.
func newFakeAntflyEmbedServer(t *testing.T, dims int) (*httptest.Server, *int32) {
	t.Helper()
	var embedCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/embed") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&embedCalls, 1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data := make([]map[string]any, len(req.Input))
		for i, text := range req.Input {
			data[i] = map[string]any{
				"object":    "embedding",
				"index":     i,
				"embedding": fakeRemoteEmbeddingVector(text, dims),
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "model": req.Model})
	}))
	t.Cleanup(server.Close)
	return server, &embedCalls
}

// TestLiteNativeRemoteProviderEmbedsAndSearchesViaAPIURL reproduces (without
// a real model) the runtime gap this change fixes: a native Lite database
// created with RemoteProviderConfigured and a dense_vector index whose
// embedder config carries its own api_url must actually call that inference
// service to compute embeddings, and RunUntilIdleStatus must wait for that
// work, rather than silently publishing zero vectors.
func TestLiteNativeRemoteProviderEmbedsAndSearchesViaAPIURL(t *testing.T) {
	const dims = 4
	server, embedCalls := newFakeAntflyEmbedServer(t, dims)

	path := filepath.Join(t.TempDir(), "remote-embed.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	indexConfig, err := json.Marshal(map[string]any{
		"field":  "body",
		"dims":   dims,
		"metric": "l2_squared",
		"embedder": map[string]any{
			"provider": "antfly",
			"model":    "fake-embedder",
			"api_url":  server.URL,
		},
	})
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}
	addIndex, err := json.Marshal(map[string]any{
		"name":        "dv_remote_v1",
		"kind":        "dense_vector",
		"config_json": string(indexConfig),
	})
	if err != nil {
		t.Fatalf("marshal add-index request: %v", err)
	}
	if err := db.AddIndexJSON(addIndex); err != nil {
		t.Fatalf("add remote embedder dense index: %v", err)
	}

	const bodyOne = "alpha searchable remote embedding text"
	const bodyTwo = "beta unrelated remote embedding text"
	if err := db.Batch([]WriteIntent{
		{Key: "doc:alpha", Value: []byte(fmt.Sprintf(`{"title":"alpha","body":%q}`, bodyOne))},
		{Key: "doc:beta", Value: []byte(fmt.Sprintf(`{"title":"beta","body":%q}`, bodyTwo))},
	}, 2); err != nil {
		t.Fatalf("batch write documents: %v", err)
	}

	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	if atomic.LoadInt32(embedCalls) == 0 {
		t.Fatalf("fake inference server received no /ai/v1/embed requests; embedding runtime was not wired up")
	}

	query, err := json.Marshal(map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": "alpha"}},
		"embeddings":       map[string]any{"dv_remote_v1": fakeRemoteEmbeddingVector(bodyOne, dims)},
		"indexes":          []string{"dv_remote_v1"},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"limit":            3,
	})
	if err != nil {
		t.Fatalf("marshal hybrid search request: %v", err)
	}
	result, err := db.SearchJSON(query)
	if err != nil {
		t.Fatalf("hybrid semantic search: %v", err)
	}
	if !bytes.Contains(result, []byte("doc:alpha")) {
		t.Fatalf("hybrid semantic search result %q did not contain doc:alpha", result)
	}
}

func liteLocalEmbeddingModelAvailable() bool {
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(home, ".antfly", "inference", "models", "Qwen", "Qwen3-Embedding-0.6B-GGUF*"))
	if err != nil {
		return false
	}
	return len(matches) > 0
}

// enrichmentPendingWorkStatus is the stable subset of the Lite "enrichment"
// pending-work telemetry fields (see capi/db.zig's PendingWorkStats), used
// here to assert local embedding work fully drains.
type enrichmentPendingWorkStatus struct {
	TargetSequence  uint64 `json:"target_sequence"`
	AppliedSequence uint64 `json:"applied_sequence"`
	ErrorCount      uint64 `json:"error_count"`
	FatalErrorCount uint64 `json:"fatal_error_count"`
	Stalled         bool   `json:"stalled"`
}

// TestLiteCAPILocalEmbeddedInferenceVariant exercises libantfly's embedded
// local inference runtime through the Go binding: it creates a native Lite
// database with LocalRuntimeConfigured, adds a dense_vector index and
// embedder enrichment backed by the local Qwen embedding model with no
// api_url, writes documents, and asserts the resulting embedding work
// drains cleanly to idle (mirroring capi/db.zig's "capi lite drains an
// antfly embedder with no api_url through the embedded inference provider"
// test). It skips cleanly when the loaded libantfly does not advertise
// LocalInferenceRuntime, and when the model has not been pulled, so it is
// safe to leave enabled in normal `go test` runs.
func TestLiteCAPILocalEmbeddedInferenceVariant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-local-inference-variant.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                   OpenModeWriter,
		Profile:                ProfileNative,
		LocalRuntimeConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native Lite database with local runtime configured: %v", err)
	}
	defer db.Close()

	caps, err := db.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !caps.LocalInferenceRuntime {
		t.Skip("loaded libantfly does not advertise a local inference runtime")
	}
	if !liteLocalEmbeddingModelAvailable() {
		t.Skip("Qwen3-Embedding-0.6B-GGUF model is not present under ~/.antfly/inference/models/Qwen")
	}

	indexJSON := []byte(`{"name":"go_body_embedding","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"dims\":1024,\"metric\":\"cosine\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}`)
	if err := db.AddIndexJSON(indexJSON); err != nil {
		t.Fatalf("add dense vector index: %v", err)
	}

	enrichmentJSON := []byte(`{"name":"go_body_embedder","kind":"embedding","field":"body","vector_space":"go_body_embedding","producer_json":"{\"type\":\"embedder\",\"config\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}`)
	if err := db.AddEnrichmentJSON(enrichmentJSON); err != nil {
		t.Fatalf("add embedding enrichment: %v", err)
	}

	batchJSON := []byte(`{"inserts":{"doc:go-local-a":{"body":"antfly lite embeds documents locally through the go binding"},"doc:go-local-b":{"body":"a second document for the local embedding drain test"}},"sync_level":"write"}`)
	batchOut, err := db.BatchJSON(batchJSON)
	if err != nil {
		t.Fatalf("batch write documents: %v", err)
	}
	if !bytes.Contains(batchOut, []byte(`"inserted":2`)) {
		t.Fatalf("batch response = %s, want inserted count of 2", batchOut)
	}

	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	pending, err := db.PendingWorkStats()
	if err != nil {
		t.Fatalf("pending work stats: %v", err)
	}
	var enrichment enrichmentPendingWorkStatus
	if err := json.Unmarshal(pending.Enrichment, &enrichment); err != nil {
		t.Fatalf("decode enrichment pending work: %v; raw=%s", err, pending.Enrichment)
	}
	if enrichment.ErrorCount != 0 || enrichment.FatalErrorCount != 0 || enrichment.Stalled ||
		enrichment.TargetSequence != enrichment.AppliedSequence {
		t.Fatalf("local embedding enrichment did not drain cleanly: %#v", enrichment)
	}
}

// TestLiteOpenOptionsResourceBudgetPlumbing confirms the OpenOptions
// resource-budget fields (HostBudgetMB, BackendBudgetMB, CombinedBudgetMB,
// KVBudgetMB, ScratchBudgetMB, ProcessMemoryBudgetMB) round-trip through the
// C ABI (antfly_lite_open_options) to the embedded node and back out through
// Status().Inference, and that a handle opened with LocalRuntimeConfigured
// but no override does not fall back to the previous zero-bytes/automatic
// generation-budget policy that could not admit even one
// boundary-architecture extraction window regardless of request size (see
// GLINER25.md's "Memory budget" section and this task's
// gliner25-longdoc-handoff.md).
func TestLiteOpenOptionsResourceBudgetPlumbing(t *testing.T) {
	explicitPath := filepath.Join(t.TempDir(), "go-inference-budget-explicit.aflite")
	explicitDB, err := CreateWithOptions(explicitPath, OpenOptions{
		Mode:                   OpenModeWriter,
		Profile:                ProfileNative,
		LocalRuntimeConfigured: true,
		HostBudgetMB:           256,
		BackendBudgetMB:        128,
		CombinedBudgetMB:       384,
		KVBudgetMB:             64,
		ScratchBudgetMB:        32,
		ProcessMemoryBudgetMB:  512,
	})
	if err != nil {
		t.Fatalf("create native Lite database with explicit resource budgets: %v", err)
	}
	defer explicitDB.Close()

	explicitStatus, err := explicitDB.Status()
	if err != nil {
		t.Fatalf("explicit budget status: %v", err)
	}
	inf := explicitStatus.Inference
	if inf.HostBudgetMB != 256 || inf.BackendBudgetMB != 128 || inf.CombinedBudgetMB != 384 ||
		inf.KVBudgetMB != 64 || inf.ScratchBudgetMB != 32 || inf.ProcessMemoryBudgetMB != 512 {
		t.Fatalf("explicit resource budgets not reported back: %#v", inf)
	}
	if inf.LocalRuntimeAvailable {
		// The node actually started against the explicit override: its
		// resolved process-memory envelope is exactly the requested 512 MiB
		// (never clamped -- no test runner has less than 512 MiB) and its
		// provenance is "explicit", not a host/cgroup guess.
		if inf.ProcessMemoryLimitBytes != 512*1024*1024 {
			t.Fatalf("resolved process memory limit = %d, want 512 MiB", inf.ProcessMemoryLimitBytes)
		}
		if inf.ProcessMemoryLimitSource != "explicit" {
			t.Fatalf("resolved process memory limit source = %q, want \"explicit\"", inf.ProcessMemoryLimitSource)
		}
	}

	defaultPath := filepath.Join(t.TempDir(), "go-inference-budget-default.aflite")
	defaultDB, err := CreateWithOptions(defaultPath, OpenOptions{
		Mode:                   OpenModeWriter,
		Profile:                ProfileNative,
		LocalRuntimeConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native Lite database with default resource budgets: %v", err)
	}
	defer defaultDB.Close()

	defaultStatus, err := defaultDB.Status()
	if err != nil {
		t.Fatalf("default budget status: %v", err)
	}
	defInf := defaultStatus.Inference
	if defInf.LocalRuntimeAvailable {
		t.Logf("default embedded generation budgets on this machine: host=%d backend=%d combined=%d kv=%d scratch=%d process_memory_limit_bytes=%d process_memory_limit_source=%s",
			defInf.HostBudgetMB, defInf.BackendBudgetMB, defInf.CombinedBudgetMB, defInf.KVBudgetMB, defInf.ScratchBudgetMB,
			defInf.ProcessMemoryLimitBytes, defInf.ProcessMemoryLimitSource)
		if defInf.HostBudgetMB == 0 || defInf.BackendBudgetMB == 0 || defInf.CombinedBudgetMB == 0 ||
			defInf.KVBudgetMB == 0 || defInf.ScratchBudgetMB == 0 {
			t.Fatalf("default embedded generation budgets fell back to the automatic/zero-bytes policy: %#v", defInf)
		}
	}
}

// TestLiteNativeRemoteProviderSemanticSearchEmbedsQuery reproduces the other
// half of the runtime gap this change fixes: a public query's
// "semantic_search" text must be embedded through the index's own
// configured embedder (the retrieval-query task, not the document task)
// before it hits `handle.db.search`, not rejected outright because Lite's
// search path never supplied a resolver.
func TestLiteNativeRemoteProviderSemanticSearchEmbedsQuery(t *testing.T) {
	const dims = 4
	server, embedCalls := newFakeAntflyEmbedServer(t, dims)

	path := filepath.Join(t.TempDir(), "remote-semantic-search.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	indexConfig, err := json.Marshal(map[string]any{
		"field":  "body",
		"dims":   dims,
		"metric": "l2_squared",
		"embedder": map[string]any{
			"provider": "antfly",
			"model":    "fake-embedder",
			"api_url":  server.URL,
		},
	})
	if err != nil {
		t.Fatalf("marshal index config: %v", err)
	}
	addIndex, err := json.Marshal(map[string]any{
		"name":        "dv_semantic_v1",
		"kind":        "dense_vector",
		"config_json": string(indexConfig),
	})
	if err != nil {
		t.Fatalf("marshal add-index request: %v", err)
	}
	if err := db.AddIndexJSON(addIndex); err != nil {
		t.Fatalf("add remote embedder dense index: %v", err)
	}

	if err := db.Batch([]WriteIntent{
		{Key: "doc:alpha", Value: []byte(`{"title":"alpha","body":"alpha semantic search remote embedding text"}`)},
		{Key: "doc:beta", Value: []byte(`{"title":"beta","body":"beta semantic search remote embedding text"}`)},
	}, 2); err != nil {
		t.Fatalf("batch write documents: %v", err)
	}

	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	writeEmbedCalls := atomic.LoadInt32(embedCalls)
	if writeEmbedCalls == 0 {
		t.Fatalf("fake inference server received no /ai/v1/embed requests for document writes")
	}

	query, err := json.Marshal(map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": "alpha"}},
		"semantic_search":  "how does alpha relate to semantic search",
		"indexes":          []string{"dv_semantic_v1"},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"limit":            3,
	})
	if err != nil {
		t.Fatalf("marshal hybrid semantic-search request: %v", err)
	}
	result, err := db.SearchJSON(query)
	if err != nil {
		t.Fatalf("hybrid semantic-search query: %v", err)
	}
	if !bytes.Contains(result, []byte("doc:alpha")) {
		t.Fatalf("hybrid semantic-search result %q did not contain doc:alpha", result)
	}
	if atomic.LoadInt32(embedCalls) <= writeEmbedCalls {
		t.Fatalf("fake inference server received no /ai/v1/embed request for the semantic_search query text")
	}
}

// TestLiteNativeRemoteProviderSemanticSearchMatchesDogfoodRequestShape uses
// the exact public-query request shape examples/dogfood's query.go sends
// (full_text_search + full_text_index + semantic_search + indexes +
// merge_config + fields together). A default `libantfly` build links the
// full storage internals into every Lite handle too (capi_build_options.
// linked_storage is unconditionally true), so `searchPublicQueryJson` must
// route a genuine Lite handle to the simpler internal query path (which
// resolves semantic_search via LiteSemanticResolver) instead of
// local_query_client's storage-owner/distributed-table path, which has no
// metadata catalog to resolve a Lite index's embedder against and rejects
// the request with ANTFLY_INVALID_ARGUMENT.
func TestLiteNativeRemoteProviderSemanticSearchMatchesDogfoodRequestShape(t *testing.T) {
	const dims = 4
	server, _ := newFakeAntflyEmbedServer(t, dims)

	path := filepath.Join(t.TempDir(), "dogfood-shape.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Close()

	indexConfig, _ := json.Marshal(map[string]any{
		"field": "body", "dims": dims, "metric": "l2_squared",
		"embedder": map[string]any{"provider": "antfly", "model": "fake-embedder", "api_url": server.URL},
	})
	addIndex, _ := json.Marshal(map[string]any{"name": "chunk_vectors", "kind": "dense_vector", "config_json": string(indexConfig)})
	if err := db.AddIndexJSON(addIndex); err != nil {
		t.Fatalf("add dense index: %v", err)
	}

	if err := db.Batch([]WriteIntent{
		{Key: "doc:a", Value: []byte(`{"title":"a","body":"alpha shape text"}`)},
	}, 2); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	request := map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": "alpha"}},
		"full_text_index":  "full_text_index_v0",
		"semantic_search":  "alpha shape text",
		"indexes":          []string{"chunk_vectors"},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"limit":            5,
		"fields":           []string{"title", "body"},
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal dogfood-shape request: %v", err)
	}
	result, err := db.SearchJSON(body)
	if err != nil {
		t.Fatalf("dogfood-shape hybrid search: %v result=%s", err, result)
	}
	if !bytes.Contains(result, []byte("doc:a")) {
		t.Fatalf("dogfood-shape hybrid search result %q did not contain doc:a", result)
	}
}

// newFakeAntflyExtractServer starts an httptest server implementing the
// minimal antfly inference extraction endpoint (POST .../extract, see
// zig/EXTRACT.md's request/response envelope) that the graph index's
// extractor asset producer calls for a `relations_v1` artifact whose
// producer config carries its own api_url. Every input is answered with the
// same fixed two-entity, one-relation payload so the relation's rendered
// graph target ("antfly-core", from entity_index 1) is not itself a document
// key -- the shape dogfood's `knowledge` graph index (see
// examples/dogfood/index_config.go's knowledgeGraphIndexJSON) produces
// against real GLiNER2.5 output. It also answers unrelated paths (the
// GET capability probe) with 404, which the client tolerates.
func newFakeAntflyExtractServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/extract") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var req struct {
			Model  string `json:"model"`
			Inputs []struct {
				ID      string `json:"id"`
				Content string `json:"content"`
			} `json:"inputs"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data := make([]map[string]any, len(req.Inputs))
		for i, in := range req.Inputs {
			item := map[string]any{
				"entities": []map[string]any{
					{"text": "VOPR", "label": "component", "start": 0, "end": 4, "score": 0.95},
					{"text": "antfly-core", "label": "component", "start": 10, "end": 21, "score": 0.9},
				},
				"relations": []map[string]any{
					{
						"type":   "depends_on",
						"source": map[string]any{"entity_index": 0},
						"target": map[string]any{"entity_index": 1},
						"score":  0.88,
					},
				},
			}
			if in.ID != "" {
				item["id"] = in.ID
			}
			data[i] = item
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "extraction", "model": req.Model, "data": data})
	}))
	t.Cleanup(server.Close)
	return server
}

// knowledgeGraphIndexJSONForTest mirrors examples/dogfood/index_config.go's
// knowledgeGraphIndexJSON: a graph index fed by an extractor asset producer
// (the "autograph" pattern) whose relation targets render arbitrary entity
// text rather than existing document keys.
func knowledgeGraphIndexJSONForTest(indexName, artifactName, apiURL string) ([]byte, error) {
	config := map[string]any{
		"source": map[string]any{
			"artifact": artifactName,
			"path":     "$.relations[*]",
			"format":   "extraction_relation",
			"nodes": map[string]any{
				"model":  "document",
				"target": "{{ _item.target.text }}",
			},
			"edge": map[string]any{
				"weight": "{{ _item.score }}",
				"metadata": map[string]any{
					"type":          "{{ _item.type }}",
					"source_entity": "{{ _item.source.text }}",
					"target_entity": "{{ _item.target.text }}",
					"score":         "{{ _item.score }}",
				},
			},
		},
		"artifact": map[string]any{
			"name": artifactName,
			"kind": "asset",
			"source": map[string]any{
				"type":  "field",
				"value": "body",
			},
			"content_type": "application/json",
			"producer_json": map[string]any{
				"type": "extractor",
				"config": map[string]any{
					"provider": "antfly",
					"model":    "fake-extractor",
					"api_url":  apiURL,
					"schema": map[string]any{
						"entities":  []string{"component"},
						"relations": []map[string]any{{"type": "depends_on"}},
					},
					"options": map[string]any{
						"include_confidence": true,
						"include_spans":      true,
					},
				},
			},
		},
		"algebraic_planning": map[string]any{
			"bounded_traversal": map[string]any{
				"law": "provenance_semiring",
			},
		},
	}
	inner, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal graph config: %w", err)
	}
	envelope := struct {
		Name       string `json:"name"`
		Kind       string `json:"kind"`
		ConfigJSON string `json:"config_json"`
	}{Name: indexName, Kind: "graph", ConfigJSON: string(inner)}
	return json.Marshal(envelope)
}

// TestLiteNativeGraphEdgesFromExtractionArtifact reproduces the Lite/native
// SIGABRT reported against antfly_db_get_edges_json once a graph index fed by
// an extraction asset producer has real edges: the extractor's relation
// targets are entity text ("antfly-core") rather than existing document
// keys, and the previous agent found that once such edges exist,
// handle.db.getEdges aborts the process with no panic message. This test
// reproduces that setup without any real model, using a fake `/ai/v1/extract`
// server standing in for GLiNER2.5, mirroring dogfood's `knowledge` graph
// index (examples/dogfood/index_config.go's knowledgeGraphIndexJSON).
func TestLiteNativeGraphEdgesFromExtractionArtifact(t *testing.T) {
	server := newFakeAntflyExtractServer(t)

	path := filepath.Join(t.TempDir(), "graph-extraction-edges.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	const indexName = "knowledge"
	const artifactName = "relations_v1"
	graphIndex, err := knowledgeGraphIndexJSONForTest(indexName, artifactName, server.URL)
	if err != nil {
		t.Fatalf("build knowledge graph index config: %v", err)
	}
	if err := db.AddIndexJSON(graphIndex); err != nil {
		t.Fatalf("add knowledge graph index: %v", err)
	}

	if err := db.Batch([]WriteIntent{
		{Key: "doc:vopr-design", Value: []byte(`{"title":"VOPR design","body":"VOPR depends on antfly-core for storage."}`)},
		{Key: "doc:vopr-tests", Value: []byte(`{"title":"VOPR tests","body":"VOPR test harness also depends on antfly-core."}`)},
	}, 2); err != nil {
		t.Fatalf("batch write documents: %v", err)
	}

	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	edges, err := db.EdgesJSON(indexName, "doc:vopr-design", "", 2 /* both */)
	if err != nil {
		t.Fatalf("edges json: %v", err)
	}
	if !bytes.Contains(edges, []byte("depends_on")) {
		t.Fatalf("edges JSON %q did not contain the extracted depends_on edge", edges)
	}

	neighbors, err := db.NeighborsJSON(indexName, "doc:vopr-design", "", 2 /* both */)
	if err != nil {
		t.Fatalf("neighbors json: %v", err)
	}
	t.Logf("neighbors: %s", neighbors)

	traverseRequest, err := json.Marshal(map[string]any{
		"index_name":    indexName,
		"start_key_b64": base64.StdEncoding.EncodeToString([]byte("doc:vopr-design")),
		"direction":     2,
		"max_depth":     2,
		"max_results":   10,
	})
	if err != nil {
		t.Fatalf("marshal traverse request: %v", err)
	}
	traversal, err := db.TraverseEdgesJSON(traverseRequest)
	if err != nil {
		t.Fatalf("traverse edges json: %v", err)
	}
	t.Logf("traversal: %s", traversal)

	graphQueriesRequest, err := json.Marshal(map[string]any{
		"graph_queries": []map[string]any{
			{
				"name":       "neighbors",
				"type":       "neighbors",
				"index_name": indexName,
				"start_nodes": map[string]any{
					"keys": []string{base64.StdEncoding.EncodeToString([]byte("doc:vopr-design"))},
				},
				"direction": "both",
			},
		},
		"named_sets": []map[string]any{},
		"limit":      10,
	})
	if err != nil {
		t.Fatalf("marshal graph queries request: %v", err)
	}
	graphQueries, err := db.ExecuteGraphQueriesJSON(graphQueriesRequest)
	if err != nil {
		t.Fatalf("execute graph queries json: %v", err)
	}
	t.Logf("graph queries: %s", graphQueries)
}

// newFakeAntflyExtractServerBoundaryV2 starts an httptest server answering
// every /extract call with the schema_version=2 "boundary-architecture"
// envelope shape fastino/gliner2.5-base-v1 produces (see zig/EXTRACT.md's
// "Model support" section and GLINER25.md), as opposed to
// newFakeAntflyExtractServer's plain schema_version-1-shaped envelope.
//
// The item shape below is not copied from a live capture: repeated attempts
// to capture one from `antfly inference run` against fastino/gliner2.5-base-v1
// during this work were refused with MEMORY_BUDGET_EXCEEDED even after
// passing generous explicit --process-memory-budget-mb/--host-budget-mb/
// --backend-budget-mb overrides, because concurrent sibling agents on this
// shared machine had already committed nearly all physical memory (observed
// via `top`/`vm_stat`: ~35 of 38.6 GiB used, well under 100 MiB unused) --
// see the scratchpad's inference-run*.log and boundary-response.json (the
// literal MEMORY_BUDGET_EXCEEDED body) for that record. Absent a live
// capture, this reconstructs the shape from three first-party sources that
// agree with each other: (1) zig/EXTRACT.md's response envelope plus its
// "Model support" note that fastino/gliner2.5-base-v1 answers the same
// endpoint/shape family; (2) zig/pkg/antfly/src/asset_producer_runtime.zig's
// own `extraction_v2_response_fixture` test fixture, which is the
// schema_version=2 shape the antfly-side response validator
// (validateExtractionResult, v2=true) already accepts -- entities/relations
// still at the top level of each `data[]` item, plus the v2-only
// "long_document" window-merge metadata; and (3) the inference side's actual
// wire writer for this exact request shape,
// zig/pkg/inference/src/extractors/extraction_v2.zig's `endpoint()`, which
// unconditionally writes "text" on every relation endpoint (source and
// target) in addition to "entity_index"/"label" whenever the endpoint
// resolves unambiguously to a recognized entity -- i.e. a real boundary
// relation endpoint is not exclusively one shape or the other; it can carry
// both. This fixture exercises that combined shape plus the window-merge
// metadata schema_version=1 responses never carry, on both relation
// endpoints, so it stresses `runtimeResolveGraphEndpointEntity`'s
// entity_index path (zig/pkg/antfly/src/storage/db/enrichment/
// enrichment_runtime.zig) rather than only its inline-text fallback.
func newFakeAntflyExtractServerBoundaryV2(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/extract") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var req struct {
			Model  string `json:"model"`
			Inputs []struct {
				ID      string `json:"id"`
				Content string `json:"content"`
			} `json:"inputs"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data := make([]map[string]any, len(req.Inputs))
		for i, in := range req.Inputs {
			item := map[string]any{
				"offset_unit": "utf8_bytes",
				"entities": []map[string]any{
					{"label": "component", "text": "VOPR", "start": 0, "end": 4, "score": 0.95},
					{"label": "component", "text": "antfly-core", "start": 10, "end": 21, "score": 0.9},
				},
				"relations": []map[string]any{
					{
						"type": "depends_on",
						"source": map[string]any{
							"entity_index": 0,
							"label":        "component",
							"text":         "VOPR",
							"start":        0,
							"end":          4,
							"score":        0.95,
						},
						"target": map[string]any{
							"entity_index": 1,
							"label":        "component",
							"text":         "antfly-core",
							"start":        10,
							"end":          21,
							"score":        0.9,
						},
						"score":   0.88,
						"derived": false,
					},
				},
				"long_document": map[string]any{
					"version":                    1,
					"window_count":               2,
					"window_policy":              "source_words_midpoint_ownership",
					"classification_aggregation": "owned_word_weighted_mean_raw_logits",
					"duplicate_score":            "maximum_calibrated_score",
					"natural_record_identity":    "exact_source_anchor",
					"other_record_identity":      "occurrence",
					"solver_optimality_scope":    "retained_candidate_graph",
				},
			}
			if in.ID != "" {
				item["id"] = in.ID
			}
			data[i] = item
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object":         "extraction",
			"model":          req.Model,
			"schema_version": 2,
			"data":           data,
		})
	}))
	t.Cleanup(server.Close)
	return server
}

// TestLiteNativeGraphEdgesFromExtractionArtifactBoundaryV2 is
// TestLiteNativeGraphEdgesFromExtractionArtifact's counterpart for the
// schema_version=2 "boundary" envelope shape (see
// newFakeAntflyExtractServerBoundaryV2). The dogfood ingest report that
// prompted this test observed ~289 successful GLiNER2.5
// (fastino/gliner2.5-base-v1, schema_version 2) extraction calls yet zero
// graph edges for every `entity`/`query` name tried, even though an earlier
// run against antflydb/gliner2-base-v1 (the plain schema_version-1 shape
// TestLiteNativeGraphEdgesFromExtractionArtifact already covers) produced
// edges. This asserts the same knowledge-graph mapping
// (knowledgeGraphIndexJSONForTest, matching examples/dogfood/index_config.go)
// produces edges from the v2/boundary shape exactly as it does from the v1
// shape: both the asset producer's stored artifact (the normalized `data[0]`
// record, not the raw wire envelope -- see asset_producer_runtime.zig's
// extractionResultJsonAlloc call site, which strips the "data" wrapper
// whenever the artifact's content_type is JSON, as dogfood's is) and
// `$.relations[*]`'s entity_index resolution
// (runtimeResolveGraphEndpointEntity) are exercised identically regardless
// of schema_version.
func TestLiteNativeGraphEdgesFromExtractionArtifactBoundaryV2(t *testing.T) {
	server := newFakeAntflyExtractServerBoundaryV2(t)

	path := filepath.Join(t.TempDir(), "graph-extraction-edges-boundary-v2.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	const indexName = "knowledge"
	const artifactName = "relations_v1"
	graphIndex, err := knowledgeGraphIndexJSONForTest(indexName, artifactName, server.URL)
	if err != nil {
		t.Fatalf("build knowledge graph index config: %v", err)
	}
	if err := db.AddIndexJSON(graphIndex); err != nil {
		t.Fatalf("add knowledge graph index: %v", err)
	}

	if err := db.Batch([]WriteIntent{
		{Key: "doc:vopr-design", Value: []byte(`{"title":"VOPR design","body":"VOPR depends on antfly-core for storage."}`)},
		{Key: "doc:vopr-tests", Value: []byte(`{"title":"VOPR tests","body":"VOPR test harness also depends on antfly-core."}`)},
	}, 2); err != nil {
		t.Fatalf("batch write documents: %v", err)
	}

	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("run until idle: %v", err)
	}

	edges, err := db.EdgesJSON(indexName, "doc:vopr-design", "", 2 /* both */)
	if err != nil {
		t.Fatalf("edges json: %v", err)
	}
	if !bytes.Contains(edges, []byte("depends_on")) {
		t.Fatalf("edges JSON %q did not contain the extracted depends_on edge from the boundary v2 shape", edges)
	}
	if !bytes.Contains(edges, []byte("antfly-core")) {
		t.Fatalf("edges JSON %q did not resolve the entity_index-addressed target text \"antfly-core\"", edges)
	}

	neighbors, err := db.NeighborsJSON(indexName, "doc:vopr-design", "", 2 /* both */)
	if err != nil {
		t.Fatalf("neighbors json: %v", err)
	}
	t.Logf("boundary v2 neighbors: %s", neighbors)
}

// TestLiteNativeArtifactSourcedDenseVectorChunkPipeline reproduces the
// server's chunk-artifact pattern go/pkg/docsaf/cmd/docsaf/main.go's
// createHierarchyIndexes and antfly.NewArtifactEmbeddingIndexConfig build --
// a `chunk` enrichment producing an artifact (here "document_chunks_v1"),
// consumed by an embeddings index via
// `"sources":[{"artifact":"document_chunk_dense_v1"}]`, with the producing
// `embedding` enrichment nested in that same index's own config and pointed
// at the chunk artifact through `source_artifact_name` -- against a native
// Lite handle through nothing but AddIndexJSON. Before
// `registerLiteIndexEnrichments` (capi/db.zig), a native Lite handle
// silently dropped every nested "enrichments" declaration (db.addIndex has
// no such field), so this exact shape returned a generic ANTFLY_INTERNAL:
// the "sources" artifact reference could never resolve because the
// enrichment that produces it was never registered. It asserts: (1) both
// AddIndexJSON calls succeed, (2) the chunk artifact is independently
// queryable through its own full_text index with a hit whose
// `hierarchy.parent_doc_key` resolves to the parent document, and (3) a
// hybrid full-text + semantic query against the artifact-sourced dense
// index returns a hit with the same hierarchy resolution, proving real
// vectors were published for the generated chunk artifact (not just an
// index that silently never received any data).
func TestLiteNativeArtifactSourcedDenseVectorChunkPipeline(t *testing.T) {
	const dims = 4
	server, embedCalls := newFakeAntflyEmbedServer(t, dims)

	path := filepath.Join(t.TempDir(), "artifact-sourced-dense.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	// Producer: a full_text index over the generated chunk artifact stream,
	// with the `chunk` enrichment nested in its own config -- docsaf's
	// "document_text" index.
	chunkIndex, err := json.Marshal(map[string]any{
		"name": "document_text_chunks",
		"kind": "full_text",
		"config_json": mustMarshalJSONString(t, map[string]any{
			"chunk_name": "document_chunks_v1",
			"enrichments": []map[string]any{{
				"name":       "document_chunks_v1",
				"kind":       "chunk",
				"field":      "body",
				"chunk_size": 256,
			}},
		}),
	})
	if err != nil {
		t.Fatalf("marshal chunk index envelope: %v", err)
	}
	if err := db.AddIndexJSON(chunkIndex); err != nil {
		t.Fatalf("add chunk-producing full_text index: %v", err)
	}

	// Consumer: docsaf's exact two-stage NewArtifactEmbeddingIndexConfig
	// shape -- "sources" naming a generated embedding artifact, whose
	// producing "embedding" enrichment is nested in this index's own config
	// and references the chunk artifact above.
	vectorIndex, err := json.Marshal(map[string]any{
		"name": "document_vectors",
		"kind": "dense_vector",
		"config_json": mustMarshalJSONString(t, map[string]any{
			"type":      "embeddings",
			"sources":   []map[string]any{{"artifact": "document_chunk_dense_v1"}},
			"dimension": dims,
			"embedder": map[string]any{
				"provider": "antfly",
				"model":    "fake-embedder",
				"api_url":  server.URL,
			},
			"distance_metric": "cosine",
			"enrichments": []map[string]any{{
				"name": "document_chunk_dense_v1",
				"kind": "embedding",
				// The chunk producer stores chunked content under the same
				// field name its `chunk` enrichment read from ("body" here,
				// not a fixed "text" key), so the consuming `embedding`
				// enrichment's `field` must match it.
				"field":                "body",
				"source_artifact_name": "document_chunks_v1",
				"expected_dims":        dims,
			}},
		}),
	})
	if err != nil {
		t.Fatalf("marshal vector index envelope: %v", err)
	}
	if err := db.AddIndexJSON(vectorIndex); err != nil {
		t.Fatalf("add artifact-sourced dense_vector index: %v", err)
	}

	enrichments, err := db.EnrichmentsJSON()
	if err != nil {
		t.Fatalf("enrichments json: %v", err)
	}
	if !bytes.Contains(enrichments, []byte("document_chunks_v1")) || !bytes.Contains(enrichments, []byte("document_chunk_dense_v1")) {
		t.Fatalf("enrichments %q missing the chunk/embedding producers nested in the index configs", enrichments)
	}

	const bodyA = "alpha beta gamma antfly chunk pipeline testing text"
	const bodyB = "a totally different unrelated sentence about databases"
	if err := db.Batch([]WriteIntent{
		{Key: "doc:a", Value: []byte(fmt.Sprintf(`{"title":"a","body":%q}`, bodyA))},
		{Key: "doc:b", Value: []byte(fmt.Sprintf(`{"title":"b","body":%q}`, bodyB))},
	}, 2); err != nil {
		t.Fatalf("batch write documents: %v", err)
	}

	pending, err := db.RunUntilIdleStatus()
	if err != nil {
		t.Fatalf("run until idle: %v", err)
	}
	var enrichment enrichmentPendingWorkStatus
	if err := json.Unmarshal(pending.Enrichment, &enrichment); err != nil {
		t.Fatalf("decode enrichment pending work: %v; raw=%s", err, pending.Enrichment)
	}
	if enrichment.ErrorCount != 0 || enrichment.FatalErrorCount != 0 || enrichment.Stalled ||
		enrichment.TargetSequence != enrichment.AppliedSequence {
		t.Fatalf("chunk+embedding artifact pipeline did not drain cleanly: %#v", enrichment)
	}
	if atomic.LoadInt32(embedCalls) == 0 {
		t.Fatalf("fake inference server received no /ai/v1/embed requests; the embedding artifact enrichment never ran")
	}

	// The chunk artifact is independently queryable through its own
	// full_text index, and hierarchy projection resolves the chunk hit back
	// to its parent document.
	chunkQuery, err := json.Marshal(map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": "gamma"}},
		"full_text_index":  "document_text_chunks",
		"hierarchy":        map[string]any{"return_level": "chunk"},
		"limit":            5,
	})
	if err != nil {
		t.Fatalf("marshal chunk-artifact query: %v", err)
	}
	chunkResult, err := db.SearchJSON(chunkQuery)
	if err != nil {
		t.Fatalf("chunk-artifact full_text query: %v result=%s", err, chunkResult)
	}
	assertHierarchyParentDocKey(t, chunkResult, "doc:a")

	// A hybrid full-text + semantic query against the artifact-sourced dense
	// index resolves to the same parent document, proving the generated
	// chunk's vector was actually published (an index that silently
	// received no vectors would return zero hits here, not a wrong one --
	// the chunk full_text index above vets that document_chunks_v1 exists).
	hybridQuery, err := json.Marshal(map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": "gamma"}},
		"full_text_index":  "document_text_chunks",
		"semantic_search":  bodyA,
		"indexes":          []string{"document_vectors"},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"hierarchy":        map[string]any{"return_level": "chunk"},
		"limit":            5,
	})
	if err != nil {
		t.Fatalf("marshal hybrid query: %v", err)
	}
	hybridResult, err := db.SearchJSON(hybridQuery)
	if err != nil {
		t.Fatalf("hybrid full-text + semantic query: %v result=%s", err, hybridResult)
	}
	assertHierarchyParentDocKey(t, hybridResult, "doc:a")
}

// mustMarshalJSONString marshals v to JSON and returns it as a string, for
// building the nested `config_json` string field AddIndexJSON expects.
func mustMarshalJSONString(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal nested config_json: %v", err)
	}
	return string(data)
}

// assertHierarchyParentDocKey fails the test unless result contains at least
// one hit whose "hierarchy" object's "parent_doc_key" equals wantParentKey.
func assertHierarchyParentDocKey(t *testing.T, result []byte, wantParentKey string) {
	t.Helper()
	type hit struct {
		ID        string `json:"_id"`
		Hierarchy struct {
			ParentDocKey string `json:"parent_doc_key"`
		} `json:"hierarchy"`
	}
	var parsed struct {
		Responses []struct {
			Hits struct {
				Hits []hit `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("decode search result: %v; raw=%s", err, result)
	}
	for _, response := range parsed.Responses {
		for _, h := range response.Hits.Hits {
			if h.Hierarchy.ParentDocKey == wantParentKey {
				return
			}
		}
	}
	t.Fatalf("no hit with hierarchy.parent_doc_key=%q in result: %s", wantParentKey, result)
}

// TestLiteNativeDogfoodShapedChunkVectorsAndKnowledgeGraphConverge is a
// regression test for the dogfood in-process ingest livelock: a dense_vector
// index that consumes a two-stage chunk-then-embed pipeline entirely through
// its own nested `enrichments` (a `chunk` producer plus an `embedding`
// producer referenced by the dense index's plural `sources` config, exactly
// examples/dogfood/index_config.go's `chunk_vectors` shape) alongside a
// `knowledge` graph index fed by an extractor asset producer never converged:
// `RunUntilIdleStatus` spun forever because the durable dense-artifact target
// counter never advanced (see storage/db/enrichment/enrichment_runtime.zig's
// denseArtifactTargetsForArtifact, which matched only the singular
// `embedding_name` and not the plural `embedding_names` populated by
// `sources`). This exercises that exact combination end to end through fake
// `/ai/v1/embed` and `/ai/v1/extract` servers (no real model) across enough
// documents (50) to make a livelock-vs-slow-drain distinction meaningful, and
// asserts both a semantic hit and graph edges are actually present once
// RunUntilIdleStatus returns.
func TestLiteNativeDogfoodShapedChunkVectorsAndKnowledgeGraphConverge(t *testing.T) {
	const dims = 4
	const docCount = 50

	embedServer, embedCalls := newFakeAntflyEmbedServer(t, dims)
	extractServer := newFakeAntflyExtractServer(t)

	path := filepath.Join(t.TempDir(), "dogfood-shaped-chunk-vectors-and-knowledge.aflite")
	db, err := CreateWithOptions(path, OpenOptions{
		Mode:                     OpenModeWriter,
		Profile:                  ProfileNative,
		RemoteProviderConfigured: true,
	})
	if err != nil {
		t.Fatalf("create native remote-provider Lite database: %v", err)
	}
	defer db.Close()

	// dogfood's `chunk_vectors`: one dense_vector index nesting both the
	// `chunk` producer ("doc_chunks_v1") and the `embedding` producer
	// ("doc_chunk_dense_v1") in its own config, consuming the embedding
	// artifact through the plural `sources` array -- the exact shape that
	// hit the missing-match bug (a singular `embedding_name` config never
	// did).
	chunkVectorsIndex, err := json.Marshal(map[string]any{
		"name": "chunk_vectors",
		"kind": "dense_vector",
		"config_json": mustMarshalJSONString(t, map[string]any{
			"type":            "embeddings",
			"sources":         []map[string]any{{"artifact": "doc_chunk_dense_v1"}},
			"dimension":       dims,
			"distance_metric": "cosine",
			"embedder": map[string]any{
				"provider": "antfly",
				"model":    "fake-embedder",
				"api_url":  embedServer.URL,
			},
			"enrichments": []map[string]any{
				{
					"name":       "doc_chunks_v1",
					"kind":       "chunk",
					"field":      "body",
					"chunk_size": 256,
				},
				{
					"name":                 "doc_chunk_dense_v1",
					"kind":                 "embedding",
					"field":                "body",
					"source_artifact_name": "doc_chunks_v1",
					"expected_dims":        dims,
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("marshal chunk_vectors index envelope: %v", err)
	}
	if err := db.AddIndexJSON(chunkVectorsIndex); err != nil {
		t.Fatalf("add chunk_vectors dense_vector index: %v", err)
	}

	// dogfood's `knowledge`: a graph index fed by a `relations_v1` extractor
	// asset producer.
	const graphIndexName = "knowledge"
	const graphArtifactName = "relations_v1"
	knowledgeIndex, err := knowledgeGraphIndexJSONForTest(graphIndexName, graphArtifactName, extractServer.URL)
	if err != nil {
		t.Fatalf("build knowledge graph index config: %v", err)
	}
	if err := db.AddIndexJSON(knowledgeIndex); err != nil {
		t.Fatalf("add knowledge graph index: %v", err)
	}

	const targetBody = "the antfly VOPR harness fences strong reads during a leadership transition"
	writes := make([]WriteIntent, docCount)
	targetKey := fmt.Sprintf("doc:%03d", docCount/2)
	for i := 0; i < docCount; i++ {
		key := fmt.Sprintf("doc:%03d", i)
		body := targetBody
		if key != targetKey {
			body = fmt.Sprintf("unrelated filler document number %d about databases and storage engines", i)
		}
		writes[i] = WriteIntent{
			Key:   key,
			Value: []byte(fmt.Sprintf(`{"title":"doc %d","body":%q}`, i, body)),
		}
	}
	if err := db.Batch(writes, 2 /* sync_level: full_index */); err != nil {
		t.Fatalf("batch write %d documents: %v", docCount, err)
	}

	pending, err := db.RunUntilIdleStatus()
	if err != nil {
		t.Fatalf("run until idle: %v", err)
	}
	var enrichment enrichmentPendingWorkStatus
	if err := json.Unmarshal(pending.Enrichment, &enrichment); err != nil {
		t.Fatalf("decode enrichment pending work: %v; raw=%s", err, pending.Enrichment)
	}
	if enrichment.ErrorCount != 0 || enrichment.FatalErrorCount != 0 || enrichment.Stalled ||
		enrichment.TargetSequence != enrichment.AppliedSequence {
		t.Fatalf("chunk_vectors + knowledge did not drain cleanly: %#v", enrichment)
	}
	if atomic.LoadInt32(embedCalls) == 0 {
		t.Fatalf("fake inference server received no /ai/v1/embed requests; the embedding artifact enrichment never ran")
	}

	// A semantic hit against chunk_vectors resolves to the target document,
	// proving the two-stage chunk-then-embed pipeline actually published a
	// searchable vector (a silently-empty index returns zero hits here, not
	// a wrong one).
	semanticQuery, err := json.Marshal(map[string]any{
		"semantic_search": targetBody,
		"indexes":         []string{"chunk_vectors"},
		"hierarchy":       map[string]any{"return_level": "chunk"},
		"limit":           5,
	})
	if err != nil {
		t.Fatalf("marshal semantic query: %v", err)
	}
	semanticResult, err := db.SearchJSON(semanticQuery)
	if err != nil {
		t.Fatalf("semantic query: %v result=%s", err, semanticResult)
	}
	assertHierarchyParentDocKey(t, semanticResult, targetKey)

	// The knowledge graph index produced edges from every document's
	// extracted relation.
	edges, err := db.EdgesJSON(graphIndexName, targetKey, "", 2 /* both */)
	if err != nil {
		t.Fatalf("edges json: %v", err)
	}
	if !bytes.Contains(edges, []byte("depends_on")) {
		t.Fatalf("edges JSON %q did not contain the extracted depends_on edge for %s", edges, targetKey)
	}
}

// Exercise non-empty edge results through the C ABI without requiring an
// inference provider. Repeated reads also check that freeing one result does
// not corrupt the heap used by subsequent calls.
func TestLiteNativeGraphEdgesDoNotDoubleFree(t *testing.T) {
	db, err := CreateWithOptions(filepath.Join(t.TempDir(), "graph-edges.aflite"), OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
	})
	if err != nil {
		t.Fatalf("create native database: %v", err)
	}
	defer db.Close()
	if err := db.AddIndexJSON([]byte(`{"name":"graph","kind":"graph","config_json":"{}"}`)); err != nil {
		t.Fatalf("add graph index: %v", err)
	}
	empty, err := db.EdgesJSON("graph", "doc:source", "", 2)
	if err != nil {
		t.Fatalf("read empty edges: %v", err)
	}
	if bytes.Contains(empty, []byte("edge_type")) {
		t.Fatalf("unexpected empty edges: %s", empty)
	}
	if err := db.Batch([]WriteIntent{
		{Key: "doc:source", Value: []byte(`{"title":"source","_edges":{"graph":{"links":[{"target":"doc:target","weight":1.0}]}}}`)},
		{Key: "doc:target", Value: []byte(`{"title":"target"}`)},
	}, 2); err != nil {
		t.Fatalf("batch documents: %v", err)
	}
	if _, err := db.RunUntilIdleStatus(); err != nil {
		t.Fatalf("drain graph indexing: %v", err)
	}
	for i := 0; i < 10; i++ {
		edges, err := db.EdgesJSON("graph", "doc:source", "", 2)
		if err != nil {
			t.Fatalf("read edges %d: %v", i, err)
		}
		if !bytes.Contains(edges, []byte(`"edge_type":"links"`)) {
			t.Fatalf("read edges %d returned no links edge: %s", i, edges)
		}
	}
}
