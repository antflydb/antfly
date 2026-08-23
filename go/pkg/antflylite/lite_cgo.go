// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo

package antflylite

/*
#cgo CFLAGS: -I${SRCDIR}/include -I${SRCDIR}/../../../zig/pkg/antfly/include
#cgo LDFLAGS: -L${SRCDIR}/../../../zig/zig-out/lib -lantfly
#cgo darwin LDFLAGS: -Wl,-rpath,${SRCDIR}/../../../zig/zig-out/lib
#cgo linux LDFLAGS: -Wl,-rpath,${SRCDIR}/../../../zig/zig-out/lib
#include "antfly.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"strings"
	"unsafe"
)

// SupportedABIVersion is the Antfly C ABI version this binding expects.
const SupportedABIVersion uint32 = 1

// OpenMode controls how an Antfly Lite file is opened.
type OpenMode uint32

const (
	OpenModeWriter     OpenMode = C.ANTFLY_LITE_OPEN_MODE_WRITER
	OpenModeReadonly   OpenMode = C.ANTFLY_LITE_OPEN_MODE_READONLY
	OpenModeStatusOnly OpenMode = C.ANTFLY_LITE_OPEN_MODE_STATUS_ONLY
)

// Profile selects the Lite runtime profile.
type Profile uint32

const (
	ProfileNative Profile = C.ANTFLY_LITE_PROFILE_NATIVE
	ProfileHosted Profile = C.ANTFLY_LITE_PROFILE_HOSTED
)

// Inference mode strings returned by Lite status and capabilities.
const (
	InferenceModeCallerSuppliedOrDisabled = C.ANTFLY_LITE_INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED
	InferenceModeCallerSuppliedArtifacts  = C.ANTFLY_LITE_INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS
	InferenceModeRemoteProvider           = C.ANTFLY_LITE_INFERENCE_MODE_REMOTE_PROVIDER
	InferenceModeLocalEmbedded            = C.ANTFLY_LITE_INFERENCE_MODE_LOCAL_EMBEDDED
	InferenceModeManualMaintenance        = C.ANTFLY_LITE_INFERENCE_MODE_MANUAL_MAINTENANCE
	InferenceModeDisabledDeferred         = C.ANTFLY_LITE_INFERENCE_MODE_DISABLED_DEFERRED
)

// OpenOptions configures OpenWithOptions and CreateWithOptions.
type OpenOptions struct {
	Mode                      OpenMode
	Profile                   Profile
	NoSync                    bool
	RemoteProviderConfigured  bool
	LocalRuntimeConfigured    bool
	GeneratedEnrichmentReplay bool
	MapSize                   uint64
	TTLCleanup                *TTLCleanupOptions
}

// TTLCleanupOptions configures the optional Lite TTL cleanup runtime.
type TTLCleanupOptions struct {
	Enabled       bool
	LeaseOwned    bool
	OwnerID       string
	LeaseTTLMS    uint64
	IntervalMS    uint64
	BatchSize     uint32
	GracePeriodNS uint64
}

// WriteIntent is a single key/value write or delete in a Lite batch.
type WriteIntent struct {
	Key    string
	Value  []byte
	Delete bool
}

// DB is an embedded Antfly Lite database handle.
type DB struct {
	handle unsafe.Pointer
}

// ABIVersion returns the loaded Antfly C ABI version.
func ABIVersion() uint32 {
	return uint32(C.antfly_abi_version())
}

// OpenOptionsSize returns the loaded C ABI size of antfly_lite_open_options.
func OpenOptionsSize() uint32 {
	return uint32(C.antfly_lite_open_options_size())
}

func compiledOpenOptionsSize() uint32 {
	return uint32(C.sizeof_antfly_lite_open_options)
}

// ValidateABI verifies that the loaded C library matches the header used to
// compile this Go binding.
func ValidateABI() error {
	if got := ABIVersion(); got != SupportedABIVersion {
		return fmt.Errorf("antflylite: unsupported C ABI version %d, want %d", got, SupportedABIVersion)
	}
	if got, want := OpenOptionsSize(), compiledOpenOptionsSize(); got != want {
		return fmt.Errorf("antflylite: C ABI open options size %d, compiled header size %d", got, want)
	}
	return nil
}

func cABIErrorCodeName(code ErrorCode) string {
	return C.GoString(C.antfly_error_code_name(C.antfly_error_code(code)))
}

func cABIErrorCodeDescription(code ErrorCode) string {
	return C.GoString(C.antfly_error_code_description(C.antfly_error_code(code)))
}

// Open opens an existing native Antfly Lite database file for writing.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
	})
}

// Create creates a new native Antfly Lite database file for writing.
func Create(path string) (*DB, error) {
	return CreateWithOptions(path, OpenOptions{
		Mode:    OpenModeWriter,
		Profile: ProfileNative,
	})
}

// OpenReadonly opens an existing Antfly Lite database file read-only.
func OpenReadonly(path string) (*DB, error) {
	return OpenWithOptions(path, OpenOptions{
		Mode:    OpenModeReadonly,
		Profile: ProfileNative,
	})
}

// OpenStatusOnly opens enough of an Antfly Lite database to read status.
func OpenStatusOnly(path string) (*DB, error) {
	return OpenWithOptions(path, OpenOptions{
		Mode:    OpenModeStatusOnly,
		Profile: ProfileNative,
	})
}

// OpenHosted opens an existing Antfly Lite database in hosted/manual
// maintenance mode. In this profile callers drive pending work explicitly with
// RunUntilIdle.
func OpenHosted(path string) (*DB, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var handle unsafe.Pointer
	if err := check(C.antfly_lite_open_hosted(cPath, &handle)); err != nil {
		return nil, err
	}
	return newDB(handle), nil
}

// CreateHosted creates a new Antfly Lite database in hosted/manual maintenance
// mode. In this profile callers drive pending work explicitly with RunUntilIdle.
func CreateHosted(path string) (*DB, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var handle unsafe.Pointer
	if err := check(C.antfly_lite_create_hosted(cPath, &handle)); err != nil {
		return nil, err
	}
	return newDB(handle), nil
}

// OpenWithOptions opens an Antfly Lite database using explicit C ABI options.
func OpenWithOptions(path string, opts OpenOptions) (*DB, error) {
	return openWithOptions(path, opts, false)
}

// CreateWithOptions creates an Antfly Lite database using explicit C ABI options.
func CreateWithOptions(path string, opts OpenOptions) (*DB, error) {
	return openWithOptions(path, opts, true)
}

func openWithOptions(path string, opts OpenOptions, create bool) (*DB, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cOpts C.antfly_lite_open_options
	if err := check(C.antfly_lite_open_options_init(&cOpts)); err != nil {
		return nil, err
	}
	cOpts.open_mode = C.uint32_t(opts.Mode)
	cOpts.profile = C.uint32_t(opts.Profile)
	cOpts.map_size = C.uint64_t(opts.MapSize)
	if opts.NoSync {
		cOpts.flags |= C.ANTFLY_LITE_OPEN_FLAG_NO_SYNC
	}
	if opts.RemoteProviderConfigured {
		cOpts.flags |= C.ANTFLY_LITE_OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED
	}
	if opts.LocalRuntimeConfigured {
		cOpts.flags |= C.ANTFLY_LITE_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED
	}
	if opts.GeneratedEnrichmentReplay {
		cOpts.flags |= C.ANTFLY_LITE_OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY
	}
	if opts.TTLCleanup != nil {
		cOpts.flags |= C.ANTFLY_LITE_OPEN_FLAG_TTL_CLEANUP
		cOpts.ttl_cleanup_enabled = C.bool(opts.TTLCleanup.Enabled)
		cOpts.ttl_cleanup_lease_owned = C.bool(opts.TTLCleanup.LeaseOwned)
		cOpts.ttl_cleanup_lease_ttl_ms = C.uint64_t(opts.TTLCleanup.LeaseTTLMS)
		cOpts.ttl_cleanup_interval_ms = C.uint64_t(opts.TTLCleanup.IntervalMS)
		cOpts.ttl_cleanup_batch_size = C.uint32_t(opts.TTLCleanup.BatchSize)
		cOpts.ttl_cleanup_grace_period_ns = C.uint64_t(opts.TTLCleanup.GracePeriodNS)
		ownerID, cleanupOwnerID := makeCStringSlice([]byte(opts.TTLCleanup.OwnerID))
		defer cleanupOwnerID()
		cOpts.ttl_cleanup_owner_id = ownerID
	}

	var handle unsafe.Pointer
	var code C.antfly_error_code
	if create {
		code = C.antfly_lite_create_with_options(cPath, &cOpts, &handle)
	} else {
		code = C.antfly_lite_open_with_options(cPath, &cOpts, &handle)
	}
	if err := check(code); err != nil {
		return nil, err
	}
	return newDB(handle), nil
}

func newDB(handle unsafe.Pointer) *DB {
	db := &DB{handle: handle}
	runtime.SetFinalizer(db, (*DB).closeFinalizer)
	return db
}

func (db *DB) closeFinalizer() {
	_ = db.Close()
}

// Close releases the embedded database handle. It is safe to call more than
// once.
func (db *DB) Close() error {
	if db == nil || db.handle == nil {
		return nil
	}
	handle := db.handle
	db.handle = nil
	runtime.SetFinalizer(db, nil)
	C.antfly_db_close(handle)
	return nil
}

func (db *DB) requireHandle() (unsafe.Pointer, error) {
	if db == nil || db.handle == nil {
		return nil, InvalidArgument
	}
	return db.handle, nil
}

// StatusJSON returns a JSON status document for the Lite database.
func (db *DB) StatusJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_status_json(handle, out)
	})
}

// CapabilitiesJSON returns the database's Lite capability document.
func (db *DB) CapabilitiesJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_capabilities_json(handle, out)
	})
}

// ReplayGeneratedEnrichmentsJSON replays generated enrichment work from stored
// documents and returns {"replayed":N}.
func (db *DB) ReplayGeneratedEnrichmentsJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_replay_generated_enrichments_json(handle, out)
	})
}

// Backup returns a portable Antfly backup archive for this Lite database.
func (db *DB) Backup() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_backup(handle, out)
	})
}

// Export returns a portable Antfly backup archive for this Lite database.
func (db *DB) Export() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_export(handle, out)
	})
}

// ImportBackup imports a portable Antfly backup archive into this Lite
// database. OutcomeUnknown means the live handle adopted the imported
// generation, but crash durability could not be confirmed; inspect the handle
// and do not retry automatically.
func (db *DB) ImportBackup(backup []byte) error {
	return db.withInput(backup, func(handle unsafe.Pointer, input C.antfly_slice) C.antfly_error_code {
		return C.antfly_lite_import_backup(handle, input)
	})
}

// Import imports a portable Antfly backup archive into this Lite database.
// OutcomeUnknown means the live handle adopted the imported generation, but
// crash durability could not be confirmed; inspect the handle and do not retry
// automatically.
func (db *DB) Import(backup []byte) error {
	return db.withInput(backup, func(handle unsafe.Pointer, input C.antfly_slice) C.antfly_error_code {
		return C.antfly_lite_import(handle, input)
	})
}

func restoreBackupToFile(path string, backup []byte, replace bool) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	input, cleanup := makeCStringSlice(backup)
	defer cleanup()

	var out C.antfly_buffer
	if err := check(C.antfly_lite_restore_backup_json(cPath, input, C.bool(replace), &out)); err != nil {
		return err
	}
	C.antfly_buffer_free(&out)
	return nil
}

func restoreBackupFileToFile(path, backupPath string, replace bool) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cBackupPath := C.CString(backupPath)
	defer C.free(unsafe.Pointer(cBackupPath))

	var out C.antfly_buffer
	if err := check(C.antfly_lite_restore_backup_file_json(cPath, cBackupPath, C.bool(replace), &out)); err != nil {
		return err
	}
	C.antfly_buffer_free(&out)
	return nil
}

func restoreToFile(path string, backup []byte, replace bool) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	input, cleanup := makeCStringSlice(backup)
	defer cleanup()

	var out C.antfly_buffer
	if err := check(C.antfly_lite_restore_json(cPath, input, C.bool(replace), &out)); err != nil {
		return err
	}
	C.antfly_buffer_free(&out)
	return nil
}

// CheckJSON runs Lite integrity checks and returns the JSON result.
func (db *DB) CheckJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_check_json(handle, out)
	})
}

// CheckFileJSON runs Lite integrity checks for path without opening a database
// handle and returns the JSON result.
func CheckFileJSON(path string) ([]byte, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var out C.antfly_buffer
	if err := check(C.antfly_lite_check_file_json(cPath, &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

// VacuumJSON compacts free space and returns the JSON result.
func (db *DB) VacuumJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_vacuum_json(handle, out)
	})
}

// CompactJSON drains maintenance, compacts indexes, vacuums free space, and
// returns the JSON result.
func (db *DB) CompactJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_compact_json(handle, out)
	})
}

// CopyStableSnapshotJSON copies a stable Lite snapshot to destPath.
func (db *DB) CopyStableSnapshotJSON(destPath string, replace bool) ([]byte, error) {
	if !strings.HasSuffix(destPath, ".aflite") {
		return nil, InvalidArgument
	}
	handle, err := db.requireHandle()
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(db)
	cDest := C.CString(destPath)
	defer C.free(unsafe.Pointer(cDest))

	var out C.antfly_buffer
	if err := check(C.antfly_lite_copy_stable_snapshot_json(handle, cDest, C.bool(replace), &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

// CopyStableSnapshotFileJSON opens srcPath read-only, copies a stable Lite
// snapshot to destPath, and returns the JSON result.
func CopyStableSnapshotFileJSON(srcPath, destPath string, replace bool) ([]byte, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}
	cSrc := C.CString(srcPath)
	defer C.free(unsafe.Pointer(cSrc))
	cDest := C.CString(destPath)
	defer C.free(unsafe.Pointer(cDest))

	var out C.antfly_buffer
	if err := check(C.antfly_lite_copy_stable_snapshot_file_json(cSrc, cDest, C.bool(replace), &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

// Batch applies write intents at timestampNS.
func (db *DB) Batch(writes []WriteIntent, timestampNS uint64) error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	cWrites, cleanup, err := makeCWriteIntents(writes)
	if err != nil {
		return err
	}
	defer cleanup()

	return check(C.antfly_db_batch(
		handle,
		cWrites,
		C.size_t(len(writes)),
		(*C.antfly_version_predicate)(nil),
		0,
		C.uint64_t(timestampNS),
		0,
	))
}

// BatchJSON applies a public Antfly batch request and returns the JSON result.
func (db *DB) BatchJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_batch_json(handle, input, out)
	})
}

// LookupJSON returns the JSON lookup result for key.
func (db *DB) LookupJSON(key string) ([]byte, error) {
	return db.withStringInputOutput(key, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_lookup_json(handle, input, out)
	})
}

// Raw returns the raw stored bytes for key.
func (db *DB) Raw(key string) ([]byte, error) {
	return db.withStringInputOutput(key, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_get_raw(handle, input, out)
	})
}

// SchemaJSON returns the active schema JSON.
func (db *DB) SchemaJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_get_schema_json(handle, out)
	})
}

// SetSchemaJSON replaces the active schema JSON.
func (db *DB) SetSchemaJSON(schema []byte) error {
	return db.withInput(schema, func(handle unsafe.Pointer, input C.antfly_slice) C.antfly_error_code {
		return C.antfly_db_set_schema_json(handle, input)
	})
}

// RunUntilIdle drains pending enrichment and index work.
func (db *DB) RunUntilIdle() error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	return check(C.antfly_lite_run_until_idle(handle))
}

// RunUntilIdleJSON drains pending enrichment and index work and returns the
// post-drain pending work stats JSON.
func (db *DB) RunUntilIdleJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_run_until_idle_json(handle, out)
	})
}

// PendingWorkStatsJSON returns pending index/enrichment work as JSON.
func (db *DB) PendingWorkStatsJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_lite_pending_work_stats_json(handle, out)
	})
}

// IndexesJSON returns configured indexes as JSON.
func (db *DB) IndexesJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_list_indexes_json(handle, out)
	})
}

// AddIndexJSON adds an index from its JSON configuration.
func (db *DB) AddIndexJSON(config []byte) error {
	return db.withInput(config, func(handle unsafe.Pointer, input C.antfly_slice) C.antfly_error_code {
		return C.antfly_db_add_index_json(handle, input)
	})
}

// DeleteIndex deletes an index by name and reports whether it existed.
func (db *DB) DeleteIndex(name string) (bool, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return false, err
	}
	defer runtime.KeepAlive(db)
	input, cleanup := makeCStringSlice([]byte(name))
	defer cleanup()

	var deleted C.bool
	if err := check(C.antfly_db_delete_index(handle, input, &deleted)); err != nil {
		return false, err
	}
	return bool(deleted), nil
}

// EnrichmentsJSON returns configured enrichments as JSON.
func (db *DB) EnrichmentsJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_list_enrichments_json(handle, out)
	})
}

// AddEnrichmentJSON adds an enrichment from its JSON configuration.
func (db *DB) AddEnrichmentJSON(config []byte) error {
	return db.withInput(config, func(handle unsafe.Pointer, input C.antfly_slice) C.antfly_error_code {
		return C.antfly_db_add_enrichment_json(handle, input)
	})
}

// DeleteEnrichment deletes an enrichment by kind and name and reports whether
// it existed.
func (db *DB) DeleteEnrichment(kind, name string) (bool, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return false, err
	}
	defer runtime.KeepAlive(db)
	cKind, cleanupKind := makeCStringSlice([]byte(kind))
	defer cleanupKind()
	cName, cleanupName := makeCStringSlice([]byte(name))
	defer cleanupName()

	var deleted C.bool
	if err := check(C.antfly_db_delete_enrichment(handle, cKind, cName, &deleted)); err != nil {
		return false, err
	}
	return bool(deleted), nil
}

// ScanJSON executes a JSON scan request and returns the JSON result.
func (db *DB) ScanJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_scan_json(handle, input, out)
	})
}

// StatsJSON returns database stats as JSON.
func (db *DB) StatsJSON() ([]byte, error) {
	return db.readBuffer(func(handle unsafe.Pointer, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_stats_json(handle, out)
	})
}

// SearchJSON executes a JSON search request and returns the JSON result.
func (db *DB) SearchJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_search_json(handle, input, out)
	})
}

// DenseSearchWire executes a packed dense-vector wire search request.
func (db *DB) DenseSearchWire(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_search_dense_wire(handle, input, out)
	})
}

// TextMatchWire executes a packed text-match wire search request.
func (db *DB) TextMatchWire(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_search_text_match_wire(handle, input, out)
	})
}

// TextTermWire executes a packed text-term wire search request.
func (db *DB) TextTermWire(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_search_text_term_wire(handle, input, out)
	})
}

// TextMatchPhraseWire executes a packed text-match-phrase wire search request.
func (db *DB) TextMatchPhraseWire(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_search_text_match_phrase_wire(handle, input, out)
	})
}

// AggregateHitsJSON aggregates hits from a JSON request.
func (db *DB) AggregateHitsJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_aggregate_hits_json(handle, input, out)
	})
}

// LookupArtifactJSON looks up an artifact by base64 artifact ID.
func (db *DB) LookupArtifactJSON(artifactIDBase64 string) ([]byte, error) {
	return db.withStringInputOutput(artifactIDBase64, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_lookup_artifact_json(handle, input, out)
	})
}

// DecodeArtifactIDJSON decodes a base64 artifact ID without opening a database.
func DecodeArtifactIDJSON(artifactIDBase64 string) ([]byte, error) {
	input, cleanup := makeCStringSlice([]byte(artifactIDBase64))
	defer cleanup()

	var out C.antfly_buffer
	if err := check(C.antfly_db_decode_artifact_id_json(input, &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

// ExtractEnrichmentsJSON extracts enrichment outputs for a JSON request.
func (db *DB) ExtractEnrichmentsJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_extract_enrichments_json(handle, input, out)
	})
}

// ComputeEnrichmentsJSON computes enrichment outputs for a JSON request.
func (db *DB) ComputeEnrichmentsJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_compute_enrichments_json(handle, input, out)
	})
}

// EdgesJSON returns graph edges for a key from a graph index.
func (db *DB) EdgesJSON(indexName, key, edgeType string, direction uint8) ([]byte, error) {
	return db.graphLookup(indexName, key, edgeType, direction, func(handle unsafe.Pointer, cIndex, cKey, cEdgeType C.antfly_slice, cDirection C.uint8_t, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_get_edges_json(handle, cIndex, cKey, cEdgeType, cDirection, out)
	})
}

// TraverseEdgesJSON executes a JSON graph traversal request.
func (db *DB) TraverseEdgesJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_traverse_edges_json(handle, input, out)
	})
}

// ExecuteGraphQueriesJSON executes a batch of JSON graph queries.
func (db *DB) ExecuteGraphQueriesJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_execute_graph_queries_json(handle, input, out)
	})
}

// NeighborsJSON returns graph neighbors for a key from a graph index.
func (db *DB) NeighborsJSON(indexName, key, edgeType string, direction uint8) ([]byte, error) {
	return db.graphLookup(indexName, key, edgeType, direction, func(handle unsafe.Pointer, cIndex, cKey, cEdgeType C.antfly_slice, cDirection C.uint8_t, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_get_neighbors_json(handle, cIndex, cKey, cEdgeType, cDirection, out)
	})
}

// FindShortestPathJSON executes a JSON shortest-path graph request.
func (db *DB) FindShortestPathJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_find_shortest_path_json(handle, input, out)
	})
}

// FindKShortestPathsJSON executes a JSON k-shortest-paths graph request.
func (db *DB) FindKShortestPathsJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_find_k_shortest_paths_json(handle, input, out)
	})
}

// MatchPatternJSON executes a JSON graph pattern-match request.
func (db *DB) MatchPatternJSON(request []byte) ([]byte, error) {
	return db.withInputOutput(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_db_match_pattern_json(handle, input, out)
	})
}

func (db *DB) readBuffer(fn func(unsafe.Pointer, *C.antfly_buffer) C.antfly_error_code) ([]byte, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(db)
	var out C.antfly_buffer
	if err := check(fn(handle, &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

func (db *DB) withInput(input []byte, fn func(unsafe.Pointer, C.antfly_slice) C.antfly_error_code) error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	cInput, cleanup := makeCStringSlice(input)
	defer cleanup()
	return check(fn(handle, cInput))
}

func (db *DB) withInputOutput(input []byte, fn func(unsafe.Pointer, C.antfly_slice, *C.antfly_buffer) C.antfly_error_code) ([]byte, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(db)
	cInput, cleanup := makeCStringSlice(input)
	defer cleanup()

	var out C.antfly_buffer
	if err := check(fn(handle, cInput, &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

func (db *DB) withStringInputOutput(input string, fn func(unsafe.Pointer, C.antfly_slice, *C.antfly_buffer) C.antfly_error_code) ([]byte, error) {
	return db.withInputOutput([]byte(input), fn)
}

func (db *DB) graphLookup(indexName, key, edgeType string, direction uint8, fn func(unsafe.Pointer, C.antfly_slice, C.antfly_slice, C.antfly_slice, C.uint8_t, *C.antfly_buffer) C.antfly_error_code) ([]byte, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(db)
	cIndex, cleanupIndex := makeCStringSlice([]byte(indexName))
	defer cleanupIndex()
	cKey, cleanupKey := makeCStringSlice([]byte(key))
	defer cleanupKey()
	cEdgeType, cleanupEdgeType := makeCStringSlice([]byte(edgeType))
	defer cleanupEdgeType()

	var out C.antfly_buffer
	if err := check(fn(handle, cIndex, cKey, cEdgeType, C.uint8_t(direction), &out)); err != nil {
		return nil, err
	}
	return takeBuffer(out), nil
}

func makeCStringSlice(input []byte) (C.antfly_slice, func()) {
	if len(input) == 0 {
		return C.antfly_slice{}, func() {}
	}
	ptr := C.CBytes(input)
	slice := C.antfly_slice{
		ptr: (*C.uint8_t)(ptr),
		len: C.size_t(len(input)),
	}
	return slice, func() {
		C.free(ptr)
	}
}

func makeCWriteIntents(writes []WriteIntent) (*C.antfly_write_intent, func(), error) {
	if len(writes) == 0 {
		return nil, func() {}, nil
	}
	size := C.size_t(len(writes)) * C.size_t(unsafe.Sizeof(C.antfly_write_intent{}))
	ptr := C.malloc(size)
	if ptr == nil {
		return nil, nil, Internal
	}
	cWrites := unsafe.Slice((*C.antfly_write_intent)(ptr), len(writes))
	cleanups := make([]func(), 0, len(writes)*2)
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
		C.free(ptr)
	}

	for i, write := range writes {
		key, keyCleanup := makeCStringSlice([]byte(write.Key))
		cleanups = append(cleanups, keyCleanup)
		value, valueCleanup := makeCStringSlice(write.Value)
		cleanups = append(cleanups, valueCleanup)
		cWrites[i] = C.antfly_write_intent{
			key:       key,
			value:     value,
			is_delete: C.bool(write.Delete),
		}
	}
	return (*C.antfly_write_intent)(ptr), cleanup, nil
}

func takeBuffer(buffer C.antfly_buffer) []byte {
	defer C.antfly_buffer_free(&buffer)
	if buffer.ptr == nil || buffer.len == 0 {
		return nil
	}
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(buffer.ptr)), int(buffer.len))
	return append([]byte(nil), bytes...)
}

func check(code C.antfly_error_code) error {
	if code == C.ANTFLY_OK {
		return nil
	}
	return ErrorCode(code)
}
