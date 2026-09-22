"""ctypes bindings for the ``libantfly`` C ABI (see ``zig/CAPI.md``).

This module defines the C structures byte-for-byte as declared in
``zig/pkg/antfly/include/antfly.h`` and configures the argument/return types
for every function this binding calls. It intentionally mirrors only the
subset of the C ABI that ``go/pkg/lite`` (the reference binding) exposes.

Struct field order and types must match the header exactly: ctypes applies
the platform's native C struct layout rules (the same rules the Zig compiler
uses for ``extern struct``), so as long as the field order/types agree here,
the in-memory layout agrees too.
"""

from __future__ import annotations

import ctypes
import os

from ._library import LibraryNotFoundError, find_library

__all__ = [
    "LibraryNotFoundError",
    "ABIVersionError",
    "SUPPORTED_ABI_VERSION",
    "THREADING_SERIALIZED",
    "AntflySlice",
    "AntflyBuffer",
    "LiteOpenOptions",
    "AntflyWriteIntent",
    "AntflyVersionPredicate",
    "OPEN_FLAG_NO_SYNC",
    "OPEN_FLAG_TTL_CLEANUP",
    "OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED",
    "OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED",
    "OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY",
    "load_library",
    "get_lib",
    "validate_abi",
    "make_slice",
    "take_buffer",
    "path_to_bytes",
]


class ABIVersionError(RuntimeError):
    """Raised when the loaded libantfly ABI does not match this binding."""


# The Antfly C ABI version this binding was written against
# (antfly_abi_version() in antfly.h).
SUPPORTED_ABI_VERSION = 1

# The only threading contract libantfly implements (ANTFLY_THREADING_SERIALIZED).
THREADING_SERIALIZED = 1

# antfly_open_options / antfly_lite_open_options flag bits.
OPEN_FLAG_NO_SYNC = 1 << 0
OPEN_FLAG_TTL_CLEANUP = 1 << 1
OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED = 1 << 2
OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED = 1 << 3
OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY = 1 << 4

# ANTFLY_LITE_OPEN_MODE_* / ANTFLY_LITE_PROFILE_*
OPEN_MODE_WRITER = 0
OPEN_MODE_READONLY = 1
OPEN_MODE_STATUS_ONLY = 2
PROFILE_NATIVE = 0
PROFILE_HOSTED = 1

# ANTFLY_LITE_INFERENCE_MODE_* string constants.
INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED = "caller_supplied_or_disabled"
INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS = "caller_supplied_artifacts"
INFERENCE_MODE_REMOTE_PROVIDER = "remote_provider"
INFERENCE_MODE_LOCAL_EMBEDDED = "local_embedded"
INFERENCE_MODE_MANUAL_MAINTENANCE = "manual_maintenance"
INFERENCE_MODE_DISABLED_DEFERRED = "disabled_deferred"


class AntflySlice(ctypes.Structure):
    """Borrowed input. The caller owns the memory and must keep it valid for
    the duration of the call (antfly.h: antfly_slice)."""

    _fields_ = [
        ("ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
    ]


class AntflyBuffer(ctypes.Structure):
    """Owned output. On success the caller owns the returned memory and must
    release it with antfly_buffer_free (antfly.h: antfly_buffer)."""

    _fields_ = [
        ("ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
    ]


class LiteOpenOptions(ctypes.Structure):
    """antfly_lite_open_options. Must be initialized with
    antfly_lite_open_options_init before fields are set (see ABI Contract in
    zig/CAPI.md)."""

    _fields_ = [
        ("abi_size", ctypes.c_uint32),
        ("open_mode", ctypes.c_uint32),
        ("profile", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("map_size", ctypes.c_uint64),
        ("ttl_cleanup_enabled", ctypes.c_bool),
        ("ttl_cleanup_lease_owned", ctypes.c_bool),
        ("ttl_cleanup_batch_size", ctypes.c_uint32),
        ("ttl_cleanup_owner_id", AntflySlice),
        ("ttl_cleanup_lease_ttl_ms", ctypes.c_uint64),
        ("ttl_cleanup_interval_ms", ctypes.c_uint64),
        ("ttl_cleanup_grace_period_ns", ctypes.c_uint64),
        ("inference_host_budget_mb", ctypes.c_uint32),
        ("inference_backend_budget_mb", ctypes.c_uint32),
        ("inference_process_memory_budget_mb", ctypes.c_uint32),
        ("inference_combined_budget_mb", ctypes.c_uint32),
        ("inference_kv_budget_mb", ctypes.c_uint32),
        ("inference_scratch_budget_mb", ctypes.c_uint32),
        ("busy_timeout_ms", ctypes.c_uint64),
        ("reserved", ctypes.c_uint64 * 7),
    ]


class AntflyWriteIntent(ctypes.Structure):
    _fields_ = [
        ("key", AntflySlice),
        ("value", AntflySlice),
        ("is_delete", ctypes.c_bool),
    ]


class AntflyVersionPredicate(ctypes.Structure):
    _fields_ = [
        ("key", AntflySlice),
        ("expected_version", ctypes.c_uint64),
    ]


# 16-byte transaction id, matching `const uint8_t (*txn_id)[16]`.
TxnIDArray = ctypes.c_uint8 * 16

_VOID_P = ctypes.c_void_p
_BUF_P = ctypes.POINTER(AntflyBuffer)
_OPTS_P = ctypes.POINTER(LiteOpenOptions)
_TXN_P = ctypes.POINTER(TxnIDArray)
_ERR = ctypes.c_int  # antfly_error_code (C enum, backed by `int`)

# (name, argtypes, restype) for every function this binding calls. This is a
# deliberate subset of antfly.h: it mirrors exactly what go/pkg/lite calls,
# which is itself a considered subset of the full C ABI.
_FUNCTIONS: list[tuple[str, list[object], object]] = [
    ("antfly_abi_version", [], ctypes.c_uint32),
    ("antfly_lite_open_options_size", [], ctypes.c_uint32),
    ("antfly_error_code_name", [_ERR], ctypes.c_char_p),
    ("antfly_error_code_description", [_ERR], ctypes.c_char_p),
    ("antfly_lite_open_options_init", [_OPTS_P], _ERR),
    ("antfly_threading_mode", [], ctypes.c_uint32),
    ("antfly_lite_open_with_options", [ctypes.c_char_p, _OPTS_P, ctypes.POINTER(_VOID_P)], _ERR),
    ("antfly_lite_create_with_options", [ctypes.c_char_p, _OPTS_P, ctypes.POINTER(_VOID_P)], _ERR),
    ("antfly_lite_open_hosted", [ctypes.c_char_p, ctypes.POINTER(_VOID_P)], _ERR),
    ("antfly_lite_create_hosted", [ctypes.c_char_p, ctypes.POINTER(_VOID_P)], _ERR),
    ("antfly_lite_status_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_capabilities_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_replay_generated_enrichments_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_backup", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_export", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_import_backup", [_VOID_P, AntflySlice], _ERR),
    ("antfly_lite_import", [_VOID_P, AntflySlice], _ERR),
    ("antfly_lite_restore_backup_json", [ctypes.c_char_p, AntflySlice, ctypes.c_bool, _BUF_P], _ERR),
    ("antfly_lite_restore_json", [ctypes.c_char_p, AntflySlice, ctypes.c_bool, _BUF_P], _ERR),
    (
        "antfly_lite_restore_backup_file_json",
        [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, _BUF_P],
        _ERR,
    ),
    ("antfly_lite_check_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_check_file_json", [ctypes.c_char_p, _BUF_P], _ERR),
    ("antfly_lite_copy_stable_snapshot_json", [_VOID_P, ctypes.c_char_p, ctypes.c_bool, _BUF_P], _ERR),
    (
        "antfly_lite_copy_stable_snapshot_file_json",
        [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, _BUF_P],
        _ERR,
    ),
    ("antfly_lite_compact_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_vacuum_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_run_until_idle", [_VOID_P], _ERR),
    ("antfly_lite_run_until_idle_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_lite_pending_work_stats_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_db_close", [_VOID_P], None),
    ("antfly_buffer_free", [_BUF_P], None),
    (
        "antfly_db_batch",
        [
            _VOID_P,
            ctypes.POINTER(AntflyWriteIntent),
            ctypes.c_size_t,
            ctypes.POINTER(AntflyVersionPredicate),
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_uint8,
        ],
        _ERR,
    ),
    ("antfly_db_batch_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    (
        "antfly_db_begin_transaction_with_id",
        [_VOID_P, _TXN_P, ctypes.c_uint64, ctypes.POINTER(AntflySlice), ctypes.c_size_t],
        _ERR,
    ),
    (
        "antfly_db_write_transaction",
        [
            _VOID_P,
            _TXN_P,
            ctypes.POINTER(AntflyWriteIntent),
            ctypes.c_size_t,
            ctypes.POINTER(AntflyVersionPredicate),
            ctypes.c_size_t,
        ],
        _ERR,
    ),
    ("antfly_db_resolve_intents", [_VOID_P, _TXN_P, ctypes.c_uint8, ctypes.c_uint64], _ERR),
    ("antfly_db_get_transaction_status", [_VOID_P, _TXN_P, ctypes.POINTER(ctypes.c_uint8)], _ERR),
    ("antfly_db_get_commit_version", [_VOID_P, _TXN_P, ctypes.POINTER(ctypes.c_uint64)], _ERR),
    ("antfly_db_lookup_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_get_raw", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_get_schema_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_db_set_schema_json", [_VOID_P, AntflySlice], _ERR),
    ("antfly_db_list_indexes_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_db_add_index_json", [_VOID_P, AntflySlice], _ERR),
    ("antfly_db_delete_index", [_VOID_P, AntflySlice, ctypes.POINTER(ctypes.c_bool)], _ERR),
    ("antfly_db_list_enrichments_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_db_add_enrichment_json", [_VOID_P, AntflySlice], _ERR),
    (
        "antfly_db_delete_enrichment",
        [_VOID_P, AntflySlice, AntflySlice, ctypes.POINTER(ctypes.c_bool)],
        _ERR,
    ),
    ("antfly_db_scan_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_stats_json", [_VOID_P, _BUF_P], _ERR),
    ("antfly_db_search_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_search_dense_wire", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_search_text_match_wire", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_search_text_term_wire", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_search_text_match_phrase_wire", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_aggregate_hits_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_lookup_artifact_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_decode_artifact_id_json", [AntflySlice, _BUF_P], _ERR),
    ("antfly_db_extract_enrichments_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_compute_enrichments_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    (
        "antfly_db_get_edges_json",
        [_VOID_P, AntflySlice, AntflySlice, AntflySlice, ctypes.c_uint8, _BUF_P],
        _ERR,
    ),
    ("antfly_db_traverse_edges_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_execute_graph_queries_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    (
        "antfly_db_get_neighbors_json",
        [_VOID_P, AntflySlice, AntflySlice, AntflySlice, ctypes.c_uint8, _BUF_P],
        _ERR,
    ),
    ("antfly_db_find_shortest_path_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_find_k_shortest_paths_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
    ("antfly_db_match_pattern_json", [_VOID_P, AntflySlice, _BUF_P], _ERR),
]

_lib: ctypes.CDLL | None = None


def _configure(lib: ctypes.CDLL) -> None:
    for name, argtypes, restype in _FUNCTIONS:
        func = getattr(lib, name)
        func.argtypes = argtypes
        func.restype = restype


def load_library() -> ctypes.CDLL:
    """Locate and load libantfly, caching the result.

    Raises LibraryNotFoundError if no library can be found or it fails to
    load. Safe to call repeatedly and from multiple threads (subsequent calls
    return the cached handle).
    """
    global _lib
    if _lib is not None:
        return _lib
    path = find_library()
    if path is None:
        raise LibraryNotFoundError(
            "libantfly shared library not found. Set ANTFLY_LIBRARY to its "
            "path, ANTFLY_LIB_DIR to its directory, install antfly-cli, or "
            "build it at zig/zig-out/lib. See the antfly-lite README for "
            "the full discovery order."
        )
    try:
        lib = ctypes.CDLL(str(path))
    except OSError as exc:
        raise LibraryNotFoundError(f"failed to load libantfly from {path}: {exc}") from exc
    _configure(lib)
    _lib = lib
    return lib


def get_lib() -> ctypes.CDLL:
    """Return the already-loaded library, loading it on first use."""
    return load_library()


def validate_abi() -> None:
    """Verify the loaded C library matches the header this binding was
    written against (mirrors Go's ValidateABI)."""
    lib = load_library()
    got = lib.antfly_abi_version()
    if got != SUPPORTED_ABI_VERSION:
        raise ABIVersionError(f"lite: unsupported C ABI version {got}, want {SUPPORTED_ABI_VERSION}")
    got_size = lib.antfly_lite_open_options_size()
    want_size = ctypes.sizeof(LiteOpenOptions)
    if got_size != want_size:
        raise ABIVersionError(f"lite: C ABI open options size {got_size}, compiled struct size {want_size}")


def make_slice(data: bytes) -> tuple[AntflySlice, object]:
    """Build an antfly_slice borrowing `data`.

    Returns the slice and a keep-alive object; the caller must keep the
    keep-alive object referenced for the duration of the call (it owns the
    buffer `slice.ptr` points into).
    """
    if not data:
        return AntflySlice(), None
    arr = (ctypes.c_uint8 * len(data)).from_buffer_copy(data)
    slice_ = AntflySlice(ptr=ctypes.cast(arr, ctypes.POINTER(ctypes.c_uint8)), len=len(data))
    return slice_, arr


def take_buffer(buf: AntflyBuffer) -> bytes:
    """Copy an antfly_buffer's contents and free it.

    Only call this after a successful (ANTFLY_OK) call: buffer ownership is
    only transferred to the caller on success (see the ABI Contract in
    zig/CAPI.md).
    """
    try:
        if not buf.ptr or buf.len == 0:
            return b""
        return ctypes.string_at(buf.ptr, buf.len)
    finally:
        get_lib().antfly_buffer_free(ctypes.byref(buf))


def path_to_bytes(path: str | os.PathLike[str]) -> bytes:
    return os.fsencode(os.fspath(path))
