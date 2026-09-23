"""The embedded Antfly Lite database handle.

Threading model (see zig/CAPI.md "Thread Safety" and go/pkg/lite's DB): a
Database is safe for concurrent use by multiple threads. libantfly runs in
serialized threading mode: reads run in parallel, writes on one handle queue
behind each other instead of failing with Busy, and schema/index changes
wait for in-flight calls. close() waits for in-flight calls on other threads
to finish; calls made after close() raise InvalidArgumentError, mirroring
the C ABI's own antfly_db_close contract.

The handle is guarded with a counting condition variable rather than a plain
lock so that concurrent calls actually run concurrently: ctypes releases the
GIL for the duration of a foreign call, and libantfly itself does the real
serialization/queueing internally.
"""

from __future__ import annotations

import ctypes
import enum
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from . import _ffi, errors
from ._json import JSONInput, decode_json_response, encode_json_input, encode_text

__all__ = [
    "TxnStatus",
    "TxnID",
    "WriteIntent",
    "GraphDirection",
    "Database",
]


class TxnStatus(enum.IntEnum):
    """A transaction intent lifecycle state (antfly_txn_status)."""

    PENDING = 0
    COMMITTED = 1
    ABORTED = 2


class GraphDirection(enum.IntEnum):
    """Graph edge traversal direction, as used by edges()/neighbors()."""

    OUT = 0
    IN = 1
    BOTH = 2


TxnID = bytes
"""A 16-byte transaction identifier used by the C ABI."""


@dataclass
class WriteIntent:
    """A single key/value write or delete in a Lite batch."""

    key: str
    value: Any = b""
    """Document bytes, a str (UTF-8), or any JSON-serializable value."""
    delete: bool = False


WriteLike = WriteIntent | tuple


def _normalize_write(write: WriteLike) -> WriteIntent:
    if isinstance(write, WriteIntent):
        if write.delete or isinstance(write.value, bytes):
            return write
        return WriteIntent(key=write.key, value=_coerce_value(write.value), delete=False)
    if isinstance(write, tuple):
        if len(write) == 2:
            key, value = write
            return WriteIntent(key=key, value=_coerce_value(value))
        if len(write) == 3:
            key, value, delete = write
            return WriteIntent(key=key, value=_coerce_value(value), delete=bool(delete))
    raise TypeError(f"unsupported write intent: {write!r}")


def _coerce_value(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return encode_json_input(value)


def _validate_txn_id(txn_id: TxnID) -> bytes:
    if not isinstance(txn_id, (bytes, bytearray)) or len(txn_id) != 16:
        raise TypeError("txn_id must be 16 bytes")
    return bytes(txn_id)


class Database:
    """An embedded Antfly Lite database handle.

    Do not construct directly; use the module-level create()/open()/
    open_readonly()/open_status_only()/open_hosted()/create_hosted().
    """

    def __init__(self, handle: int) -> None:
        self._lib = _ffi.get_lib()
        self._handle: int | None = handle
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._active = 0
        self._closing = False
        self._closed = False

    # -- handle lifecycle -------------------------------------------------

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _acquire(self) -> int:
        with self._cond:
            # `_closing` (not just `_closed`) must be checked here: once a
            # close() has started, no new call may begin, or a steady stream
            # of new callers could keep `_active` above zero forever and
            # starve the closer's wait loop below (a classic
            # readers-writer-lock starvation bug). Rejecting immediately
            # once closing has started guarantees `_active` only decreases
            # from this point on, mirroring Go's sync.RWMutex, which blocks
            # new RLock()s once a Lock() is pending.
            if self._closing or self._handle is None:
                raise errors.InvalidArgumentError()
            self._active += 1
            return self._handle

    def _release(self) -> None:
        with self._cond:
            self._active -= 1
            if self._active == 0:
                self._cond.notify_all()

    def close(self) -> None:
        """Release the embedded database handle, waiting for in-flight
        calls on other threads to finish first. Safe to call more than once
        and concurrently."""
        with self._cond:
            if self._closing or self._closed:
                while not self._closed:
                    self._cond.wait()
                return
            self._closing = True
            while self._active > 0:
                self._cond.wait()
            handle = self._handle
            self._handle = None
            self._closed = True
            self._cond.notify_all()
        if handle is not None:
            self._lib.antfly_db_close(ctypes.c_void_p(handle))

    # -- low-level call helpers --------------------------------------------

    def _read_buffer(self, fn) -> bytes:
        handle = self._acquire()
        try:
            out = _ffi.AntflyBuffer()
            errors.raise_for_code(fn(ctypes.c_void_p(handle), ctypes.byref(out)))
            return _ffi.take_buffer(out)
        finally:
            self._release()

    def _with_input(self, fn, data: bytes) -> None:
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(data)
            errors.raise_for_code(fn(ctypes.c_void_p(handle), sl))
        finally:
            self._release()

    def _with_input_output(self, fn, data: bytes) -> bytes:
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(data)
            out = _ffi.AntflyBuffer()
            errors.raise_for_code(fn(ctypes.c_void_p(handle), sl, ctypes.byref(out)))
            return _ffi.take_buffer(out)
        finally:
            self._release()

    def _json_read(self, fn, *, raw: bool) -> Any:
        return decode_json_response(self._read_buffer(fn), raw)

    def _json_call(self, fn, request: JSONInput, *, raw: bool) -> Any:
        data = self._with_input_output(fn, encode_json_input(request))
        return decode_json_response(data, raw)

    # -- status / capabilities / maintenance -------------------------------

    def status(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_status_json, raw=raw)

    def capabilities(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_capabilities_json, raw=raw)

    def replay_generated_enrichments(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_replay_generated_enrichments_json, raw=raw)

    def pending_work_stats(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_pending_work_stats_json, raw=raw)

    def run_until_idle(self) -> None:
        """Drain pending enrichment and index work."""
        handle = self._acquire()
        try:
            errors.raise_for_code(self._lib.antfly_lite_run_until_idle(ctypes.c_void_p(handle)))
        finally:
            self._release()

    def run_until_idle_status(self, *, raw: bool = False) -> Any:
        """Drain pending enrichment and index work, returning the post-drain
        pending-work readiness document."""
        return self._json_read(self._lib.antfly_lite_run_until_idle_json, raw=raw)

    def check(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_check_json, raw=raw)

    def vacuum(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_vacuum_json, raw=raw)

    def compact(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_lite_compact_json, raw=raw)

    def copy_stable_snapshot(self, dest_path: str, replace: bool = False, *, raw: bool = False) -> Any:
        if not str(dest_path).endswith(".aflite"):
            raise errors.InvalidArgumentError()
        handle = self._acquire()
        try:
            out = _ffi.AntflyBuffer()
            code = self._lib.antfly_lite_copy_stable_snapshot_json(
                ctypes.c_void_p(handle), _ffi.path_to_bytes(dest_path), ctypes.c_bool(replace), ctypes.byref(out)
            )
            errors.raise_for_code(code)
            return decode_json_response(_ffi.take_buffer(out), raw)
        finally:
            self._release()

    # -- backup / export / import ------------------------------------------

    def backup(self) -> bytes:
        return self._read_buffer(self._lib.antfly_lite_backup)

    def export(self) -> bytes:
        return self._read_buffer(self._lib.antfly_lite_export)

    def import_backup(self, backup: bytes) -> None:
        self._with_input(self._lib.antfly_lite_import_backup, bytes(backup))

    def import_(self, backup: bytes) -> None:
        self._with_input(self._lib.antfly_lite_import, bytes(backup))

    def backup_to_file(self, path: str) -> None:
        if not str(path).endswith(".afb"):
            raise errors.InvalidArgumentError()
        _write_file_atomically(path, self.backup())

    def export_to_file(self, path: str) -> None:
        if not str(path).endswith(".afb"):
            raise errors.InvalidArgumentError()
        _write_file_atomically(path, self.export())

    # -- data ---------------------------------------------------------------

    def batch(self, writes: Sequence[WriteLike], timestamp: int) -> None:
        """Apply write intents at `timestamp` (nanoseconds since epoch)."""
        normalized = [_normalize_write(w) for w in writes]
        handle = self._acquire()
        try:
            c_writes, _keep = _build_write_intents(normalized)
            code = self._lib.antfly_db_batch(
                ctypes.c_void_p(handle),
                c_writes,
                ctypes.c_size_t(len(normalized)),
                None,
                ctypes.c_size_t(0),
                ctypes.c_uint64(timestamp),
                ctypes.c_uint8(0),
            )
            errors.raise_for_code(code)
        finally:
            self._release()

    def batch_json(self, request: JSONInput, *, raw: bool = False) -> Any:
        """Apply a public Antfly batch request (e.g. {"inserts": {...},
        "deletes": [...]})."""
        return self._json_call(self._lib.antfly_db_batch_json, request, raw=raw)

    def lookup(self, key: str, *, raw: bool = False) -> Any:
        data = self._with_input_output(self._lib.antfly_db_lookup_json, encode_text(key))
        return decode_json_response(data, raw)

    def get_raw(self, key: str) -> bytes:
        """Return the raw stored bytes for `key` (not JSON-decoded)."""
        return self._with_input_output(self._lib.antfly_db_get_raw, encode_text(key))

    def get_schema(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_db_get_schema_json, raw=raw)

    def set_schema(self, schema: JSONInput) -> None:
        self._with_input(self._lib.antfly_db_set_schema_json, encode_json_input(schema))

    def scan(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_scan_json, request, raw=raw)

    def stats(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_db_stats_json, raw=raw)

    def search(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_search_json, request, raw=raw)

    def dense_search_wire(self, request: bytes) -> bytes:
        """Execute a packed dense-vector wire search request. Wire format,
        not JSON: always returns raw bytes."""
        return self._with_input_output(self._lib.antfly_db_search_dense_wire, bytes(request))

    def text_match_wire(self, request: bytes) -> bytes:
        return self._with_input_output(self._lib.antfly_db_search_text_match_wire, bytes(request))

    def text_term_wire(self, request: bytes) -> bytes:
        return self._with_input_output(self._lib.antfly_db_search_text_term_wire, bytes(request))

    def text_match_phrase_wire(self, request: bytes) -> bytes:
        return self._with_input_output(self._lib.antfly_db_search_text_match_phrase_wire, bytes(request))

    def aggregate_hits(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_aggregate_hits_json, request, raw=raw)

    def lookup_artifact(self, artifact_id_b64: str, *, raw: bool = False) -> Any:
        data = self._with_input_output(self._lib.antfly_db_lookup_artifact_json, encode_text(artifact_id_b64))
        return decode_json_response(data, raw)

    def extract_enrichments(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_extract_enrichments_json, request, raw=raw)

    def compute_enrichments(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_compute_enrichments_json, request, raw=raw)

    # -- indexes / enrichments -----------------------------------------------

    def list_indexes(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_db_list_indexes_json, raw=raw)

    def add_index(self, config: JSONInput) -> None:
        self._with_input(self._lib.antfly_db_add_index_json, encode_json_input(config))

    def delete_index(self, name: str) -> bool:
        """Delete an index by name, returning whether it existed."""
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(encode_text(name))
            deleted = ctypes.c_bool(False)
            code = self._lib.antfly_db_delete_index(ctypes.c_void_p(handle), sl, ctypes.byref(deleted))
            errors.raise_for_code(code)
            return bool(deleted.value)
        finally:
            self._release()

    def list_enrichments(self, *, raw: bool = False) -> Any:
        return self._json_read(self._lib.antfly_db_list_enrichments_json, raw=raw)

    def add_enrichment(self, config: JSONInput) -> None:
        self._with_input(self._lib.antfly_db_add_enrichment_json, encode_json_input(config))

    def delete_enrichment(self, kind: str, name: str) -> bool:
        """Delete an enrichment by kind and name, returning whether it
        existed."""
        handle = self._acquire()
        try:
            c_kind, _keep1 = _ffi.make_slice(encode_text(kind))
            c_name, _keep2 = _ffi.make_slice(encode_text(name))
            deleted = ctypes.c_bool(False)
            code = self._lib.antfly_db_delete_enrichment(ctypes.c_void_p(handle), c_kind, c_name, ctypes.byref(deleted))
            errors.raise_for_code(code)
            return bool(deleted.value)
        finally:
            self._release()

    # -- graph ----------------------------------------------------------------

    def edges(self, index_name: str, key: str, edge_type: str, direction: int, *, raw: bool = False) -> Any:
        return self._graph_lookup(self._lib.antfly_db_get_edges_json, index_name, key, edge_type, direction, raw=raw)

    def neighbors(self, index_name: str, key: str, edge_type: str, direction: int, *, raw: bool = False) -> Any:
        return self._graph_lookup(
            self._lib.antfly_db_get_neighbors_json, index_name, key, edge_type, direction, raw=raw
        )

    def _graph_lookup(self, fn, index_name: str, key: str, edge_type: str, direction: int, *, raw: bool) -> Any:
        handle = self._acquire()
        try:
            c_index, _k1 = _ffi.make_slice(encode_text(index_name))
            c_key, _k2 = _ffi.make_slice(encode_text(key))
            c_edge_type, _k3 = _ffi.make_slice(encode_text(edge_type))
            out = _ffi.AntflyBuffer()
            code = fn(
                ctypes.c_void_p(handle), c_index, c_key, c_edge_type, ctypes.c_uint8(int(direction)), ctypes.byref(out)
            )
            errors.raise_for_code(code)
            return decode_json_response(_ffi.take_buffer(out), raw)
        finally:
            self._release()

    def traverse_edges(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_traverse_edges_json, request, raw=raw)

    def execute_graph_queries(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_execute_graph_queries_json, request, raw=raw)

    def find_shortest_path(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_find_shortest_path_json, request, raw=raw)

    def find_k_shortest_paths(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_find_k_shortest_paths_json, request, raw=raw)

    def match_pattern(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_db_match_pattern_json, request, raw=raw)

    # -- transactions -----------------------------------------------------

    def begin_transaction(self, txn_id: TxnID, timestamp: int, participants: Sequence[str] | None = None) -> None:
        txn = _validate_txn_id(txn_id)
        handle = self._acquire()
        try:
            c_txn = _ffi.TxnIDArray.from_buffer_copy(txn)
            c_participants, _keep = _build_participants(participants or ())
            code = self._lib.antfly_db_begin_transaction_with_id(
                ctypes.c_void_p(handle),
                ctypes.byref(c_txn),
                ctypes.c_uint64(timestamp),
                c_participants,
                ctypes.c_size_t(len(participants) if participants else 0),
            )
            errors.raise_for_code(code)
        finally:
            self._release()

    def write_transaction(self, txn_id: TxnID, writes: Sequence[WriteLike]) -> None:
        txn = _validate_txn_id(txn_id)
        normalized = [_normalize_write(w) for w in writes]
        handle = self._acquire()
        try:
            c_txn = _ffi.TxnIDArray.from_buffer_copy(txn)
            c_writes, _keep = _build_write_intents(normalized)
            code = self._lib.antfly_db_write_transaction(
                ctypes.c_void_p(handle),
                ctypes.byref(c_txn),
                c_writes,
                ctypes.c_size_t(len(normalized)),
                None,
                ctypes.c_size_t(0),
            )
            errors.raise_for_code(code)
        finally:
            self._release()

    def resolve_transaction(self, txn_id: TxnID, status: TxnStatus, commit_version: int) -> None:
        txn = _validate_txn_id(txn_id)
        handle = self._acquire()
        try:
            c_txn = _ffi.TxnIDArray.from_buffer_copy(txn)
            code = self._lib.antfly_db_resolve_intents(
                ctypes.c_void_p(handle),
                ctypes.byref(c_txn),
                ctypes.c_uint8(int(status)),
                ctypes.c_uint64(commit_version),
            )
            errors.raise_for_code(code)
        finally:
            self._release()

    def transaction_status(self, txn_id: TxnID) -> TxnStatus:
        txn = _validate_txn_id(txn_id)
        handle = self._acquire()
        try:
            c_txn = _ffi.TxnIDArray.from_buffer_copy(txn)
            status = ctypes.c_uint8(0)
            code = self._lib.antfly_db_get_transaction_status(
                ctypes.c_void_p(handle), ctypes.byref(c_txn), ctypes.byref(status)
            )
            errors.raise_for_code(code)
            return TxnStatus(status.value)
        finally:
            self._release()

    def commit_version(self, txn_id: TxnID) -> int:
        txn = _validate_txn_id(txn_id)
        handle = self._acquire()
        try:
            c_txn = _ffi.TxnIDArray.from_buffer_copy(txn)
            version = ctypes.c_uint64(0)
            code = self._lib.antfly_db_get_commit_version(
                ctypes.c_void_p(handle), ctypes.byref(c_txn), ctypes.byref(version)
            )
            errors.raise_for_code(code)
            return int(version.value)
        finally:
            self._release()


def _build_write_intents(writes: Sequence[WriteIntent]):
    if not writes:
        return None, None
    arr = (_ffi.AntflyWriteIntent * len(writes))()
    keep: list[object] = []
    for i, write in enumerate(writes):
        key_slice, key_keep = _ffi.make_slice(encode_text(write.key))
        value_slice, value_keep = (_ffi.AntflySlice(), None) if write.delete else _ffi.make_slice(write.value)
        keep.append(key_keep)
        keep.append(value_keep)
        arr[i] = _ffi.AntflyWriteIntent(key=key_slice, value=value_slice, is_delete=write.delete)
    return arr, keep


def _build_participants(participants: Sequence[str]):
    if not participants:
        return None, None
    arr = (_ffi.AntflySlice * len(participants))()
    keep: list[object] = []
    for i, participant in enumerate(participants):
        sl, k = _ffi.make_slice(encode_text(participant))
        keep.append(k)
        arr[i] = sl
    return arr, keep


def _write_file_atomically(path: str, data: bytes) -> None:
    import os
    import tempfile

    directory = os.path.dirname(path) or "."
    base = os.path.basename(path)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{base}.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise
