# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Python binding for embedded Antfly Lite databases.

Pure Python (ctypes) binding over the ``libantfly`` C ABI's storage-neutral
Lite open surface. See the README for library discovery, the JSON
conventions used across the API, and the threading contract.

Typical usage::

    import antfly_lite

    with antfly_lite.create("my.aflite") as db:
        db.batch([antfly_lite.WriteIntent(key="doc:1", value=b'{"title":"hi"}')], timestamp=1)
        db.run_until_idle()
        print(db.lookup("doc:1"))
"""

from __future__ import annotations

import ctypes as _ctypes
import enum
import math
import os
from dataclasses import dataclass
from datetime import timedelta

from . import _ffi, errors
from ._database import Database, GraphDirection, TxnID, TxnStatus, WriteIntent
from ._inference import Inference, PullProgress
from ._json import decode_json_response, encode_text
from ._library import LibraryNotFoundError
from .errors import (
    AntflyError,
    BusyError,
    CancelledError,
    IntentConflictError,
    InternalError,
    InvalidArgumentError,
    NotFoundError,
    OutcomeUnknownError,
    StalledError,
    TxnNotFoundError,
    UnsupportedError,
    VersionConflictError,
)

__all__ = [
    "__version__",
    "Database",
    "WriteIntent",
    "TxnID",
    "TxnStatus",
    "GraphDirection",
    "OpenMode",
    "Profile",
    "Storage",
    "OpenOptions",
    "TTLCleanupOptions",
    "Inference",
    "PullProgress",
    "LibraryNotFoundError",
    "AntflyError",
    "InvalidArgumentError",
    "NotFoundError",
    "VersionConflictError",
    "IntentConflictError",
    "TxnNotFoundError",
    "BusyError",
    "OutcomeUnknownError",
    "UnsupportedError",
    "StalledError",
    "CancelledError",
    "InternalError",
    "MIN_THREAD_STACK_SIZE",
    "THREADING_SERIALIZED",
    "INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED",
    "INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS",
    "INFERENCE_MODE_REMOTE_PROVIDER",
    "INFERENCE_MODE_LOCAL_EMBEDDED",
    "INFERENCE_MODE_MANUAL_MAINTENANCE",
    "INFERENCE_MODE_DISABLED_DEFERRED",
    "abi_version",
    "threading_mode",
    "validate_abi",
    "create",
    "open",
    "create_with_options",
    "open_with_options",
    "open_readonly",
    "open_status_only",
    "open_hosted",
    "create_hosted",
    "check_file",
    "restore",
    "restore_file",
    "copy_stable_snapshot_file",
    "decode_artifact_id",
]

__version__ = "0.1.0"

THREADING_SERIALIZED = _ffi.THREADING_SERIALIZED
#: Minimum native stack, in bytes, for threads calling into libantfly
#: (ANTFLY_MIN_THREAD_STACK_SIZE). Python's own threads already meet it on
#: Linux and macOS; set threading.stack_size() if your platform's are smaller.
MIN_THREAD_STACK_SIZE = 8 * 1024 * 1024

INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED = _ffi.INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED
INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS = _ffi.INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS
INFERENCE_MODE_REMOTE_PROVIDER = _ffi.INFERENCE_MODE_REMOTE_PROVIDER
INFERENCE_MODE_LOCAL_EMBEDDED = _ffi.INFERENCE_MODE_LOCAL_EMBEDDED
INFERENCE_MODE_MANUAL_MAINTENANCE = _ffi.INFERENCE_MODE_MANUAL_MAINTENANCE
INFERENCE_MODE_DISABLED_DEFERRED = _ffi.INFERENCE_MODE_DISABLED_DEFERRED


class OpenMode(enum.IntEnum):
    """Controls how an Antfly Lite file is opened."""

    WRITER = _ffi.OPEN_MODE_WRITER
    READONLY = _ffi.OPEN_MODE_READONLY
    STATUS_ONLY = _ffi.OPEN_MODE_STATUS_ONLY


class Profile(enum.IntEnum):
    """Selects the Lite runtime profile."""

    NATIVE = _ffi.PROFILE_NATIVE
    HOSTED = _ffi.PROFILE_HOSTED


class Storage(enum.IntEnum):
    """Selects how a database is stored (antfly_open_options.storage_kind).
    The default, LITE, is a single-file .aflite database; DIRECTORY is a
    normal single-node Antfly directory."""

    DIRECTORY = _ffi.STORAGE_KIND_DIRECTORY
    LITE = _ffi.STORAGE_KIND_LITE


@dataclass
class TTLCleanupOptions:
    """Configures the optional Lite TTL cleanup runtime."""

    enabled: bool = False
    lease_owned: bool = False
    owner_id: str = ""
    lease_ttl_ms: int = 0
    interval_ms: int = 0
    batch_size: int = 0
    grace_period_ns: int = 0


@dataclass
class OpenOptions:
    """Configures open_with_options()/create_with_options().

    storage selects a .aflite file (the default, Storage.LITE) or a normal
    Antfly directory (Storage.DIRECTORY). Directory storage is created by
    opening a missing path; create_with_options() has exclusive-create
    semantics only for Storage.LITE, so creating directory storage surfaces
    the library's own error rather than being special-cased here.

    busy_timeout, like sqlite3_busy_timeout, keeps retrying a writer open
    while another process or handle holds the database's writer lock. None
    or zero fails immediately with BusyError. Accepts a number of seconds
    (float/int) or a datetime.timedelta; the C ABI takes whole milliseconds,
    rounded up.

    host_budget_mb, backend_budget_mb, combined_budget_mb, kv_budget_mb,
    scratch_budget_mb, and process_memory_budget_mb are explicit
    resource-budget overrides for the local embedded inference runtime (0
    means the embedded node's own host-clamped default). They are only
    consulted when local_runtime_configured is set.
    """

    storage: Storage = Storage.LITE
    mode: OpenMode = OpenMode.WRITER
    profile: Profile = Profile.NATIVE
    no_sync: bool = False
    remote_provider_configured: bool = False
    local_runtime_configured: bool = False
    generated_enrichment_replay: bool = False
    map_size: int = 0
    ttl_cleanup: TTLCleanupOptions | None = None
    host_budget_mb: int = 0
    backend_budget_mb: int = 0
    combined_budget_mb: int = 0
    kv_budget_mb: int = 0
    scratch_budget_mb: int = 0
    process_memory_budget_mb: int = 0
    busy_timeout: float | int | timedelta | None = None


def _new_database(handle: _ctypes.c_void_p) -> Database:
    if handle.value is None:
        raise errors.InternalError(message="antfly_db_open returned ANTFLY_OK with a null handle")
    return Database(handle.value)


def abi_version() -> int:
    """The loaded Antfly C ABI version."""
    return int(_ffi.get_lib().antfly_abi_version())


def threading_mode() -> int:
    """The loaded libantfly threading contract (like sqlite3_threadsafe());
    always THREADING_SERIALIZED."""
    return int(_ffi.get_lib().antfly_threading_mode())


def validate_abi() -> None:
    """Verify the loaded C library matches the header this binding was
    written against. Raises antfly_lite.errors... a RuntimeError subclass on
    mismatch (see _ffi.ABIVersionError)."""
    _ffi.validate_abi()


def _busy_timeout_ms(value: float | int | timedelta | None) -> int:
    if value is None:
        return 0
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
    else:
        seconds = float(value)
    if seconds <= 0:
        return 0
    return math.ceil(seconds * 1000)


def _build_c_options(opts: OpenOptions) -> tuple[_ffi.AntflyOpenOptions, object]:
    """Build the C options struct. Returns a keep-alive object that must
    stay referenced for the duration of the call using the returned
    struct (it backs the ttl_cleanup owner-id slice, if any)."""
    lib = _ffi.get_lib()
    c_opts = _ffi.AntflyOpenOptions()
    errors.raise_for_code(lib.antfly_open_options_init(_ctypes.byref(c_opts)))

    c_opts.storage_kind = int(opts.storage)
    c_opts.open_mode = int(opts.mode)
    c_opts.profile = int(opts.profile)
    c_opts.map_size = opts.map_size
    c_opts.inference_host_budget_mb = opts.host_budget_mb
    c_opts.inference_backend_budget_mb = opts.backend_budget_mb
    c_opts.inference_process_memory_budget_mb = opts.process_memory_budget_mb
    c_opts.inference_combined_budget_mb = opts.combined_budget_mb
    c_opts.inference_kv_budget_mb = opts.kv_budget_mb
    c_opts.inference_scratch_budget_mb = opts.scratch_budget_mb
    c_opts.busy_timeout_ms = _busy_timeout_ms(opts.busy_timeout)

    flags = 0
    keep_alive: object = None
    if opts.no_sync:
        flags |= _ffi.OPEN_FLAG_NO_SYNC
    if opts.remote_provider_configured:
        flags |= _ffi.OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED
    if opts.local_runtime_configured:
        flags |= _ffi.OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED
    if opts.generated_enrichment_replay:
        flags |= _ffi.OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY
    if opts.ttl_cleanup is not None:
        flags |= _ffi.OPEN_FLAG_TTL_CLEANUP
        ttl = opts.ttl_cleanup
        c_opts.ttl_cleanup_enabled = ttl.enabled
        c_opts.ttl_cleanup_lease_owned = ttl.lease_owned
        c_opts.ttl_cleanup_lease_ttl_ms = ttl.lease_ttl_ms
        c_opts.ttl_cleanup_interval_ms = ttl.interval_ms
        c_opts.ttl_cleanup_batch_size = ttl.batch_size
        c_opts.ttl_cleanup_grace_period_ns = ttl.grace_period_ns
        owner_slice, keep_alive = _ffi.make_slice(encode_text(ttl.owner_id))
        c_opts.ttl_cleanup_owner_id = owner_slice
    c_opts.flags = flags
    return c_opts, keep_alive


def open_with_options(path: str | os.PathLike[str], options: OpenOptions) -> Database:
    """Open a database of the storage kind `options.storage` selects, using
    explicit options. A directory is created if the path is missing; a
    missing .aflite path fails (use create_with_options() to create one)."""
    return _open_with_options(path, options, create=False)


def create_with_options(path: str | os.PathLike[str], options: OpenOptions) -> Database:
    """Create a database using explicit options. Exclusive-create semantics
    only apply to Storage.LITE; creating Storage.DIRECTORY surfaces the
    library's own error."""
    return _open_with_options(path, options, create=True)


def _open_with_options(path: str | os.PathLike[str], opts: OpenOptions, create: bool) -> Database:
    _ffi.validate_abi()
    lib = _ffi.get_lib()
    c_opts, _keep = _build_c_options(opts)

    c_path = _ffi.path_to_bytes(path)
    handle = _ctypes.c_void_p()
    fn = lib.antfly_db_create_with_options if create else lib.antfly_db_open_with_options
    code = fn(c_path, _ctypes.byref(c_opts), _ctypes.byref(handle))
    errors.raise_for_code(code)
    return _new_database(handle)


def create(
    path: str | os.PathLike[str],
    *,
    storage: Storage = Storage.LITE,
    mode: OpenMode = OpenMode.WRITER,
    profile: Profile = Profile.NATIVE,
    no_sync: bool = False,
    busy_timeout: float | int | timedelta | None = None,
    local_runtime: bool = False,
    remote_provider: bool = False,
    generated_enrichment_replay: bool = False,
    map_size: int = 0,
    ttl_cleanup: TTLCleanupOptions | None = None,
    host_budget_mb: int = 0,
    backend_budget_mb: int = 0,
    combined_budget_mb: int = 0,
    kv_budget_mb: int = 0,
    scratch_budget_mb: int = 0,
    process_memory_budget_mb: int = 0,
) -> Database:
    """Create a new Antfly database: a .aflite file (storage=Storage.LITE,
    the default) or a directory (storage=Storage.DIRECTORY, which surfaces
    the library's own error since directories aren't exclusively created)."""
    opts = OpenOptions(
        storage=storage,
        mode=mode,
        profile=profile,
        no_sync=no_sync,
        busy_timeout=busy_timeout,
        remote_provider_configured=remote_provider,
        local_runtime_configured=local_runtime,
        generated_enrichment_replay=generated_enrichment_replay,
        map_size=map_size,
        ttl_cleanup=ttl_cleanup,
        host_budget_mb=host_budget_mb,
        backend_budget_mb=backend_budget_mb,
        combined_budget_mb=combined_budget_mb,
        kv_budget_mb=kv_budget_mb,
        scratch_budget_mb=scratch_budget_mb,
        process_memory_budget_mb=process_memory_budget_mb,
    )
    return create_with_options(path, opts)


def open(
    path: str | os.PathLike[str],
    *,
    storage: Storage = Storage.LITE,
    mode: OpenMode = OpenMode.WRITER,
    profile: Profile = Profile.NATIVE,
    no_sync: bool = False,
    busy_timeout: float | int | timedelta | None = None,
    local_runtime: bool = False,
    remote_provider: bool = False,
    generated_enrichment_replay: bool = False,
    map_size: int = 0,
    ttl_cleanup: TTLCleanupOptions | None = None,
    host_budget_mb: int = 0,
    backend_budget_mb: int = 0,
    combined_budget_mb: int = 0,
    kv_budget_mb: int = 0,
    scratch_budget_mb: int = 0,
    process_memory_budget_mb: int = 0,
) -> Database:
    """Open an existing Antfly database: a .aflite file (storage=Storage.LITE,
    the default) or a directory (storage=Storage.DIRECTORY, created
    automatically if the path is missing)."""
    opts = OpenOptions(
        storage=storage,
        mode=mode,
        profile=profile,
        no_sync=no_sync,
        busy_timeout=busy_timeout,
        remote_provider_configured=remote_provider,
        local_runtime_configured=local_runtime,
        generated_enrichment_replay=generated_enrichment_replay,
        map_size=map_size,
        ttl_cleanup=ttl_cleanup,
        host_budget_mb=host_budget_mb,
        backend_budget_mb=backend_budget_mb,
        combined_budget_mb=combined_budget_mb,
        kv_budget_mb=kv_budget_mb,
        scratch_budget_mb=scratch_budget_mb,
        process_memory_budget_mb=process_memory_budget_mb,
    )
    return open_with_options(path, opts)


def open_readonly(path: str | os.PathLike[str]) -> Database:
    """Open an existing Antfly Lite database file read-only."""
    return open_with_options(path, OpenOptions(mode=OpenMode.READONLY, profile=Profile.NATIVE))


def open_status_only(path: str | os.PathLike[str]) -> Database:
    """Open enough of an Antfly Lite database to read status."""
    return open_with_options(path, OpenOptions(mode=OpenMode.STATUS_ONLY, profile=Profile.NATIVE))


def open_hosted(path: str | os.PathLike[str]) -> Database:
    """Open an existing Antfly Lite database in hosted/manual maintenance
    mode. In this profile callers drive pending work explicitly with
    run_until_idle()."""
    _ffi.validate_abi()
    lib = _ffi.get_lib()
    handle = _ctypes.c_void_p()
    errors.raise_for_code(lib.antfly_lite_open_hosted(_ffi.path_to_bytes(path), _ctypes.byref(handle)))
    return _new_database(handle)


def create_hosted(path: str | os.PathLike[str]) -> Database:
    """Create a new Antfly Lite database in hosted/manual maintenance mode."""
    _ffi.validate_abi()
    lib = _ffi.get_lib()
    handle = _ctypes.c_void_p()
    errors.raise_for_code(lib.antfly_lite_create_hosted(_ffi.path_to_bytes(path), _ctypes.byref(handle)))
    return _new_database(handle)


def check_file(path: str | os.PathLike[str], *, raw: bool = False):
    """Run Lite integrity checks for `path` without opening a database
    handle."""
    _ffi.validate_abi()
    lib = _ffi.get_lib()
    out = _ffi.AntflyBuffer()
    errors.raise_for_code(lib.antfly_lite_check_file_json(_ffi.path_to_bytes(path), _ctypes.byref(out)))
    return decode_json_response(_ffi.take_buffer(out), raw)


def copy_stable_snapshot_file(
    src_path: str | os.PathLike[str],
    dest_path: str | os.PathLike[str],
    replace: bool = False,
    *,
    raw: bool = False,
):
    """Open `src_path` read-only, copy a stable Lite snapshot to
    `dest_path`, and return the JSON result."""
    if not str(src_path).endswith(".aflite") or not str(dest_path).endswith(".aflite"):
        raise errors.InvalidArgumentError()
    _ffi.validate_abi()
    lib = _ffi.get_lib()
    out = _ffi.AntflyBuffer()
    code = lib.antfly_lite_copy_stable_snapshot_file_json(
        _ffi.path_to_bytes(src_path), _ffi.path_to_bytes(dest_path), _ctypes.c_bool(replace), _ctypes.byref(out)
    )
    errors.raise_for_code(code)
    return decode_json_response(_ffi.take_buffer(out), raw)


def _restore_c_options(storage: Storage) -> _ffi.AntflyOpenOptions:
    lib = _ffi.get_lib()
    c_opts = _ffi.AntflyOpenOptions()
    errors.raise_for_code(lib.antfly_open_options_init(_ctypes.byref(c_opts)))
    c_opts.storage_kind = int(storage)
    return c_opts


def restore(
    path: str | os.PathLike[str],
    backup: bytes,
    *,
    storage: Storage = Storage.LITE,
    replace: bool = False,
) -> None:
    """Create a database at `path` from a portable Antfly backup archive
    held in memory. A backup of either storage kind restores into either
    kind; `storage` selects the destination kind (the default, Storage.LITE,
    still requires a `.aflite` path client-side; Storage.DIRECTORY has no
    suffix requirement)."""
    if not backup or (storage == Storage.LITE and not str(path).endswith(".aflite")):
        raise errors.InvalidArgumentError()
    lib = _ffi.get_lib()
    c_opts = _restore_c_options(storage)
    sl, _keep = _ffi.make_slice(bytes(backup))
    out = _ffi.AntflyBuffer()
    code = lib.antfly_restore_backup_json(
        _ffi.path_to_bytes(path), _ctypes.byref(c_opts), sl, _ctypes.c_bool(replace), _ctypes.byref(out)
    )
    errors.raise_for_code(code)
    _ffi.take_buffer(out)


def restore_file(
    path: str | os.PathLike[str],
    backup_path: str | os.PathLike[str],
    *,
    storage: Storage = Storage.LITE,
    replace: bool = False,
) -> None:
    """Stream-restore a database at `path` from a portable Antfly backup
    archive file with bounded memory use. `storage` selects the destination
    kind (the default, Storage.LITE, still requires a `.aflite` path
    client-side; Storage.DIRECTORY has no suffix requirement)."""
    if not str(backup_path).endswith(".afb") or (storage == Storage.LITE and not str(path).endswith(".aflite")):
        raise errors.InvalidArgumentError()
    lib = _ffi.get_lib()
    c_opts = _restore_c_options(storage)
    out = _ffi.AntflyBuffer()
    code = lib.antfly_restore_backup_file_json(
        _ffi.path_to_bytes(path),
        _ctypes.byref(c_opts),
        _ffi.path_to_bytes(backup_path),
        _ctypes.c_bool(replace),
        _ctypes.byref(out),
    )
    errors.raise_for_code(code)
    _ffi.take_buffer(out)


def decode_artifact_id(artifact_id_b64: str, *, raw: bool = False):
    """Decode a base64 artifact ID without opening a database."""
    lib = _ffi.get_lib()
    sl, _keep = _ffi.make_slice(encode_text(artifact_id_b64))
    out = _ffi.AntflyBuffer()
    errors.raise_for_code(lib.antfly_decode_artifact_id_json(sl, _ctypes.byref(out)))
    return decode_json_response(_ffi.take_buffer(out), raw)
