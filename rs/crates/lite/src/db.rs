//! The embedded Antfly Lite database handle and its core operations.

use std::ffi::c_void;
use std::path::Path;
use std::sync::{Condvar, Mutex, PoisonError};

use antfly_lite_sys::{self as sys, antfly_buffer, antfly_error_code, antfly_slice};

use crate::error::{Error, Result};
use crate::ffi::{borrow_slice, check, path_has_suffix, path_to_cstring, take_buffer};
use crate::options::{GraphDirection, OpenOptions, WriteIntent};

/// The Antfly C ABI version this binding expects.
pub const SUPPORTED_ABI_VERSION: u32 = 1;

/// The only threading mode libantfly provides: any thread may call any
/// method on a [`Database`] concurrently. See [`threading_mode`].
pub const THREADING_SERIALIZED: u32 = sys::ANTFLY_THREADING_SERIALIZED;

/// Minimum stack, in bytes, for any thread that calls into a [`Database`].
/// Rust's default for spawned threads is 2 MiB, which is not enough; spawn
/// callers with `std::thread::Builder::new().stack_size(MIN_THREAD_STACK_SIZE)`
/// (and size async runtimes' blocking pools the same way).
pub const MIN_THREAD_STACK_SIZE: usize = sys::ANTFLY_MIN_THREAD_STACK_SIZE;

/// Reports the loaded libantfly threading contract, like
/// `sqlite3_threadsafe()`.
pub fn threading_mode() -> u32 {
    unsafe { sys::antfly_threading_mode() }
}

/// Returns the loaded Antfly C ABI version.
pub fn abi_version() -> u32 {
    unsafe { sys::antfly_abi_version() }
}

/// Returns the loaded C ABI size of `antfly_lite_open_options`.
pub fn open_options_size() -> u32 {
    unsafe { sys::antfly_lite_open_options_size() }
}

fn compiled_open_options_size() -> u32 {
    std::mem::size_of::<sys::antfly_lite_open_options>() as u32
}

/// Verifies that the loaded C library matches the header this binding was
/// compiled against. Every open function calls this first, like the Go
/// binding. A mismatch is reported as [`Error::Internal`]: it indicates a
/// build/deployment inconsistency (a `libantfly` built for a different ABI
/// generation), not a normal operational failure.
pub fn validate_abi() -> Result<()> {
    if abi_version() != SUPPORTED_ABI_VERSION {
        return Err(Error::Internal);
    }
    if open_options_size() != compiled_open_options_size() {
        return Err(Error::Internal);
    }
    Ok(())
}

/// Shared mutable state behind [`HandleGate`]: the handle itself (`None`
/// once closed) plus the bookkeeping needed for a writer-preferring
/// reader/writer gate.
struct GateState {
    handle: Option<*mut c_void>,
    readers: u32,
    /// Set while at least one thread is waiting to close. Blocks *new*
    /// reader acquisitions so that a sustained stream of reads cannot starve
    /// `close` -- see the doc comment on [`HandleGate`].
    closer_waiting: bool,
    /// Set while the (single, at a time) close is actually running.
    closer_active: bool,
}

/// A small hand-rolled reader/writer gate, used instead of
/// `std::sync::RwLock` because `std`'s `RwLock` explicitly makes no
/// fairness guarantees -- on at least macOS's `pthread_rwlock`, a steady
/// stream of readers can starve a writer indefinitely. The Go binding's
/// `sync.RWMutex` does not have that problem (new `RLock` calls block once a
/// `Lock` is pending), and libantfly's own contract says `antfly_db_close`
/// "waits for every call that has already entered ... and then frees the
/// handle" -- a bounded wait, not a potentially-unbounded one. This gate
/// gives the same guarantee: once a close is requested, new calls block
/// until it completes (or observe the database as already closed), while
/// already in-flight calls are allowed to finish normally.
struct HandleGate {
    state: Mutex<GateState>,
    cond: Condvar,
}

impl HandleGate {
    fn new(handle: *mut c_void) -> HandleGate {
        HandleGate {
            state: Mutex::new(GateState {
                handle: Some(handle),
                readers: 0,
                closer_waiting: false,
                closer_active: false,
            }),
            cond: Condvar::new(),
        }
    }

    /// Runs `f` with the live handle, or returns [`Error::InvalidArgument`]
    /// if the database has already been closed or a close is in progress.
    fn with_handle<T>(&self, f: impl FnOnce(*mut c_void) -> Result<T>) -> Result<T> {
        let handle = {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if guard.closer_waiting || guard.closer_active {
                    guard = self
                        .cond
                        .wait(guard)
                        .unwrap_or_else(PoisonError::into_inner);
                    continue;
                }
                break;
            }
            match guard.handle {
                Some(handle) => {
                    guard.readers += 1;
                    handle
                }
                None => return Err(Error::InvalidArgument),
            }
        };

        // Release the reader slot even if `f` panics; otherwise a later
        // close (or Drop) would wait forever for a reader that never leaves.
        struct ReaderSlot<'a>(&'a HandleGate);
        impl Drop for ReaderSlot<'_> {
            fn drop(&mut self) {
                let mut guard = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
                guard.readers -= 1;
                if guard.readers == 0 {
                    self.0.cond.notify_all();
                }
            }
        }
        let _slot = ReaderSlot(self);
        f(handle)
    }

    /// Waits for every call already in flight to finish, then closes the
    /// handle (a no-op if it is already closed, whether by this call or a
    /// concurrent one). Safe to call concurrently and more than once.
    fn close(&self, close_fn: impl FnOnce(*mut c_void)) {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.handle.is_none() {
            return;
        }
        guard.closer_waiting = true;
        while guard.readers > 0 || guard.closer_active {
            guard = self
                .cond
                .wait(guard)
                .unwrap_or_else(PoisonError::into_inner);
        }
        guard.closer_waiting = false;

        let Some(handle) = guard.handle.take() else {
            // Another concurrent close already took it while we waited.
            self.cond.notify_all();
            return;
        };
        guard.closer_active = true;
        drop(guard);

        close_fn(handle);

        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        guard.closer_active = false;
        self.cond.notify_all();
    }

    fn is_open(&self) -> bool {
        self.state
            .lock()
            .map(|guard| guard.handle.is_some())
            .unwrap_or(false)
    }
}

/// An embedded Antfly Lite database handle.
///
/// `Database` is safe for concurrent use from multiple threads, like
/// `*sql.DB` in the Go binding: libantfly runs in serialized threading mode
/// (see [`threading_mode`]) -- reads run in parallel, writes on one handle
/// queue behind each other instead of failing with `Busy`, and schema or
/// index changes wait for in-flight calls. [`Database::close`] (and
/// [`Drop`]) wait for in-flight calls to finish; calls made after close
/// return [`Error::InvalidArgument`].
pub struct Database {
    // See `HandleGate`: a writer-preferring reader/writer gate, not
    // `std::sync::RwLock`, so a steady stream of reads cannot starve
    // `close`. The actual write-vs-write serialization for concurrent
    // mutations happens inside libantfly itself, not in this gate.
    gate: HandleGate,
}

// SAFETY: libantfly's only threading mode is "serialized" (see
// `antfly_threading_mode`, `ANTFLY_THREADING_SERIALIZED`, and
// zig/CAPI.md "Thread Safety"): any thread may call any exported function on
// any handle concurrently -- reads run in parallel, writes on one handle
// queue behind each other, and schema/admin changes wait for in-flight
// calls. `HandleGate` above only prevents calling into a handle after
// `antfly_db_close` has returned (a use-after-free libantfly itself cannot
// guard against); nothing about `*mut c_void` here is thread-affine.
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    fn from_handle(handle: *mut c_void) -> Database {
        Database {
            gate: HandleGate::new(handle),
        }
    }

    /// Runs `f` with the live handle, or returns [`Error::InvalidArgument`]
    /// if the database has already been closed. See [`HandleGate`].
    pub(crate) fn with_handle<T>(&self, f: impl FnOnce(*mut c_void) -> Result<T>) -> Result<T> {
        self.gate.with_handle(f)
    }

    pub(crate) fn read_buffer(
        &self,
        f: impl FnOnce(*mut c_void, *mut antfly_buffer) -> antfly_error_code,
    ) -> Result<Vec<u8>> {
        self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            check(f(handle, &mut out))?;
            Ok(unsafe { take_buffer(out) })
        })
    }

    pub(crate) fn with_input(
        &self,
        input: &[u8],
        f: impl FnOnce(*mut c_void, antfly_slice) -> antfly_error_code,
    ) -> Result<()> {
        self.with_handle(|handle| check(f(handle, borrow_slice(input))))
    }

    pub(crate) fn with_input_output(
        &self,
        input: &[u8],
        f: impl FnOnce(*mut c_void, antfly_slice, *mut antfly_buffer) -> antfly_error_code,
    ) -> Result<Vec<u8>> {
        self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            check(f(handle, borrow_slice(input), &mut out))?;
            Ok(unsafe { take_buffer(out) })
        })
    }

    fn graph_lookup(
        &self,
        index_name: &str,
        key: &str,
        edge_type: &str,
        direction: u8,
        f: impl FnOnce(
            *mut c_void,
            antfly_slice,
            antfly_slice,
            antfly_slice,
            u8,
            *mut antfly_buffer,
        ) -> antfly_error_code,
    ) -> Result<Vec<u8>> {
        self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            check(f(
                handle,
                borrow_slice(index_name.as_bytes()),
                borrow_slice(key.as_bytes()),
                borrow_slice(edge_type.as_bytes()),
                direction,
                &mut out,
            ))?;
            Ok(unsafe { take_buffer(out) })
        })
    }

    // -- Open / create ---------------------------------------------------

    /// Opens an existing Antfly Lite database using explicit options.
    pub fn open(path: impl AsRef<Path>, options: &OpenOptions) -> Result<Database> {
        Self::open_with(path.as_ref(), options, false)
    }

    /// Creates a new Antfly Lite database using explicit options.
    pub fn create(path: impl AsRef<Path>, options: &OpenOptions) -> Result<Database> {
        Self::open_with(path.as_ref(), options, true)
    }

    /// Opens an existing native Antfly Lite database file for writing
    /// (writer mode, native profile, no other options).
    pub fn open_default(path: impl AsRef<Path>) -> Result<Database> {
        Self::open(path, &OpenOptions::default())
    }

    /// Creates a new native Antfly Lite database file for writing (writer
    /// mode, native profile, no other options).
    pub fn create_default(path: impl AsRef<Path>) -> Result<Database> {
        Self::create(path, &OpenOptions::default())
    }

    /// Opens an existing Antfly Lite database file read-only.
    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Database> {
        Self::open(
            path,
            &OpenOptions {
                mode: crate::options::OpenMode::Readonly,
                ..Default::default()
            },
        )
    }

    /// Opens enough of an Antfly Lite database to read status.
    pub fn open_status_only(path: impl AsRef<Path>) -> Result<Database> {
        Self::open(
            path,
            &OpenOptions {
                mode: crate::options::OpenMode::StatusOnly,
                ..Default::default()
            },
        )
    }

    /// Opens an existing Antfly Lite database in hosted/manual maintenance
    /// mode. In this profile callers drive pending work explicitly with
    /// [`Database::run_until_idle`].
    pub fn open_hosted(path: impl AsRef<Path>) -> Result<Database> {
        validate_abi()?;
        let c_path = path_to_cstring(path.as_ref())?;
        let mut handle: *mut c_void = std::ptr::null_mut();
        check(unsafe { sys::antfly_lite_open_hosted(c_path.as_ptr(), &mut handle) })?;
        Ok(Database::from_handle(handle))
    }

    /// Creates a new Antfly Lite database in hosted/manual maintenance
    /// mode. In this profile callers drive pending work explicitly with
    /// [`Database::run_until_idle`].
    pub fn create_hosted(path: impl AsRef<Path>) -> Result<Database> {
        validate_abi()?;
        let c_path = path_to_cstring(path.as_ref())?;
        let mut handle: *mut c_void = std::ptr::null_mut();
        check(unsafe { sys::antfly_lite_create_hosted(c_path.as_ptr(), &mut handle) })?;
        Ok(Database::from_handle(handle))
    }

    fn open_with(path: &Path, options: &OpenOptions, create: bool) -> Result<Database> {
        validate_abi()?;
        let c_path = path_to_cstring(path)?;

        let mut c_opts: sys::antfly_lite_open_options = unsafe { std::mem::zeroed() };
        check(unsafe { sys::antfly_lite_open_options_init(&mut c_opts) })?;

        c_opts.open_mode = options.mode.as_u32();
        c_opts.profile = options.profile.as_u32();
        c_opts.map_size = options.map_size;
        if options.no_sync {
            c_opts.flags |= sys::ANTFLY_LITE_OPEN_FLAG_NO_SYNC;
        }
        if options.remote_provider_configured {
            c_opts.flags |= sys::ANTFLY_LITE_OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED;
        }
        if options.local_runtime_configured {
            c_opts.flags |= sys::ANTFLY_LITE_OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED;
        }
        if options.generated_enrichment_replay {
            c_opts.flags |= sys::ANTFLY_LITE_OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY;
        }
        c_opts.inference_host_budget_mb = options.host_budget_mb;
        c_opts.inference_backend_budget_mb = options.backend_budget_mb;
        c_opts.inference_combined_budget_mb = options.combined_budget_mb;
        c_opts.inference_kv_budget_mb = options.kv_budget_mb;
        c_opts.inference_scratch_budget_mb = options.scratch_budget_mb;
        c_opts.inference_process_memory_budget_mb = options.process_memory_budget_mb;
        c_opts.busy_timeout_ms = options.busy_timeout_ms();

        if let Some(ttl) = &options.ttl_cleanup {
            c_opts.flags |= sys::ANTFLY_LITE_OPEN_FLAG_TTL_CLEANUP;
            c_opts.ttl_cleanup_enabled = ttl.enabled;
            c_opts.ttl_cleanup_lease_owned = ttl.lease_owned;
            c_opts.ttl_cleanup_lease_ttl_ms = ttl.lease_ttl_ms;
            c_opts.ttl_cleanup_interval_ms = ttl.interval_ms;
            c_opts.ttl_cleanup_batch_size = ttl.batch_size;
            c_opts.ttl_cleanup_grace_period_ns = ttl.grace_period_ns;
            // `ttl.owner_id` outlives this call (borrowed from `options`,
            // which the caller keeps alive for the duration of `open_with`).
            c_opts.ttl_cleanup_owner_id = borrow_slice(ttl.owner_id.as_bytes());
        }

        let mut handle: *mut c_void = std::ptr::null_mut();
        let code = if create {
            unsafe { sys::antfly_lite_create_with_options(c_path.as_ptr(), &c_opts, &mut handle) }
        } else {
            unsafe { sys::antfly_lite_open_with_options(c_path.as_ptr(), &c_opts, &mut handle) }
        };
        check(code)?;
        Ok(Database::from_handle(handle))
    }

    // -- Close ------------------------------------------------------------

    /// Releases the embedded database handle, waiting for in-flight calls
    /// on other threads to finish first. Idempotent and safe to call
    /// concurrently with itself or from several threads at once (matching
    /// `antfly_db_close`'s contract), including through a shared
    /// `Arc<Database>` -- calls made after (or racing) `close` return
    /// [`Error::InvalidArgument`] rather than touching a freed handle.
    ///
    /// Takes `&self`, not `self`, specifically so it can be called on a
    /// `Database` shared across threads (e.g. `Arc<Database>`) without every
    /// other clone having to be dropped first; [`Drop`] still closes
    /// automatically once the last owner goes out of scope, for callers who
    /// never need to close early.
    pub fn close(&self) -> Result<()> {
        self.close_inner();
        Ok(())
    }

    fn close_inner(&self) {
        self.gate
            .close(|handle| unsafe { sys::antfly_db_close(handle) });
    }

    // -- Status / maintenance JSON -----------------------------------------

    pub fn status_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_status_json(h, out) })
    }

    pub fn capabilities_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_capabilities_json(h, out) })
    }

    pub fn replay_generated_enrichments_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe {
            sys::antfly_lite_replay_generated_enrichments_json(h, out)
        })
    }

    /// Returns a portable Antfly backup archive for this Lite database.
    pub fn backup(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_backup(h, out) })
    }

    /// Returns a portable Antfly backup archive for this Lite database.
    /// Kept alongside [`Database::backup`] for parity with the Go binding's
    /// in-progress backup/export naming migration; both call the same
    /// underlying semantics through distinct C ABI entry points.
    pub fn export(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_export(h, out) })
    }

    /// Imports a portable Antfly backup archive into this Lite database.
    /// [`Error::OutcomeUnknown`] means the live handle adopted the imported
    /// generation, but crash durability could not be confirmed; inspect the
    /// handle and do not retry automatically.
    pub fn import_backup(&self, backup: impl AsRef<[u8]>) -> Result<()> {
        self.with_input(backup.as_ref(), |h, input| unsafe {
            sys::antfly_lite_import_backup(h, input)
        })
    }

    /// See [`Database::import_backup`].
    pub fn import(&self, backup: impl AsRef<[u8]>) -> Result<()> {
        self.with_input(backup.as_ref(), |h, input| unsafe {
            sys::antfly_lite_import(h, input)
        })
    }

    /// Runs Lite integrity checks and returns the JSON result.
    pub fn check_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_check_json(h, out) })
    }

    /// Compacts free space and returns the JSON result.
    pub fn vacuum_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_vacuum_json(h, out) })
    }

    /// Drains maintenance, compacts indexes, vacuums free space, and
    /// returns the JSON result.
    pub fn compact_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_compact_json(h, out) })
    }

    /// Copies a stable Lite snapshot to `dest_path`, which must end in
    /// `.aflite`.
    pub fn copy_stable_snapshot_json(
        &self,
        dest_path: impl AsRef<Path>,
        replace: bool,
    ) -> Result<Vec<u8>> {
        let dest_path = dest_path.as_ref();
        if !path_has_suffix(dest_path, ".aflite") {
            return Err(Error::InvalidArgument);
        }
        let c_dest = path_to_cstring(dest_path)?;
        self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            check(unsafe {
                sys::antfly_lite_copy_stable_snapshot_json(
                    handle,
                    c_dest.as_ptr(),
                    replace,
                    &mut out,
                )
            })?;
            Ok(unsafe { take_buffer(out) })
        })
    }

    // -- Batch / documents --------------------------------------------------

    /// Applies write intents at `timestamp_ns`.
    pub fn batch(&self, writes: &[WriteIntent], timestamp_ns: u64) -> Result<()> {
        self.with_handle(|handle| {
            // Keep every borrowed antfly_slice's backing bytes alive for the
            // duration of the call by holding the WriteIntent Vecs (`writes`)
            // borrowed for the whole closure, and building the antfly_slice
            // array from them just before the call.
            let c_writes: Vec<sys::antfly_write_intent> = writes
                .iter()
                .map(|w| sys::antfly_write_intent {
                    key: borrow_slice(&w.key),
                    value: borrow_slice(&w.value),
                    is_delete: w.delete,
                })
                .collect();
            let ptr = if c_writes.is_empty() {
                std::ptr::null()
            } else {
                c_writes.as_ptr()
            };
            check(unsafe {
                sys::antfly_db_batch(
                    handle,
                    ptr,
                    c_writes.len(),
                    std::ptr::null(),
                    0,
                    timestamp_ns,
                    0,
                )
            })
        })
    }

    /// Applies a public Antfly batch request and returns the JSON result.
    pub fn batch_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_batch_json(h, input, out)
        })
    }

    /// Returns the JSON lookup result for `key`.
    pub fn lookup_json(&self, key: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(key.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_lookup_json(h, input, out)
        })
    }

    /// Returns the raw stored bytes for `key`.
    pub fn get_raw(&self, key: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(key.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_get_raw(h, input, out)
        })
    }

    // -- Schema / indexes / enrichments -------------------------------------

    pub fn schema_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_db_get_schema_json(h, out) })
    }

    pub fn set_schema_json(&self, schema: impl AsRef<[u8]>) -> Result<()> {
        self.with_input(schema.as_ref(), |h, input| unsafe {
            sys::antfly_db_set_schema_json(h, input)
        })
    }

    /// Drains pending enrichment and index work.
    pub fn run_until_idle(&self) -> Result<()> {
        self.with_handle(|handle| check(unsafe { sys::antfly_lite_run_until_idle(handle) }))
    }

    /// Drains pending enrichment and index work and returns the post-drain
    /// pending work stats JSON.
    pub fn run_until_idle_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_run_until_idle_json(h, out) })
    }

    pub fn pending_work_stats_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_lite_pending_work_stats_json(h, out) })
    }

    pub fn indexes_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_db_list_indexes_json(h, out) })
    }

    pub fn add_index_json(&self, config: impl AsRef<[u8]>) -> Result<()> {
        self.with_input(config.as_ref(), |h, input| unsafe {
            sys::antfly_db_add_index_json(h, input)
        })
    }

    /// Deletes an index by name and reports whether it existed.
    pub fn delete_index(&self, name: &str) -> Result<bool> {
        self.with_handle(|handle| {
            let mut deleted = false;
            check(unsafe {
                sys::antfly_db_delete_index(handle, borrow_slice(name.as_bytes()), &mut deleted)
            })?;
            Ok(deleted)
        })
    }

    pub fn enrichments_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_db_list_enrichments_json(h, out) })
    }

    pub fn add_enrichment_json(&self, config: impl AsRef<[u8]>) -> Result<()> {
        self.with_input(config.as_ref(), |h, input| unsafe {
            sys::antfly_db_add_enrichment_json(h, input)
        })
    }

    /// Deletes an enrichment by kind and name and reports whether it
    /// existed.
    pub fn delete_enrichment(&self, kind: &str, name: &str) -> Result<bool> {
        self.with_handle(|handle| {
            let mut deleted = false;
            check(unsafe {
                sys::antfly_db_delete_enrichment(
                    handle,
                    borrow_slice(kind.as_bytes()),
                    borrow_slice(name.as_bytes()),
                    &mut deleted,
                )
            })?;
            Ok(deleted)
        })
    }

    // -- Scan / stats / search ----------------------------------------------

    pub fn scan_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_scan_json(h, input, out)
        })
    }

    pub fn stats_json(&self) -> Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_db_stats_json(h, out) })
    }

    pub fn search_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_search_json(h, input, out)
        })
    }

    /// Executes a packed dense-vector wire search request.
    pub fn dense_search_wire(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_search_dense_wire(h, input, out)
        })
    }

    /// Executes a packed text-match wire search request.
    pub fn text_match_wire(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_search_text_match_wire(h, input, out)
        })
    }

    /// Executes a packed text-term wire search request.
    pub fn text_term_wire(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_search_text_term_wire(h, input, out)
        })
    }

    /// Executes a packed text-match-phrase wire search request.
    pub fn text_match_phrase_wire(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_search_text_match_phrase_wire(h, input, out)
        })
    }

    /// Aggregates hits from a JSON request.
    pub fn aggregate_hits_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_aggregate_hits_json(h, input, out)
        })
    }

    /// Looks up an artifact by base64 artifact ID.
    pub fn lookup_artifact_json(&self, artifact_id_base64: &str) -> Result<Vec<u8>> {
        self.with_input_output(artifact_id_base64.as_bytes(), |h, input, out| unsafe {
            sys::antfly_db_lookup_artifact_json(h, input, out)
        })
    }

    pub fn extract_enrichments_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_extract_enrichments_json(h, input, out)
        })
    }

    pub fn compute_enrichments_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_compute_enrichments_json(h, input, out)
        })
    }

    // -- Graph ---------------------------------------------------------------

    /// Returns graph edges for `key` from a graph index. `edge_type` of
    /// `""` matches every edge type.
    pub fn edges_json(
        &self,
        index_name: &str,
        key: &str,
        edge_type: &str,
        direction: GraphDirection,
    ) -> Result<Vec<u8>> {
        self.graph_lookup(
            index_name,
            key,
            edge_type,
            direction.as_u8(),
            |h, i, k, e, d, out| unsafe { sys::antfly_db_get_edges_json(h, i, k, e, d, out) },
        )
    }

    pub fn traverse_edges_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_traverse_edges_json(h, input, out)
        })
    }

    pub fn execute_graph_queries_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_execute_graph_queries_json(h, input, out)
        })
    }

    /// Returns graph neighbors for `key` from a graph index. `edge_type` of
    /// `""` matches every edge type.
    pub fn neighbors_json(
        &self,
        index_name: &str,
        key: &str,
        edge_type: &str,
        direction: GraphDirection,
    ) -> Result<Vec<u8>> {
        self.graph_lookup(
            index_name,
            key,
            edge_type,
            direction.as_u8(),
            |h, i, k, e, d, out| unsafe { sys::antfly_db_get_neighbors_json(h, i, k, e, d, out) },
        )
    }

    pub fn find_shortest_path_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_find_shortest_path_json(h, input, out)
        })
    }

    pub fn find_k_shortest_paths_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_find_k_shortest_paths_json(h, input, out)
        })
    }

    pub fn match_pattern_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.with_input_output(request.as_ref(), |h, input, out| unsafe {
            sys::antfly_db_match_pattern_json(h, input, out)
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.close_inner();
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("open", &self.gate.is_open())
            .finish()
    }
}

/// Decodes a base64 artifact ID without opening a database.
pub fn decode_artifact_id_json(artifact_id_base64: &str) -> Result<Vec<u8>> {
    let mut out = antfly_buffer::default();
    check(unsafe {
        sys::antfly_db_decode_artifact_id_json(
            borrow_slice(artifact_id_base64.as_bytes()),
            &mut out,
        )
    })?;
    Ok(unsafe { take_buffer(out) })
}
