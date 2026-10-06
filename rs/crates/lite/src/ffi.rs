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

//! Internal FFI plumbing shared by `db.rs`, `inference.rs`, `transactions.rs`,
//! and the free functions in `maintenance.rs`/`files.rs`.

use std::ffi::CString;
use std::path::Path;
use std::sync::{Condvar, Mutex, PoisonError};

use antfly_lite_sys::{ANTFLY_OK, antfly_buffer, antfly_error_code, antfly_slice};

use crate::error::{Error, Result};

/// Borrows `bytes` as an `antfly_slice`. The returned slice is only valid
/// for the lifetime of `bytes`; libantfly's contract is that `antfly_slice`
/// is borrowed input the callee must not retain past the call.
pub(crate) fn borrow_slice(bytes: &[u8]) -> antfly_slice {
    if bytes.is_empty() {
        antfly_slice {
            ptr: std::ptr::null(),
            len: 0,
        }
    } else {
        antfly_slice {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
}

/// Reports whether `path`'s raw bytes end with `suffix`, matching Go's
/// `strings.HasSuffix(path, suffix)` exactly (unlike `Path::extension()`,
/// which treats a leading-dot filename like `.aflite` as having no
/// extension).
pub(crate) fn path_has_suffix(path: &Path, suffix: &str) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().ends_with(suffix.as_bytes())
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().ends_with(suffix)
    }
}

/// Converts a filesystem path to a NUL-terminated C string, preserving raw
/// bytes on Unix (matching Go's `C.CString`, which does not require valid
/// UTF-8 paths).
#[cfg(unix)]
pub(crate) fn path_to_cstring(path: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).map_err(|_| Error::InvalidArgument)
}

#[cfg(not(unix))]
pub(crate) fn path_to_cstring(path: &Path) -> Result<CString> {
    let s = path.to_str().ok_or(Error::InvalidArgument)?;
    CString::new(s).map_err(|_| Error::InvalidArgument)
}

/// Copies an owned `antfly_buffer` into a `Vec<u8>` and releases it with
/// `antfly_buffer_free`, matching the ABI contract that returned buffers are
/// caller-owned until freed.
///
/// # Safety
/// `buffer` must be a buffer that was actually populated by a successful
/// libantfly call (or left as `{NULL, 0}` by one), never a buffer this
/// process did not receive ownership of.
pub(crate) unsafe fn take_buffer(mut buffer: antfly_buffer) -> Vec<u8> {
    let out = if buffer.ptr.is_null() || buffer.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) }.to_vec()
    };
    unsafe { antfly_lite_sys::antfly_buffer_free(&mut buffer) };
    out
}

/// Maps a raw `antfly_error_code` to `Result<()>`, per the ABI convention
/// that `ANTFLY_OK` (0) is the only success value.
pub(crate) fn check(code: antfly_error_code) -> Result<()> {
    if code == ANTFLY_OK {
        Ok(())
    } else {
        Err(Error::from_code(code))
    }
}

/// Converts a byte-oriented path to raw bytes for an `antfly_slice` (not a
/// NUL-terminated `CString` -- some C ABI fields, like
/// `antfly_inference_options.models_dir`, take a path as a borrowed slice
/// rather than a C string). Preserves raw bytes on Unix, like
/// [`path_to_cstring`]; embedded NUL bytes are fine for a slice, unlike a
/// C string.
pub(crate) fn path_to_bytes(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().into_owned().into_bytes()
    }
}

// ---------------------------------------------------------------------
// HandleGate<T> -- shared writer-preferring reader/writer gate for a raw
// C ABI handle, used by both `Database` (`*mut antfly_db`) and `Inference`
// (`*mut antfly_inference`).
// ---------------------------------------------------------------------

/// Shared mutable state behind [`HandleGate`]: the handle itself (`None`
/// once closed) plus the bookkeeping needed for a writer-preferring
/// reader/writer gate.
struct GateState<T> {
    handle: Option<*mut T>,
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
/// stream of readers can starve a writer indefinitely. libantfly's own
/// contract for both `antfly_db` and `antfly_inference` handles says close
/// "waits for every call that has already entered ... and then frees the
/// handle" -- a bounded wait, not a potentially-unbounded one. This gate
/// gives the same guarantee: once a close is requested, new calls block
/// until it completes (or observe the handle as already closed), while
/// already in-flight calls are allowed to finish normally.
///
/// Generic over the pointee type `T` (`antfly_db` or `antfly_inference`) so
/// both handle wrappers share one implementation instead of two hand-rolled
/// copies of the same synchronization logic.
pub(crate) struct HandleGate<T> {
    state: Mutex<GateState<T>>,
    cond: Condvar,
}

impl<T> HandleGate<T> {
    pub(crate) fn new(handle: *mut T) -> HandleGate<T> {
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
    /// if the handle has already been closed or a close is in progress.
    pub(crate) fn with_handle<R>(&self, f: impl FnOnce(*mut T) -> Result<R>) -> Result<R> {
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
        struct ReaderSlot<'a, T>(&'a HandleGate<T>);
        impl<T> Drop for ReaderSlot<'_, T> {
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
    pub(crate) fn close(&self, close_fn: impl FnOnce(*mut T)) {
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

    pub(crate) fn is_open(&self) -> bool {
        self.state
            .lock()
            .map(|guard| guard.handle.is_some())
            .unwrap_or(false)
    }
}
