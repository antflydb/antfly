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

//! Internal FFI plumbing shared by `db.rs`, `transactions.rs`, and the free
//! functions in `maintenance.rs`/`files.rs`.

use std::ffi::CString;
use std::path::Path;

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
