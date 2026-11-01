// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Stable Antfly C ABI error codes.
//!
//! This mirrors `go/pkg/embedded/errors.go`: names and descriptions are
//! hand-maintained Rust values, not sourced from `antfly_error_code_name`/
//! `antfly_error_code_description` at runtime, so `Error` is usable (and its
//! `Display` output is meaningful) even when this crate is built without the
//! `libantfly` feature and no dylib is linked. `tests/errors.rs` (linked)
//! cross-checks these against the live C ABI.

use std::fmt;

/// A stable Antfly C ABI error code, mapped onto a typed Rust enum.
///
/// `ANTFLY_OK` (0) is success and is never represented by `Error`; every
/// fallible call in this crate returns `Result<T, Error> = Result<T>`.
#[derive(Debug, Clone)]
pub enum Error {
    /// Owned diagnostic from a native call. Use `code()`/`kind()` to classify.
    Native { code: i32, detail: String },
    /// An argument, request, path, or open mode is invalid.
    InvalidArgument,
    /// The requested database object was not found.
    NotFound,
    /// A version predicate did not match the current document version.
    VersionConflict,
    /// A transaction intent conflicts with the requested operation.
    IntentConflict,
    /// The requested transaction was not found.
    TxnNotFound,
    /// The requested resource is temporarily busy or changed during
    /// streaming; stabilize it and retry.
    Busy,
    /// The operation was published, but crash durability could not be
    /// confirmed. Inspect the destination; do not retry automatically.
    OutcomeUnknown,
    /// The operation requires a capability that is not supported by this
    /// platform or filesystem. Retrying unchanged will not succeed.
    Unsupported,
    /// Derived work stalled or a derived worker failed. Inspect pending-work
    /// diagnostics and repair or reopen the database. A batch may already
    /// have committed: inspect its error detail and do not replay a batch
    /// reported as committed.
    Stalled,
    /// The operation was cancelled, including a pending replacement or a
    /// progress/stream callback returning `false`, or an IO interruption.
    Cancelled,
    /// A replacement exhausted its consecutive or lifetime retry budget,
    /// or encountered a permanent error. Inspect its pending-work diagnostics.
    ReplacementFailed,
    /// The requested operation conflicts with a pending replacement.
    ReplacementPending,
    /// The requested configuration differs from the pending replacement.
    ReplacementConfigurationMismatch,
    /// An internal error occurred.
    Internal,
    /// A C ABI error code this binding does not recognize, carrying the raw
    /// numeric value.
    Unknown(i32),
}

impl Error {
    /// Stable category, independent of the owned diagnostic.
    pub fn kind(&self) -> Error {
        Error::from_code(self.code())
    }
    pub fn detail(&self) -> Option<&str> {
        match self {
            Error::Native { detail, .. } => Some(detail),
            _ => None,
        }
    }
    pub(crate) fn from_native(code: i32) -> Error {
        #[cfg(feature = "libantfly")]
        if unsafe { antfly_embedded_sys::antfly_last_error_code() } == code {
            let detail = last_error_detail();
            if !detail.is_empty() {
                return Error::Native { code, detail };
            }
        }
        Error::from_code(code)
    }

    /// Maps a raw `antfly_error_code` to a typed `Error`. Callers should
    /// treat `ANTFLY_OK` (0) as success, not as an `Error`; this function
    /// will happily map it to `Error::Unknown(0)` if asked.
    pub fn from_code(code: i32) -> Error {
        match code {
            1 => Error::InvalidArgument,
            2 => Error::NotFound,
            3 => Error::VersionConflict,
            4 => Error::IntentConflict,
            5 => Error::TxnNotFound,
            6 => Error::Busy,
            7 => Error::OutcomeUnknown,
            8 => Error::Unsupported,
            9 => Error::Stalled,
            11 => Error::ReplacementFailed,
            12 => Error::ReplacementPending,
            13 => Error::ReplacementConfigurationMismatch,
            10 => Error::Cancelled,
            255 => Error::Internal,
            other => Error::Unknown(other),
        }
    }

    /// The raw numeric `antfly_error_code` this value represents.
    pub fn code(&self) -> i32 {
        match self {
            Error::Native { code, .. } => *code,
            Error::InvalidArgument => 1,
            Error::NotFound => 2,
            Error::VersionConflict => 3,
            Error::IntentConflict => 4,
            Error::TxnNotFound => 5,
            Error::Busy => 6,
            Error::OutcomeUnknown => 7,
            Error::Unsupported => 8,
            Error::Stalled => 9,
            Error::ReplacementFailed => 11,
            Error::ReplacementPending => 12,
            Error::ReplacementConfigurationMismatch => 13,
            Error::Cancelled => 10,
            Error::Internal => 255,
            Error::Unknown(code) => *code,
        }
    }

    /// The stable symbolic C ABI name for this error, e.g. `"ANTFLY_BUSY"`.
    pub fn name(&self) -> &'static str {
        match self {
            Error::InvalidArgument => "ANTFLY_INVALID_ARGUMENT",
            Error::NotFound => "ANTFLY_NOT_FOUND",
            Error::VersionConflict => "ANTFLY_VERSION_CONFLICT",
            Error::IntentConflict => "ANTFLY_INTENT_CONFLICT",
            Error::TxnNotFound => "ANTFLY_TXN_NOT_FOUND",
            Error::Busy => "ANTFLY_BUSY",
            Error::OutcomeUnknown => "ANTFLY_OUTCOME_UNKNOWN",
            Error::Unsupported => "ANTFLY_UNSUPPORTED",
            Error::Stalled => "ANTFLY_STALLED",
            Error::ReplacementFailed => "ANTFLY_REPLACEMENT_FAILED",
            Error::ReplacementPending => "ANTFLY_REPLACEMENT_PENDING",
            Error::ReplacementConfigurationMismatch => "ANTFLY_REPLACEMENT_CONFIGURATION_MISMATCH",
            Error::Cancelled => "ANTFLY_CANCELLED",
            Error::Internal => "ANTFLY_INTERNAL",
            Error::Unknown(_) => "ANTFLY_UNKNOWN_ERROR",
            Error::Native { code, .. } => Error::from_code(*code).name(),
        }
    }

    /// A short, stable description for this error.
    pub fn description(&self) -> &'static str {
        match self {
            Error::InvalidArgument => "an argument, request, path, or open mode is invalid",
            Error::NotFound => "the requested database object was not found",
            Error::VersionConflict => {
                "a version predicate did not match the current document version"
            }
            Error::IntentConflict => "a transaction intent conflicts with the requested operation",
            Error::TxnNotFound => "the requested transaction was not found",
            Error::Busy => {
                "the requested resource is temporarily busy or changed during streaming; \
                 stabilize it and retry"
            }
            Error::OutcomeUnknown => {
                "the operation was published, but crash durability could not be confirmed; \
                 inspect the destination and do not retry automatically"
            }
            Error::Unsupported => {
                "the operation requires a capability that is not supported by this platform \
                 or filesystem"
            }
            Error::Stalled => {
                "derived work stalled or failed; inspect pending-work diagnostics and \
                 repair or reopen the database"
            }
            Error::ReplacementFailed => {
                "replacement failed after its retry budget; inspect diagnostics and cancel before retrying"
            }
            Error::ReplacementPending => "index name is reserved by a pending replacement",
            Error::ReplacementConfigurationMismatch => {
                "replacement configuration differs from the reserved request"
            }
            Error::Cancelled => "the caller cancelled the operation",
            Error::Internal => "an internal error occurred",
            Error::Unknown(_) => "unknown Antfly error code",
            Error::Native { code, .. } => Error::from_code(*code).description(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name(), self.description())?;
        if let Some(detail) = self.detail() {
            write!(f, ": {detail}")?;
        }
        Ok(())
    }
}

// Preserve stable-code comparisons when native diagnostics are present.
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}
impl Eq for Error {}
impl std::hash::Hash for Error {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.code(), state);
    }
}

impl std::error::Error for Error {}

/// A `Result` whose error type is [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Copy the most recent native failure detail on the current thread.
/// Call immediately after a failed operation; later failures replace it.
#[cfg(feature = "libantfly")]
pub fn last_error_detail() -> String {
    unsafe {
        std::ffi::CStr::from_ptr(antfly_embedded_sys::antfly_last_error_detail())
            .to_string_lossy()
            .into_owned()
    }
}
