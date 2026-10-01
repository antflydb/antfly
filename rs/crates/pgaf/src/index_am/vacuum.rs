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

use pgrx::pg_sys;

/// No-op for v1: a remote index has no local pages to clean up.
/// Stale documents in Antfly are harmless — PostgreSQL's heap visibility
/// checks filter out ctids pointing to dead or reused tuples.
#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    if stats.is_null() {
        return unsafe {
            pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>())
                as *mut pg_sys::IndexBulkDeleteResult
        };
    }
    stats
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    if stats.is_null() {
        return unsafe {
            pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>())
                as *mut pg_sys::IndexBulkDeleteResult
        };
    }
    stats
}
