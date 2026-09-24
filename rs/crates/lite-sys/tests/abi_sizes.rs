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

//! Verifies the hand-written `#[repr(C)]` option structs match the loaded
//! `libantfly`'s notion of their size. Requires linking against the real
//! library (`--features libantfly`).
use antfly_lite_sys::{
    antfly_inference_options, antfly_inference_options_size, antfly_inference_pull_progress,
    antfly_open_options, antfly_open_options_size,
};

#[test]
fn open_options_size_matches_library() {
    let want = unsafe { antfly_open_options_size() } as usize;
    let got = std::mem::size_of::<antfly_open_options>();
    assert_eq!(
        got, want,
        "antfly_open_options size mismatch: Rust repr(C) says {got}, library says {want}"
    );
}

#[test]
fn inference_options_size_matches_library() {
    let want = unsafe { antfly_inference_options_size() } as usize;
    let got = std::mem::size_of::<antfly_inference_options>();
    assert_eq!(
        got, want,
        "antfly_inference_options size mismatch: Rust repr(C) says {got}, library says {want}"
    );
}

/// Pins every field offset in `antfly_inference_options` to what the header
/// documents, catching a reordered/misaligned field even when the total
/// size (checked above) happens to still match by coincidence.
#[test]
fn inference_options_field_offsets_match_header_order() {
    assert_eq!(std::mem::offset_of!(antfly_inference_options, abi_size), 0);
    assert_eq!(std::mem::offset_of!(antfly_inference_options, flags), 4);
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, models_dir),
        8
    );
    let host_budget_offset = std::mem::offset_of!(antfly_inference_options, host_budget_mb);
    assert_eq!(
        host_budget_offset,
        8 + std::mem::size_of::<antfly_lite_sys::antfly_slice>()
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, backend_budget_mb),
        host_budget_offset + 4
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, process_memory_budget_mb),
        host_budget_offset + 8
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, combined_budget_mb),
        host_budget_offset + 12
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, kv_budget_mb),
        host_budget_offset + 16
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, scratch_budget_mb),
        host_budget_offset + 20
    );
    // call_timeout_ms is u64: aligned up to the next 8-byte boundary after
    // the six u32 budget fields (host_budget_offset + 24, already a
    // multiple of 8 since host_budget_offset is 8-aligned and 24 is too).
    let call_timeout_offset = std::mem::offset_of!(antfly_inference_options, call_timeout_ms);
    assert_eq!(call_timeout_offset, host_budget_offset + 24);
    assert_eq!(
        std::mem::offset_of!(antfly_inference_options, reserved),
        call_timeout_offset + 8
    );
}

/// No library-reported size function exists for `antfly_inference_pull_progress`
/// (it is a callback-only struct, not an options struct with its own `_size`
/// accessor), so this pins the field offsets the crate compiles against
/// directly: a reordering in `zig/pkg/antfly/include/antfly.h` without a
/// matching Rust update would otherwise pass silently (the pull tests in
/// `antfly-lite` exercise the values, not the raw layout).
#[test]
fn inference_pull_progress_field_offsets_match_header_order() {
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, abi_size),
        0
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, reserved0),
        4
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, model),
        8
    );
    let file_offset = std::mem::offset_of!(antfly_inference_pull_progress, file);
    assert_eq!(
        file_offset,
        8 + std::mem::size_of::<antfly_lite_sys::antfly_slice>()
    );
    let bytes_downloaded_offset =
        std::mem::offset_of!(antfly_inference_pull_progress, bytes_downloaded);
    assert_eq!(
        bytes_downloaded_offset,
        file_offset + std::mem::size_of::<antfly_lite_sys::antfly_slice>()
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, total_bytes),
        bytes_downloaded_offset + 8
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, files_done),
        bytes_downloaded_offset + 16
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, files_total),
        bytes_downloaded_offset + 24
    );
    assert_eq!(
        std::mem::offset_of!(antfly_inference_pull_progress, cached),
        bytes_downloaded_offset + 32
    );
}
