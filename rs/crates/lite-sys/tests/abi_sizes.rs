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
    antfly_lite_open_options, antfly_lite_open_options_size, antfly_open_options,
    antfly_open_options_size,
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
fn lite_open_options_size_matches_library() {
    let want = unsafe { antfly_lite_open_options_size() } as usize;
    let got = std::mem::size_of::<antfly_lite_open_options>();
    assert_eq!(
        got, want,
        "antfly_lite_open_options size mismatch: Rust repr(C) says {got}, library says {want}"
    );
}
