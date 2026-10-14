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

//! Cross-checks `antfly-embedded::Error`'s hard-coded names/descriptions against
//! the live `antfly_error_code_name`/`antfly_error_code_description` C ABI
//! functions, and checks ABI validation and struct sizes. Requires linking
//! against the real library (`--features libantfly`).

use std::ffi::CStr;

use antfly_embedded::Error;
use antfly_embedded_sys as sys;

fn c_name(code: i32) -> String {
    unsafe { CStr::from_ptr(sys::antfly_error_code_name(code)) }
        .to_string_lossy()
        .into_owned()
}

fn c_description(code: i32) -> String {
    unsafe { CStr::from_ptr(sys::antfly_error_code_description(code)) }
        .to_string_lossy()
        .into_owned()
}

/// Codes 0-10 plus 255, the full range antfly.h's `antfly_error_code` and
/// the Go binding's `ErrorCode` cover (`ANTFLY_CANCELLED` = 10 is the
/// caller-cancelled-a-callback code used by `Inference::pull`/
/// `Inference::generate_stream`).
const ALL_CODES: &[i32] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 255];

#[test]
fn error_names_match_c_abi_for_every_known_code() {
    for &code in ALL_CODES {
        let want = c_name(code);
        if code == 0 {
            assert_eq!(want, "ANTFLY_OK");
            continue;
        }
        let err = Error::from_code(code);
        assert_eq!(
            err.name(),
            want,
            "code {code}: Rust name {:?} != C name {want:?}",
            err.name()
        );
    }
}

#[test]
fn error_descriptions_match_c_abi_for_every_known_code() {
    for &code in ALL_CODES {
        if code == 0 {
            continue;
        }
        let want = c_description(code);
        let err = Error::from_code(code);
        assert_eq!(
            err.description(),
            want,
            "code {code}: Rust description {:?} != C description {want:?}",
            err.description()
        );
    }
}

#[test]
fn unknown_code_matches_c_abi_unknown_error_text() {
    let unknown = 12345;
    assert_eq!(c_name(unknown), "ANTFLY_UNKNOWN_ERROR");
    assert_eq!(c_description(unknown), "unknown Antfly error code");
    let err = Error::from_code(unknown);
    assert_eq!(err.name(), "ANTFLY_UNKNOWN_ERROR");
    assert_eq!(err.description(), "unknown Antfly error code");
    assert_eq!(err.code(), unknown);
}

#[test]
fn validate_abi_succeeds_against_the_loaded_library() {
    antfly_embedded::validate_abi().expect("loaded libantfly should match this binding's ABI");
    assert_eq!(
        antfly_embedded::abi_version(),
        antfly_embedded::SUPPORTED_ABI_VERSION
    );
}

#[test]
fn threading_mode_is_serialized() {
    assert_eq!(
        antfly_embedded::threading_mode(),
        antfly_embedded::THREADING_SERIALIZED
    );
    assert_eq!(
        antfly_embedded::THREADING_SERIALIZED,
        sys::ANTFLY_THREADING_SERIALIZED
    );
}

#[test]
fn open_options_struct_sizes_match_library() {
    let want = antfly_embedded::open_options_size() as usize;
    let got = std::mem::size_of::<sys::antfly_open_options>();
    assert_eq!(got, want);
}

#[test]
fn native_diagnostics_are_owned_and_sql_cannot_reuse_same_code_detail() {
    std::thread::Builder::new()
        .stack_size(antfly_embedded::MIN_THREAD_STACK_SIZE)
        .spawn(|| {
            use antfly_embedded::Database;
            let path = std::env::temp_dir().join(format!(
                "antfly-native-errors-{}.aflite",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);
            let db = Database::create_hosted(&path).unwrap();
            db.create_table_json("entities", b"{}").unwrap();
            let table = db.open_table("entities").unwrap();
            let index_error = db.add_index_json(br#"{"name":"invalid"}"#).unwrap_err();
            assert_eq!(index_error.kind(), Error::InvalidArgument);
            assert!(index_error.detail().unwrap().contains("MissingIndexType"));
            let sql_error = table.sql_session_open().unwrap_err();
            assert_eq!(sql_error.kind(), Error::InvalidArgument);
            assert!(!sql_error.detail().unwrap().contains("MissingIndexType"));
            assert!(sql_error.detail().unwrap().contains("root database handle"));
            assert!(index_error.detail().unwrap().contains("MissingIndexType"));
            drop(table);
            drop(db);
            std::fs::remove_file(path).unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}
