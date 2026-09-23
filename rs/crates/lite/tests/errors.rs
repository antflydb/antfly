//! Cross-checks `antfly-lite::Error`'s hard-coded names/descriptions against
//! the live `antfly_error_code_name`/`antfly_error_code_description` C ABI
//! functions, and checks ABI validation and struct sizes. Requires linking
//! against the real library (`--features libantfly`).

use std::ffi::CStr;

use antfly_lite::Error;
use antfly_lite_sys as sys;

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

/// Codes 0-9 plus 255, the full range antfly.h's `antfly_error_code` and the
/// Go binding's `ErrorCode` cover (including `ANTFLY_STALLED` = 9, which the
/// live library recognizes -- see the crate-level report for why the header
/// enum itself does not list it).
const ALL_CODES: &[i32] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255];

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
    antfly_lite::validate_abi().expect("loaded libantfly should match this binding's ABI");
    assert_eq!(
        antfly_lite::abi_version(),
        antfly_lite::SUPPORTED_ABI_VERSION
    );
}

#[test]
fn threading_mode_is_serialized() {
    assert_eq!(
        antfly_lite::threading_mode(),
        antfly_lite::THREADING_SERIALIZED
    );
    assert_eq!(
        antfly_lite::THREADING_SERIALIZED,
        sys::ANTFLY_THREADING_SERIALIZED
    );
}

#[test]
fn open_options_struct_sizes_match_library() {
    let want = antfly_lite::open_options_size() as usize;
    let got = std::mem::size_of::<sys::antfly_lite_open_options>();
    assert_eq!(got, want);
}
