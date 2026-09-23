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
