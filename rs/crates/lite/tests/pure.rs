//! Pure (no-FFI) sanity checks for `antfly-lite`'s value types. Runs
//! without the `libantfly` feature and without any dylib present.

use std::time::Duration;

use antfly_lite::{Error, OpenMode, OpenOptions, Profile};

#[test]
fn error_names_and_descriptions_are_stable() {
    let cases: &[(Error, &str)] = &[
        (Error::InvalidArgument, "ANTFLY_INVALID_ARGUMENT"),
        (Error::NotFound, "ANTFLY_NOT_FOUND"),
        (Error::VersionConflict, "ANTFLY_VERSION_CONFLICT"),
        (Error::IntentConflict, "ANTFLY_INTENT_CONFLICT"),
        (Error::TxnNotFound, "ANTFLY_TXN_NOT_FOUND"),
        (Error::Busy, "ANTFLY_BUSY"),
        (Error::OutcomeUnknown, "ANTFLY_OUTCOME_UNKNOWN"),
        (Error::Unsupported, "ANTFLY_UNSUPPORTED"),
        (Error::Stalled, "ANTFLY_STALLED"),
        (Error::Internal, "ANTFLY_INTERNAL"),
        (Error::Unknown(12345), "ANTFLY_UNKNOWN_ERROR"),
    ];
    for (err, name) in cases {
        assert_eq!(err.name(), *name);
        assert!(!err.description().is_empty());
        assert_eq!(
            err.to_string(),
            format!("{}: {}", err.name(), err.description())
        );
    }
}

#[test]
fn error_code_round_trips() {
    assert_eq!(Error::Busy.code(), 6);
    assert_eq!(Error::Internal.code(), 255);
    assert_eq!(Error::Stalled.code(), 9);
    assert_eq!(Error::Unknown(42).code(), 42);
}

#[test]
fn open_options_default_is_writer_native() {
    let opts = OpenOptions::default();
    assert_eq!(opts.mode, OpenMode::Writer);
    assert_eq!(opts.profile, Profile::Native);
    assert!(opts.busy_timeout.is_none());
    assert_eq!(opts.map_size, 0);
}

#[test]
fn open_options_builder_sets_fields() {
    let opts = OpenOptions::new()
        .mode(OpenMode::Readonly)
        .profile(Profile::Hosted)
        .no_sync(true)
        .busy_timeout(Duration::from_millis(150));
    assert_eq!(opts.mode, OpenMode::Readonly);
    assert_eq!(opts.profile, Profile::Hosted);
    assert!(opts.no_sync);
    assert_eq!(opts.busy_timeout, Some(Duration::from_millis(150)));
}

#[test]
fn busy_timeout_rounds_up_to_whole_milliseconds() {
    let opts = OpenOptions::new().busy_timeout(Duration::from_micros(1500));
    // 1.5ms should round up to 2ms, matching the Go binding's
    // `(timeout + time.Millisecond - 1) / time.Millisecond`.
    assert_eq!(opts.busy_timeout_ms(), 2);

    let exact = OpenOptions::new().busy_timeout(Duration::from_millis(150));
    assert_eq!(exact.busy_timeout_ms(), 150);

    let zero = OpenOptions::new().busy_timeout(Duration::ZERO);
    assert_eq!(zero.busy_timeout_ms(), 0);

    let none = OpenOptions::new();
    assert_eq!(none.busy_timeout_ms(), 0);
}

#[test]
fn database_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<antfly_lite::Database>();
}
