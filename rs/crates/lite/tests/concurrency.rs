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

//! Concurrency behavior over a single shared handle: writes queue rather
//! than fail with `Busy`, reads run alongside writes and admin calls, close
//! waits for in-flight calls, and the busy-timeout retry policy behaves like
//! `sqlite3_busy_timeout`. Mirrors
//! `go/pkg/lite/concurrency_cgo_test.go`. Requires linking against the real
//! library (`--features libantfly`).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use antfly_lite::{Database, Error, OpenOptions};

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "antfly-lite-concurrency-{tag}-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn no_sync_opts() -> OpenOptions {
    OpenOptions::new().no_sync(true)
}

/// Spawns with exactly `MIN_THREAD_STACK_SIZE`, the documented minimum for
/// threads calling libantfly (see `ANTFLY_MIN_THREAD_STACK_SIZE` in
/// antfly.h). Rust's default spawned-thread stack (2 MiB) and the test
/// harness's are too small; running at the minimum also turns any stack
/// growth in libantfly into a test failure.
fn spawn_with_stack<F, T>(f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .stack_size(antfly_lite::MIN_THREAD_STACK_SIZE)
        .spawn(f)
        .expect("spawn thread")
}

/// Runs `f` on a large-stack thread and joins it, so that the `#[test]`
/// function's own (small, harness-provided) stack is never the one under
/// pressure. See [`spawn_with_stack`].
fn run_with_stack<F: FnOnce() + Send + 'static>(f: F) {
    spawn_with_stack(f)
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}

#[test]
fn database_is_send_and_sync_compile_time() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Database>();
}

#[test]
fn threading_mode_and_capabilities_report_serialized() {
    run_with_stack(|| {
        assert_eq!(
            antfly_lite::threading_mode(),
            antfly_lite::THREADING_SERIALIZED
        );

        let dir = tmp_dir("threading");
        let db = Database::create(dir.join("threading.aflite"), &no_sync_opts()).expect("create");
        let caps = db.capabilities().expect("capabilities");
        assert_eq!(caps.threading, "serialized");
        db.close().expect("close");
    });
}

/// One handle shared by threads doing writes, reads, drains, and exclusive
/// schema calls at once. Writes must queue rather than fail with `Busy`,
/// and every write must be visible afterwards.
#[test]
fn concurrent_calls_on_one_handle() {
    run_with_stack(concurrent_calls_on_one_handle_inner);
}

fn concurrent_calls_on_one_handle_inner() {
    let dir = tmp_dir("concurrent");
    let db =
        Arc::new(Database::create(dir.join("concurrent.aflite"), &no_sync_opts()).expect("create"));

    const WRITERS: usize = 4;
    const WRITES_PER_WRITER: usize = 25;
    const READERS: usize = 4;

    let timestamp = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        let timestamp = Arc::clone(&timestamp);
        handles.push(spawn_with_stack(move || {
            for i in 0..WRITES_PER_WRITER {
                let key = format!("doc:w{w}:{i}");
                let value = format!(r#"{{"body":"concurrent writer {w} item {i}"}}"#);
                let ts = timestamp.fetch_add(1, Ordering::SeqCst) + 1;
                db.batch(&[antfly_lite::WriteIntent::put(key, value)], ts)
                    .unwrap_or_else(|e| panic!("batch: {e}"));
            }
        }));
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let search_request =
        br#"{"full_text_search":{"match":{"field":"body","text":"concurrent writer"}},"limit":5}"#;
    let scan_request = br#"{"from":"doc:w","to":"doc:x","include_documents":true,"limit":20}"#;

    let mut reader_handles = Vec::new();
    for _ in 0..READERS {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        reader_handles.push(spawn_with_stack(move || {
            while !stop.load(Ordering::SeqCst) {
                db.search_json(search_request)
                    .unwrap_or_else(|e| panic!("search: {e}"));
                db.stats_json().unwrap_or_else(|e| panic!("stats: {e}"));
                db.scan_json(scan_request)
                    .unwrap_or_else(|e| panic!("scan: {e}"));
                match db.lookup_json("doc:w0:0") {
                    Ok(_) | Err(Error::NotFound) => {}
                    Err(e) => panic!("lookup: {e}"),
                }
            }
        }));
    }

    {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        reader_handles.push(spawn_with_stack(move || {
            while !stop.load(Ordering::SeqCst) {
                db.run_until_idle()
                    .unwrap_or_else(|e| panic!("run until idle: {e}"));
                db.delete_index("no_such_index")
                    .unwrap_or_else(|e| panic!("delete missing index: {e}"));
            }
        }));
    }

    for h in handles {
        h.join().expect("writer thread panicked");
    }
    stop.store(true, Ordering::SeqCst);
    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }

    db.run_until_idle().expect("final run until idle");
    for w in 0..WRITERS {
        for i in 0..WRITES_PER_WRITER {
            let key = format!("doc:w{w}:{i}");
            let got = db
                .lookup_json(&key)
                .unwrap_or_else(|e| panic!("lookup {key} after concurrent writes: {e}"));
            let got = String::from_utf8_lossy(&got);
            assert!(got.contains(&format!("item {i}")), "lookup {key} = {got}");
        }
    }

    db.close().expect("close");
}

/// Close must wait for in-flight calls, and calls racing or following it
/// must fail cleanly rather than touch a freed handle. `close` takes
/// `&self` precisely so this works through a shared `Arc<Database>`, like
/// the Go binding calling `Close` concurrently on a shared `*DB`.
#[test]
fn close_races_in_flight_calls() {
    run_with_stack(close_races_in_flight_calls_inner);
}

fn close_races_in_flight_calls_inner() {
    let dir = tmp_dir("close-race");
    let db = Database::create(dir.join("close-race.aflite"), &no_sync_opts()).expect("create");
    db.batch(
        &[antfly_lite::WriteIntent::put(
            "doc:close",
            r#"{"body":"close race"}"#,
        )],
        1,
    )
    .expect("batch");
    let db = Arc::new(db);

    let closed_seen = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let db = Arc::clone(&db);
        let closed_seen = Arc::clone(&closed_seen);
        readers.push(spawn_with_stack(move || {
            loop {
                match db.lookup_json("doc:close") {
                    Ok(_) => {}
                    Err(Error::InvalidArgument) => {
                        closed_seen.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                    Err(e) => panic!("lookup during close: {e}"),
                }
            }
        }));
    }

    std::thread::sleep(Duration::from_millis(20));

    let mut closers = Vec::new();
    for _ in 0..3 {
        let db = Arc::clone(&db);
        closers.push(spawn_with_stack(move || db.close().expect("close")));
    }
    for h in closers {
        h.join().expect("closer thread panicked");
    }
    for h in readers {
        h.join().expect("reader thread panicked");
    }
    assert_eq!(closed_seen.load(Ordering::SeqCst), 8);
    assert!(matches!(db.status_json(), Err(Error::InvalidArgument)));
}

#[test]
fn busy_timeout_waits_for_writer_lock() {
    run_with_stack(busy_timeout_waits_for_writer_lock_inner);
}

fn busy_timeout_waits_for_writer_lock_inner() {
    let dir = tmp_dir("busy-timeout");
    let path = dir.join("busy-timeout.aflite");
    let first = Database::create(&path, &no_sync_opts()).expect("create");

    // Without a timeout the second writer fails immediately.
    match Database::open(&path, &no_sync_opts()) {
        Err(Error::Busy) => {}
        other => panic!("second writer without timeout = {other:?}, want Busy"),
    }

    // With a short timeout it fails with Busy only after waiting.
    let start = Instant::now();
    let short_timeout = no_sync_opts().busy_timeout(Duration::from_millis(150));
    match Database::open(&path, &short_timeout) {
        Err(Error::Busy) => {}
        other => panic!("second writer with short timeout = {other:?}, want Busy"),
    }
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(140),
        "busy timeout returned after {waited:?}, want about 150ms"
    );

    // With a longer timeout it succeeds once the first writer closes.
    let closer = spawn_with_stack(move || {
        std::thread::sleep(Duration::from_millis(100));
        first.close().expect("close first writer");
    });
    let long_timeout = no_sync_opts().busy_timeout(Duration::from_secs(10));
    let second = Database::open(&path, &long_timeout).expect("second writer after first closed");
    second.close().expect("close second writer");
    closer.join().expect("closer thread panicked");
}
