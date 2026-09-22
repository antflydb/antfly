"""Concurrency and threading-contract tests, mirroring
go/pkg/lite/concurrency_cgo_test.go.

libantfly is ANTFLY_THREADING_SERIALIZED: any thread may call any function
on a handle concurrently. Writes on one handle queue behind each other
instead of failing with Busy; Close waits for in-flight calls; calls after
Close raise InvalidArgumentError.
"""

from __future__ import annotations

import queue
import threading
import time
from pathlib import Path

import pytest

import antfly_lite

pytestmark = pytest.mark.usefixtures("require_native")


def test_threading_mode_is_serialized(tmp_path: Path) -> None:
    assert antfly_lite.threading_mode() == antfly_lite.THREADING_SERIALIZED
    with antfly_lite.create(tmp_path / "threading.aflite", no_sync=True) as db:
        caps = db.capabilities()
        assert caps["threading"] == "serialized"


def test_concurrent_calls_on_one_handle(tmp_path: Path) -> None:
    db = antfly_lite.create(tmp_path / "concurrent.aflite", no_sync=True)
    try:
        writers, writes_per_writer, readers = 4, 25, 4
        errors_q: queue.Queue[str] = queue.Queue()
        stop = threading.Event()
        counter_lock = threading.Lock()
        counter = {"value": 0}

        def next_timestamp() -> int:
            with counter_lock:
                counter["value"] += 1
                return counter["value"]

        def run_writer(w: int) -> None:
            for i in range(writes_per_writer):
                key = f"doc:w{w}:{i}"
                value = f'{{"body":"concurrent writer {w} item {i}"}}'.encode()
                try:
                    db.batch([antfly_lite.WriteIntent(key=key, value=value)], next_timestamp())
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"batch {key}: {exc!r}")
                    return

        search_request = {"full_text_search": {"match": {"field": "body", "text": "concurrent writer"}}, "limit": 5}
        scan_request = {"from": "doc:w", "to": "doc:x", "include_documents": True, "limit": 20}

        def run_reader() -> None:
            while not stop.is_set():
                try:
                    db.search(search_request)
                    db.stats()
                    db.scan(scan_request)
                    try:
                        db.lookup("doc:w0:0")
                    except antfly_lite.NotFoundError:
                        pass
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"reader: {exc!r}")
                    return

        def run_maintainer() -> None:
            while not stop.is_set():
                try:
                    db.run_until_idle()
                    db.delete_index("no_such_index")
                except Exception as exc:  # noqa: BLE001
                    errors_q.put(f"maintainer: {exc!r}")
                    return

        writer_threads = [threading.Thread(target=run_writer, args=(w,)) for w in range(writers)]
        reader_threads = [threading.Thread(target=run_reader) for _ in range(readers)]
        maintainer_thread = threading.Thread(target=run_maintainer)

        for t in writer_threads:
            t.start()
        for t in reader_threads:
            t.start()
        maintainer_thread.start()

        for t in writer_threads:
            t.join()
        stop.set()
        for t in reader_threads:
            t.join()
        maintainer_thread.join()

        collected = []
        while not errors_q.empty():
            collected.append(errors_q.get())
        assert not collected, "\n".join(collected)

        db.run_until_idle()
        for w in range(writers):
            for i in range(writes_per_writer):
                key = f"doc:w{w}:{i}"
                got = db.lookup(key, raw=True)
                assert f"item {i}".encode() in got, f"lookup {key} = {got!r}"
    finally:
        db.close()


def test_close_races_in_flight_calls(tmp_path: Path) -> None:
    db = antfly_lite.create(tmp_path / "close-race.aflite", no_sync=True)
    db.batch([antfly_lite.WriteIntent(key="doc:close", value=b'{"body":"close race"}')], 1)

    closed_seen = {"count": 0}
    seen_lock = threading.Lock()
    unexpected: list[str] = []

    def run_reader() -> None:
        while True:
            try:
                db.lookup("doc:close")
            except antfly_lite.InvalidArgumentError:
                with seen_lock:
                    closed_seen["count"] += 1
                return
            except Exception as exc:  # noqa: BLE001
                unexpected.append(repr(exc))
                return

    reader_threads = [threading.Thread(target=run_reader) for _ in range(8)]
    for t in reader_threads:
        t.start()
    time.sleep(0.02)

    closer_threads = [threading.Thread(target=db.close) for _ in range(3)]
    for t in closer_threads:
        t.start()
    for t in closer_threads:
        t.join()
    for t in reader_threads:
        t.join()

    assert not unexpected, "\n".join(unexpected)
    assert closed_seen["count"] == 8, f"{closed_seen['count']} of 8 readers observed the closed handle"
    with pytest.raises(antfly_lite.InvalidArgumentError):
        db.stats()


def test_busy_timeout_waits_for_writer_lock(tmp_path: Path) -> None:
    path = tmp_path / "busy-timeout.aflite"
    first = antfly_lite.create(path, no_sync=True)
    try:
        # Without a timeout the second writer fails immediately.
        with pytest.raises(antfly_lite.BusyError):
            antfly_lite.open(path, no_sync=True)

        # With a short timeout it fails with Busy only after waiting.
        start = time.monotonic()
        with pytest.raises(antfly_lite.BusyError):
            antfly_lite.open(path, no_sync=True, busy_timeout=0.15)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.14, f"busy timeout returned after {elapsed:.3f}s, want about 0.15s"

        # With a longer timeout it succeeds once the first writer closes.
        def close_first_later() -> None:
            time.sleep(0.1)
            first.close()

        closer = threading.Thread(target=close_first_later)
        closer.start()
        second = antfly_lite.open(path, no_sync=True, busy_timeout=10.0)
        closer.join()
        second.close()
    finally:
        first.close()
