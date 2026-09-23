# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs the shared libantfly conformance cases through the public
antfly_lite API.

See zig/pkg/antfly/capi-conformance/README.md for the case format. This is a
straight port of go/pkg/lite/conformance_cgo_test.go's semantics: every
runner (C, Go, Python, Rust) executes the same declarative cases so the
bindings stay behaviorally identical.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import antfly_lite

pytestmark = pytest.mark.usefixtures("require_native")

CASES_DIR = Path(__file__).resolve().parents[4] / "zig" / "pkg" / "antfly" / "capi-conformance" / "cases"
CASE_FILES = sorted(CASES_DIR.glob("*.json")) if CASES_DIR.is_dir() else []

_MODE_NAMES = {
    None: antfly_lite.OpenMode.WRITER,
    "": antfly_lite.OpenMode.WRITER,
    "writer": antfly_lite.OpenMode.WRITER,
    "readonly": antfly_lite.OpenMode.READONLY,
    "status_only": antfly_lite.OpenMode.STATUS_ONLY,
}
_PROFILE_NAMES = {
    None: antfly_lite.Profile.NATIVE,
    "": antfly_lite.Profile.NATIVE,
    "native": antfly_lite.Profile.NATIVE,
    "hosted": antfly_lite.Profile.HOSTED,
}
_STORAGE_NAMES = {
    None: antfly_lite.Storage.LITE,
    "": antfly_lite.Storage.LITE,
    "lite": antfly_lite.Storage.LITE,
    "directory": antfly_lite.Storage.DIRECTORY,
}
_STATUS_NAMES = {
    antfly_lite.TxnStatus.PENDING: "pending",
    antfly_lite.TxnStatus.COMMITTED: "committed",
    antfly_lite.TxnStatus.ABORTED: "aborted",
}


def _build_open_options(spec: dict) -> antfly_lite.OpenOptions:
    mode = spec.get("mode")
    if mode not in _MODE_NAMES:
        raise ValueError(f"unknown mode {mode!r}")
    profile = spec.get("profile")
    if profile not in _PROFILE_NAMES:
        raise ValueError(f"unknown profile {profile!r}")
    storage = spec.get("storage")
    if storage not in _STORAGE_NAMES:
        raise ValueError(f"unknown storage {storage!r}")
    busy_timeout_ms = spec.get("busy_timeout_ms") or 0
    return antfly_lite.OpenOptions(
        storage=_STORAGE_NAMES[storage],
        mode=_MODE_NAMES[mode],
        profile=_PROFILE_NAMES[profile],
        no_sync=bool(spec.get("no_sync", False)),
        busy_timeout=(busy_timeout_ms / 1000.0) if busy_timeout_ms else None,
    )


def _open_db(path: Path, spec: dict) -> antfly_lite.Database:
    opts = _build_open_options(spec)
    if spec.get("create"):
        return antfly_lite.create_with_options(path, opts)
    return antfly_lite.open_with_options(path, opts)


def _writes(raw_writes: list[dict]) -> list[antfly_lite.WriteIntent]:
    out = []
    for w in raw_writes:
        if w.get("delete"):
            out.append(antfly_lite.WriteIntent(key=w["key"], delete=True))
        else:
            out.append(
                antfly_lite.WriteIntent(key=w["key"], value=json.dumps(w.get("value"), separators=(",", ":")).encode())
            )
    return out


class ConformanceRunner:
    def __init__(self, tmp_path: Path) -> None:
        self.dir = tmp_path
        self.db: antfly_lite.Database | None = None
        self.path: Path | None = None
        self.backup: bytes | None = None

    def resolve_path(self, name: str | None) -> Path:
        return self.dir / (name or "db.aflite")

    def open_current(self, spec: dict) -> None:
        path = self.resolve_path(spec.get("path"))
        self.db = _open_db(path, spec)
        self.path = path

    def close_current(self) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None


def _execute_transaction(runner: ConformanceRunner, step: dict) -> Any:
    assert runner.db is not None
    db = runner.db
    txn_id = bytes.fromhex(step["txn_id"])
    if len(txn_id) != 16:
        raise ValueError(f"txn_id {step['txn_id']!r} must be 32 hex characters")
    op = step["op"]
    if op == "begin_transaction":
        db.begin_transaction(txn_id, step.get("timestamp", 0), None)
        return None
    if op == "write_transaction":
        db.write_transaction(txn_id, _writes(step.get("writes", [])))
        return None
    if op == "resolve_transaction":
        status_name = step.get("status")
        if status_name == "committed":
            status = antfly_lite.TxnStatus.COMMITTED
        elif status_name == "aborted":
            status = antfly_lite.TxnStatus.ABORTED
        else:
            raise ValueError(f"unknown status {status_name!r}")
        db.resolve_transaction(txn_id, status, step.get("commit_version", 0))
        return None
    if op == "transaction_status":
        return _STATUS_NAMES[db.transaction_status(txn_id)]
    return db.commit_version(txn_id)  # commit_version


def _execute(runner: ConformanceRunner, step: dict) -> Any:
    assert runner.db is not None
    db = runner.db
    op = step["op"]
    if op == "batch":
        db.batch(_writes(step.get("writes", [])), step.get("timestamp", 0))
        return None
    if op == "batch_json":
        return db.batch_json(step["request"], raw=True)
    if op == "lookup":
        return db.lookup(step["key"], raw=True)
    if op == "scan":
        return db.scan(step["request"], raw=True)
    if op == "search":
        return db.search(step["request"], raw=True)
    if op == "stats":
        return db.stats(raw=True)
    if op == "status":
        return db.status(raw=True)
    if op == "capabilities":
        return db.capabilities(raw=True)
    if op == "check":
        return db.check(raw=True)
    if op == "pending_work_stats":
        return db.pending_work_stats(raw=True)
    if op == "run_until_idle":
        db.run_until_idle()
        return None
    if op == "get_schema":
        return db.get_schema(raw=True)
    if op == "set_schema":
        db.set_schema(step["schema"])
        return None
    if op == "list_indexes":
        return db.list_indexes(raw=True)
    if op == "add_index":
        db.add_index(step["config"])
        return None
    if op == "delete_index":
        return db.delete_index(step["name"])
    if op == "get_edges":
        direction = antfly_lite.GraphDirection[step.get("direction", "out").upper()]
        return db.edges(step["index"], step["key"], step.get("edge_type", ""), direction, raw=True)
    if op == "list_enrichments":
        return db.list_enrichments(raw=True)
    if op == "add_enrichment":
        db.add_enrichment(step["config"])
        return None
    if op == "delete_enrichment":
        return db.delete_enrichment(step["kind"], step["name"])
    if op in ("begin_transaction", "write_transaction", "resolve_transaction", "transaction_status", "commit_version"):
        return _execute_transaction(runner, step)
    if op == "backup":
        runner.backup = db.backup()
        return None
    if op == "import_backup":
        db.import_backup(runner.backup or b"")
        return None
    if op == "restore_open":
        path = runner.resolve_path(step.get("path"))
        storage = step.get("storage")
        if storage not in _STORAGE_NAMES:
            raise ValueError(f"unknown storage {storage!r}")
        antfly_lite.restore(str(path), runner.backup or b"", storage=_STORAGE_NAMES[storage], replace=False)
        runner.close_current()
        runner.open_current(step)
        return None
    if op == "reopen":
        runner.close_current()
        spec = dict(step)
        if not spec.get("path"):
            assert runner.path is not None
            spec["path"] = runner.path.name
        runner.open_current(spec)
        return None
    if op == "open_second":
        second = _open_db(runner.resolve_path(step.get("path")), step)
        second.close()
        return None
    if op == "close":
        runner.close_current()
        return None
    raise ValueError(f"unknown op {op!r}")


def _result_text(result: Any) -> str:
    if isinstance(result, (bytes, bytearray)):
        return result.decode("utf-8", errors="replace")
    if result is None:
        return ""
    return json.dumps(result, separators=(",", ":"))


def _json_subset(want: Any, got: Any) -> bool:
    if isinstance(want, dict):
        if not isinstance(got, dict):
            return False
        return all(k in got and _json_subset(v, got[k]) for k, v in want.items())
    if isinstance(want, list):
        if not isinstance(got, list) or len(got) != len(want):
            return False
        return all(_json_subset(w, g) for w, g in zip(want, got))
    return want == got


def _check_result(result: Any, expect: dict) -> None:
    text = _result_text(result)
    if expect.get("json_subset") is not None:
        want = expect["json_subset"]
        got = json.loads(text) if text else None
        if not _json_subset(want, got):
            raise AssertionError(f"result {text} does not contain {want!r}")
    for needle in expect.get("contains") or []:
        if needle not in text:
            raise AssertionError(f"result {text} does not contain {needle!r}")
    for needle in expect.get("not_contains") or []:
        if needle in text:
            raise AssertionError(f"result {text} unexpectedly contains {needle!r}")
    if "equals" in expect:
        want = expect["equals"]
        got = json.loads(text) if text else None
        if want != got:
            raise AssertionError(f"result {got!r}, want {want!r}")


def _run_step(runner: ConformanceRunner, step: dict, index: int) -> None:
    try:
        result = _execute(runner, step)
        error: BaseException | None = None
    except Exception as exc:  # noqa: BLE001 - re-raised/inspected below
        result = None
        error = exc

    expect = step.get("expect")
    if expect is None:
        if error is not None:
            raise AssertionError(f"step {index} ({step['op']}): {error}") from error
        return

    want_error = expect.get("error")
    if want_error:
        if error is None:
            raise AssertionError(f"step {index} ({step['op']}): succeeded, want {want_error}")
        name = getattr(error, "name", None)
        if name is None:
            raise AssertionError(
                f"step {index} ({step['op']}): error {error!r} is not an AntflyError, want {want_error}"
            ) from error
        if name != want_error:
            raise AssertionError(f"step {index} ({step['op']}): error {name}, want {want_error}") from error
        return

    if error is not None:
        raise AssertionError(f"step {index} ({step['op']}): {error}") from error
    _check_result(result, expect)


@pytest.mark.parametrize("case_file", CASE_FILES, ids=lambda p: p.stem)
def test_conformance_case(case_file: Path, tmp_path: Path) -> None:
    case = json.loads(case_file.read_text())
    runner = ConformanceRunner(tmp_path)
    try:
        runner.open_current(case["open"])
        for index, step in enumerate(case.get("steps", [])):
            _run_step(runner, step, index)
    finally:
        runner.close_current()


def test_conformance_cases_found() -> None:
    assert CASE_FILES, f"no conformance cases found in {CASES_DIR}"


def test_restore_backup_into_directory_storage_and_reopen(tmp_path: Path) -> None:
    """A .aflite backup restores into directory storage (not just another
    .aflite file), and the resulting directory can be reopened and read
    independently of the conformance case runner above."""
    src_path = tmp_path / "source.aflite"
    with antfly_lite.create(src_path, no_sync=True) as db:
        db.batch(
            [antfly_lite.WriteIntent(key="doc:portable", value=b'{"title":"restored into a directory"}')],
            timestamp=1,
        )
        db.run_until_idle()
        backup = db.backup()

    dest_path = tmp_path / "restored-dir"
    antfly_lite.restore(str(dest_path), backup, storage=antfly_lite.Storage.DIRECTORY)

    opts = antfly_lite.OpenOptions(storage=antfly_lite.Storage.DIRECTORY, mode=antfly_lite.OpenMode.READONLY)
    with antfly_lite.open_with_options(dest_path, opts) as restored:
        assert restored.lookup("doc:portable") == {"title": "restored into a directory"}
        status = restored.status()
        assert status["storage"]["format"] == "directory"
