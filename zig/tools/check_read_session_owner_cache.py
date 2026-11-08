#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Qualify retained-session implementation isolation in actual production owners.

Debug only. Actual private fields must rebuild the physical engine/source
fixture while preserving API, kernel and native-client compiler caches. The
sessions profile covers three session implementations and shared Budget; the
reader profile covers physical Reader/query-lease/document-reader state and shared
Reader Request. Shared values must rebuild all owners. This checks cache reuse,
not release performance.
"""

import argparse
from contextlib import ExitStack
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

from cache_overlay import link_children, own

ZIG = Path(__file__).resolve().parents[1]
REPO = ZIG.parent
EDITS = {
    "publication_lease_state": (
        "pkg/antfly-embedded/src/storage/db/publication_read_lease.zig",
        "    return struct {\n",
    ),
    "shared_publication_lease": (
        "pkg/antfly-embedded/src/storage/read_session_values.zig",
        "pub const PublicationReadLease = extern struct {\n",
    ),
    "relational_state": (
        "pkg/antfly-embedded/src/storage/db/relational_read_session.zig",
        "pub const Implementation = struct {\n",
    ),
    "document_state": (
        "pkg/antfly-embedded/src/storage/db/document_rows.zig",
        "pub const Implementation = struct {\n",
    ),
    "statement_state": (
        "pkg/antfly-embedded/src/storage/db/read_statement_snapshot.zig",
        "pub const Implementation = struct {\n",
    ),
    "capture_fence_state": (
        "pkg/antfly-embedded/src/storage/db/statement_capture_fence.zig",
        "pub const Implementation = struct {\n",
    ),
    "shared_handle": (
        "pkg/antfly-embedded/src/storage/read_session_contract.zig",
        "pub const Handle = extern struct {\n",
    ),
    "shared_budget": (
        "pkg/antfly-embedded/src/storage/read_session_values.zig",
        "pub const Budget = struct {\n",
    ),
    "reader_state": (
        "pkg/antfly-embedded/src/storage/db/relational_rows.zig",
        "pub const Reader = struct {\n",
    ),
    "shared_reader_request": (
        "pkg/antfly-embedded/src/storage/read_reader_values.zig",
        "pub const Request = struct {\n",
    ),
    "query_lease_state": (
        "pkg/antfly-embedded/src/storage/db/query_read_lease.zig",
        "        db: *DB,\n",
    ),
    "document_reader_state": (
        "pkg/antfly-embedded/src/storage/db/document_read_rows.zig",
        "        db: *DB,\n",
    ),
}
PROFILES = {
    "publication": ("publication_lease_state", "shared_publication_lease"),
    "capture": ("capture_fence_state", "statement_state", "shared_handle"),
    "sessions": (
        "relational_state",
        "document_state",
        "statement_state",
        "shared_budget",
    ),
    "reader": (
        "reader_state",
        "query_lease_state",
        "document_reader_state",
        "shared_reader_request",
    ),
}
FIELD = "    cache_isolation_epoch: u64 = 0,\n"
ARTIFACTS = {
    "api_client": ("lib", "antfly-runtime-api_kernel"),
    "kernel_client": ("lib", "antfly-storage-kernel"),
    "session_client": ("lib", "read-session-compiled-client-probe"),
    "physical_engine": ("lib", "antfly-storage-engine"),
    "source_fixture": ("test_obj", "read-session-compiled-client-tests"),
}


def compiler_states(output):
    result = {}
    errors = []
    for role, (kind, name) in ARTIFACTS.items():
        found = re.findall(
            rf"compile {kind} {re.escape(name)} debug \S+ (cached|success)\b",
            output,
        )
        if len(found) != 1:
            errors.append(f"Expected one compiler result for {role}, found {found}")
        else:
            result[role] = found[0]
    return result, errors


def field_probe(phase, original):
    marker = EDITS[phase][1]
    if original.count(marker) != 1:
        raise RuntimeError(f"Session shape changed; update {phase} field probe")
    field = (
        "    " + FIELD
        if phase
        in {"query_lease_state", "document_reader_state", "publication_lease_state"}
        else FIELD
    )
    return original.replace(marker, marker + field)


def provider_archive(root):
    try:
        candidates = list((root / ".zig-cache/o").glob("*/libantfly-storage-engine.a"))
        if not candidates:
            return {}
        path = max(candidates, key=lambda p: p.stat().st_mtime_ns)
        return {
            "provider_archive": str(path),
            "provider_archive_bytes": path.stat().st_size,
            "provider_archive_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    except OSError as error:
        return dict(provider_archive_error=str(error))


def expected_states(phase):
    return {
        role: "cached"
        if phase == "warm"
        or (
            phase in EDITS
            and not phase.startswith("shared_")
            and role.endswith("client")
        )
        else "success"
        for role in ARTIFACTS
    }


def linked_tests_passed(output, count):
    return (
        re.search(rf"(?m)^{count} passed; 0 skipped; 0 failed; 0 leaked\.$", output)
        is not None
    )


def authored_files(root, extensions):
    for directory, children, files in os.walk(root):
        children[:] = [
            name
            for name in children
            if name
            not in {
                ".git",
                ".zig-cache",
                ".zig-global-cache",
                "zig-pkg",
                "target",
                "node_modules",
                "__pycache__",
            }
            and not name.startswith("zig-out")
        ]
        for name in files:
            path = Path(directory) / name
            if path.suffix in extensions:
                yield path


def retain_source_snapshot(root, report_path, report):
    """Archive explicit baseline inputs, never linked assets or cache trees."""
    archive_path = report_path.with_suffix(".sources.tar.gz")
    with tarfile.open(archive_path, "w:gz") as archive:
        for prefix, base, hashes in (
            ("zig/", root, report["source_snapshot_sha256"]),
            ("", root.parent, report["repository_input_snapshot_sha256"]),
        ):
            for relative, expected in sorted(hashes.items()):
                path = base / relative
                if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                    raise RuntimeError(
                        f"Snapshot input changed before archiving: {path}"
                    )
                archive.add(path, arcname=prefix + relative, recursive=False)
        manifest = json.dumps(report, indent=2, sort_keys=True).encode() + b"\n"
        entry = tarfile.TarInfo("snapshot-manifest.json")
        entry.size = len(manifest)
        archive.addfile(entry, io.BytesIO(manifest))
    return {
        "source_snapshot_archive": str(archive_path),
        "source_snapshot_archive_bytes": archive_path.stat().st_size,
        "source_snapshot_archive_sha256": hashlib.sha256(
            archive_path.read_bytes()
        ).hexdigest(),
    }


class CacheEvicted(RuntimeError):
    pass


class CacheLease:
    """P0 lease pattern, including partial output-root deletion detection."""

    def __init__(self, roots):
        self.entries = []
        self.last_touch = 0.0
        self.invalid = None
        self.marker_name = f".antfly-active-retained-read-{os.getpid()}"
        for root in roots:
            self.protect(root)

    def protect(self, root):
        # Initialization is the only place that creates directories/markers.
        if self.entries:
            self.pulse()
        for suffix in ("", "o", "h", "z", "tmp", "args"):
            directory = root / suffix
            if any(entry[0] == directory for entry in self.entries):
                continue
            directory.mkdir(parents=True, exist_ok=True)
            marker = directory / self.marker_name
            marker.write_text(str(os.getpid()) + "\n")
            status = directory.stat()
            self.entries.append((directory, marker, (status.st_dev, status.st_ino)))

    def pulse(self):
        if self.invalid:
            raise CacheEvicted(self.invalid)
        try:
            for directory, marker, identity in self.entries:
                status = directory.stat()
                if (status.st_dev, status.st_ino) != identity or not marker.is_file():
                    raise FileNotFoundError(
                        f"cache or active marker replaced: {directory}"
                    )
            now = time.monotonic()
            if now - self.last_touch >= 5:
                for _, marker, _ in self.entries:
                    # Unlike touch(exist_ok=True), utime never recreates a lost marker.
                    os.utime(marker, None)
                self.last_touch = now
        except OSError as error:
            self.invalid = (
                f"Active compiler cache removed; qualification invalid: {error}"
            )
            raise CacheEvicted(self.invalid) from error

    def state(self):
        result = []
        for directory, marker, identity in self.entries:
            row = dict(
                directory=str(directory), marker=str(marker), expected_identity=identity
            )
            try:
                status = directory.stat()
                row.update(
                    identity=(status.st_dev, status.st_ino),
                    marker_exists=marker.is_file(),
                )
            except OSError as error:
                row["error"] = str(error)
            result.append(row)
        return result

    def close(self):
        for _, marker, _ in self.entries:
            marker.unlink(missing_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def failure_diagnostics(root, report_path, phase, lease):
    destination = report_path.parent / (report_path.stem + "-" + phase + "-diagnostics")
    destination.mkdir(parents=True, exist_ok=True)
    copied = []
    copy_errors = []
    for source in (root / ".zig-cache/args").glob("*"):
        if source.is_file() and not source.name.startswith(".antfly-active-"):
            target = destination / "compiler-args" / source.name
            target.parent.mkdir(exist_ok=True)
            try:
                shutil.copyfile(source, target)
                copied.append(str(target))
            except OSError as error:
                copy_errors.append(dict(path=str(source), error=str(error)))
    listing = []
    cache_root = root / ".zig-cache"
    for directory, _, files in os.walk(cache_root):
        for name in files:
            path = Path(directory) / name
            try:
                status = path.stat()
                listing.append(
                    dict(
                        path=str(path.relative_to(cache_root)),
                        bytes=status.st_size,
                        mtime_ns=status.st_mtime_ns,
                    )
                )
            except OSError as error:
                listing.append(dict(path=str(path), error=str(error)))
    output = destination / "cache-state.json"
    output.write_text(
        json.dumps(
            dict(
                lease_invalid=lease.invalid,
                protected_roots=lease.state(),
                local_cache_files=listing,
                preserved_compiler_args=copied,
                copy_errors=copy_errors,
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return dict(cache_diagnostics=str(output))


def preserve_late_cache_loss(root, report_path, report, lease):
    """Check source preparation/phase gaps before TemporaryDirectory cleanup."""
    try:
        lease.pulse()
    except CacheEvicted as error:
        report.update(
            passed=False, cache_invalid=True, cache_eviction_detail=str(error)
        )
        if not any(row.get("cache_diagnostics") for row in report["phases"]):
            report.update(
                failure_diagnostics(root, report_path, "cache-eviction", lease)
            )
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def run(root, command, log, timeout, lease):
    process = None
    interruption = None
    interruption_detail = None
    started = time.monotonic()
    env = dict(
        os.environ,
        ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
        ZIG_GLOBAL_CACHE_DIR=str(ZIG / ".zig-cache/global"),
    )
    with log.open("w") as stream:
        try:
            lease.pulse()
            process = subprocess.Popen(
                command,
                cwd=root,
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = started + timeout
            while True:
                lease.pulse()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    process.wait(timeout=min(1, remaining))
                    lease.pulse()
                    break
                except subprocess.TimeoutExpired:
                    continue
        except BaseException as error:
            interruption = type(error).__name__
            interruption_detail = str(error)
            if process is not None and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
    return dict(
        elapsed_s=time.monotonic() - started,
        returncode=process.returncode if process is not None else None,
        interruption=interruption,
        interruption_detail=interruption_detail,
        cache_invalid=lease.invalid is not None,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--profile", choices=PROFILES, default="sessions")
    parser.add_argument(
        "--strip", action="store_true", help="Omit Debug information for cache checks"
    )
    parser.add_argument("--expected-tests", type=int, default=6)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    if args.expected_tests < 1:
        parser.error("expected-tests must be positive")
    edits = {phase: EDITS[phase] for phase in PROFILES[args.profile]}
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = ZIG / ".zig-cache/read-session-owner-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "tools/run_bounded_zig_build.py",
        "--zig",
        args.zig,
        "--",
        "build",
        "opaque-read-session-abi-integration-test",
        "runtime-unit-api_kernel",
        "runtime-unit-storage-adapter",
        "runtime-unit-storage-engine",
        "-Doptimize=debug",
        f"-Dstrip={str(args.strip).lower()}",
        "-Dshared-image-operations=true",
        "-Dshared-lake-execution=true",
        "-Dmetal=false",
        "-Dcuda=false",
        "-Dsystem-blas=false",
        "-Dcpu=baseline",
        "--summary",
        "all",
        "-j1",
    ]
    report = dict(
        scope=__doc__,
        profile=args.profile,
        strip=args.strip,
        expected_linked_tests=args.expected_tests,
        passed=False,
        command=command,
        phases=[],
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        zig_version=subprocess.check_output([args.zig, "version"], text=True).strip(),
        base_git_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
    )
    # Retain the exact runner even if unrelated formatting/header edits follow.
    shutil.copyfile(Path(__file__), report_path.with_suffix(".runner.py"))
    with CacheLease((ZIG / ".zig-cache", ZIG / ".zig-cache/global")) as lease:
        with (
            tempfile.TemporaryDirectory(dir=parent) as directory,
            ExitStack() as diagnostics,
        ):
            root = Path(directory) / "zig"
            link_children(ZIG, root)
            lease.protect(root / ".zig-cache")
            diagnostics.callback(
                preserve_late_cache_loss, root, report_path, report, lease
            )
            # API schema modules deliberately read ordinary repository file inputs
            # at compile time. Preserve their ../ paths as well as Zig sources.
            repository_inputs = [REPO / "openapi.yaml"]
            repository_inputs.extend((REPO / "specs/openapi").rglob("*.yaml"))
            repository_inputs.extend((REPO / "scripts").glob("*.py"))
            repository_inputs.extend(
                REPO / "scripts" / name for name in ("pyproject.toml", "uv.lock")
            )
            repository_hashes = {}
            for path in repository_inputs:
                relative = path.relative_to(REPO)
                destination = root.parent / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
                repository_hashes[str(relative)] = hashlib.sha256(
                    destination.read_bytes()
                ).hexdigest()
            report["repository_input_snapshot_sha256"] = repository_hashes
            # Freeze authored compiler and constructor inputs, including C headers
            # and build/link helpers. Immutable assets remain linked to the checkout.
            sources = [ZIG / "build.zig", ZIG / "build.zig.zon"]
            extensions = {".zig", ".zon", ".c", ".h", ".m", ".mm", ".S"}
            for family in ("lib", "pkg", "build_support"):
                sources.extend(authored_files(ZIG / family, extensions))
            sources.extend((ZIG / "tools").glob("*.py"))
            hashes = {}
            for path in sorted(set(sources)):
                relative = str(path.relative_to(ZIG))
                owned = own(root, relative)
                hashes[relative] = hashlib.sha256(owned.read_bytes()).hexdigest()
            report["source_snapshot_sha256"] = hashes
            originals = {
                phase: (root / relative).read_text()
                for phase, (relative, _) in edits.items()
            }
            preflights = {}
            for phase, (relative, marker) in edits.items():
                private = root / relative
                original = originals[phase]
                private.write_text(field_probe(phase, original))
                preflight = subprocess.run(
                    [args.zig, "fmt", "--check", str(private)],
                    capture_output=True,
                    text=True,
                )
                preflights[phase] = dict(
                    returncode=preflight.returncode,
                    stdout=preflight.stdout,
                    stderr=preflight.stderr,
                    candidate_sha256=hashlib.sha256(private.read_bytes()).hexdigest(),
                )
                private.write_text(original)
            report["field_probe_preflights"] = preflights
            report["cache_lease"] = dict(
                heartbeat_s=5,
                poll_s=1,
                roots=lease.state(),
                policy="Any lost/replaced root or marker invalidates the matrix; never recreate during qualification.",
            )
            report.update(retain_source_snapshot(root, report_path, report))
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            if any(row["returncode"] != 0 for row in preflights.values()):
                raise SystemExit(1)
            for phase in ("initial", "warm", *edits):
                print("read session:", phase, flush=True)
                if phase in edits:
                    relative, marker = edits[phase]
                    (root / relative).write_text(field_probe(phase, originals[phase]))
                log = report_path.with_name(report_path.stem + "-" + phase + ".log")
                row = dict(phase=phase, log=str(log), errors=[])
                row.update(run(root, command, log, args.timeout, lease))
                output = log.read_text(errors="replace")
                states, errors = compiler_states(output)
                row.update(
                    compilers=states,
                    log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                )
                row["errors"].extend(errors)
                expected = expected_states(phase)
                row["expected"] = expected
                if states != expected:
                    row["errors"].append(f"Expected {expected}, observed {states}")
                if row["returncode"] != 0 or row["interruption"]:
                    row["errors"].append("Build failed or was interrupted")
                if not re.search(r"Build Summary: (\d+)/\1 steps succeeded", output):
                    row["errors"].append("No complete successful build summary")
                if not linked_tests_passed(output, args.expected_tests):
                    row["errors"].append(
                        "All expected linked source/client session tests must execute"
                    )
                row.update(provider_archive(root))
                if not row.get("provider_archive_sha256"):
                    row["errors"].append(
                        "Physical provider archive is missing or unreadable"
                    )
                try:
                    lease.pulse()
                except CacheEvicted as error:
                    row.update(
                        cache_invalid=True,
                        interruption="CacheEvicted",
                        interruption_detail=str(error),
                    )
                    row["errors"].append(str(error))
                if phase in edits:
                    row["edited_source"] = edits[phase][0]
                    row["edited_source_sha256"] = hashlib.sha256(
                        (root / edits[phase][0]).read_bytes()
                    ).hexdigest()
                    before = report["phases"][-1].get("provider_archive_sha256")
                    if not before or before == row.get("provider_archive_sha256"):
                        row["errors"].append(
                            "Real field edit did not change provider archive"
                        )
                row["passed"] = not row["errors"]
                report["phases"].append(row)
                report_path.write_text(
                    json.dumps(report, indent=2, sort_keys=True) + "\n"
                )
                print(
                    json.dumps(
                        {
                            "phase": phase,
                            "passed": row["passed"],
                            "compilers": states,
                            "errors": row["errors"],
                        }
                    ),
                    flush=True,
                )
                if not row["passed"]:
                    row.update(failure_diagnostics(root, report_path, phase, lease))
                    report["cache_invalid"] = row["cache_invalid"]
                    report_path.write_text(
                        json.dumps(report, indent=2, sort_keys=True) + "\n"
                    )
                    raise SystemExit(1)
    if report.get("cache_invalid"):
        raise SystemExit(1)
    report["passed"] = True
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
