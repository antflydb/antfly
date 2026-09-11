# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time
import unittest
from unittest import mock
import urllib.request

import bootstrap_monitoring as boot


class Response(io.BytesIO):
    def __init__(self, content, headers=None, url="https://release-assets.githubusercontent.com/file", status=200):
        super().__init__(content)
        self.headers = headers or {}
        self.url = url
        self.status = status
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)


def archive(path, entries):
    with tarfile.open(path, "w:gz") as output:
        for name, content, kind in entries:
            item = tarfile.TarInfo(name)
            item.type = kind
            item.size = len(content) if kind == tarfile.REGTYPE else 0
            item.linkname = "../../outside" if kind != tarfile.REGTYPE else ""
            output.addfile(item, io.BytesIO(content) if item.size else None)


class MonitoringBootstrapTests(unittest.TestCase):
    def test_platforms_and_manifest_pin_official_release_architectures(self):
        data = boot.manifest()
        for system, machine, expected in (("Linux", "x86_64", "linux-amd64"),
                                          ("Linux", "aarch64", "linux-arm64"),
                                          ("Darwin", "arm64", "darwin-arm64")):
            with self.subTest(system=system, machine=machine), \
                    mock.patch.object(boot.platform, "system", return_value=system), \
                    mock.patch.object(boot.platform, "machine", return_value=machine):
                self.assertEqual(boot.current_platform(), expected)
                self.assertIn(expected, data["platforms"])
        with mock.patch.object(boot.platform, "system", return_value="Other"):
            with self.assertRaises(boot.ToolError):
                boot.current_platform()
        self.assertEqual(data["platforms"]["linux-amd64"]["archive_sha256"],
                         "f665c6da19eb7ba399c915d30c7d9793c9b417bf8a749b504bc470678631478d")
        self.assertEqual(data["platforms"]["linux-arm64"]["archive_sha256"],
                         "077f3781ab7245dc04c9a3c9b78ba120fc8e41aa0dc97489b0af67247e50ba83")

    def test_manifest_rejects_path_escape_and_work_limit_increases(self):
        original = boot.manifest()
        for mutate in (
            lambda d: d["platforms"]["linux-amd64"].update(archive_name="../escaped"),
            lambda d: d["limits"].update(max_archive_bytes=129 * 1024 * 1024),
            lambda d: d["limits"].update(command_timeout_seconds=True),
        ):
            data = copy.deepcopy(original)
            mutate(data)
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "tooling.json"
                path.write_text(json.dumps(data))
                with mock.patch.object(boot, "MANIFEST", path), self.assertRaises(boot.ToolError):
                    boot.manifest()

    def test_bounded_download_checks_exact_bytes_hash_headers_and_deadline(self):
        payload = b"payload" * 11000
        digest = hashlib.sha256(payload).hexdigest()
        cases = [(payload, {}, digest, True), (payload[:-1], {}, digest, False),
                 (payload + b"x", {}, digest, False), (payload, {}, "0" * 64, False),
                 (payload, {"Content-Length": str(len(payload) + 1)}, digest, False),
                 (payload, {"Content-Encoding": "gzip"}, digest, False)]
        for index, (content, headers, pin, succeeds) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as tmp:
                response = Response(content, headers)
                path = Path(tmp) / "archive"
                call = lambda: boot.download(boot.RELEASE_BASE + "fixture", path, len(payload), pin,
                                              time.monotonic() + 10, 1,
                                              opener=lambda request, timeout: response)
                if succeeds:
                    call()
                    self.assertEqual(path.read_bytes(), payload)
                else:
                    with self.assertRaises(boot.ToolError):
                        call()
                self.assertTrue(response.closed)
                self.assertTrue(all(0 < size <= boot.CHUNK for size in response.read_sizes))
                self.assertLessEqual(path.stat().st_size if path.exists() else 0, len(payload))
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(boot.ToolError):
            boot.download(boot.RELEASE_BASE + "fixture", Path(tmp) / "archive", 1, "0" * 64,
                          time.monotonic() - 1, 1, opener=mock.Mock())

    def test_download_redirects_cannot_downgrade_tls_or_add_credentials(self):
        handler = boot.HttpsOnlyRedirects()
        request = urllib.request.Request(boot.RELEASE_BASE + "fixture")
        for url in ("http://github.com/file", "https://user:password@github.com/file"):
            with self.subTest(url=url), self.assertRaises(boot.ToolError):
                handler.redirect_request(request, None, 302, "Found", {}, url)

    def test_published_checksum_requires_one_exact_archive_entry(self):
        item = boot.manifest()["platforms"]["linux-arm64"]
        good = item["archive_sha256"] + "  " + item["archive_name"] + "\n"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sha256sums.txt"
            path.write_text(good)
            boot.check_published_checksum(path, item)
            for content in ("", good + good, good.replace(item["archive_sha256"], "0" * 64)):
                path.write_text(content)
                with self.assertRaises(boot.ToolError):
                    boot.check_published_checksum(path, item)

    def test_archive_extracts_only_exact_regular_tool_and_rejects_unsafe_inventory(self):
        member = "prometheus-3.14.0.linux-arm64/promtool"
        item = {"tool_member": member}
        limits = {"max_tar_members": 4, "max_unpacked_bytes": 64, "max_tool_bytes": 16}
        good = [("../../outside", b"ignore", tarfile.REGTYPE), (member, b"tool", tarfile.REGTYPE)]
        cases = [(good, True), (good + [(member, b"tool", tarfile.REGTYPE)], False),
                 ([(member, b"", tarfile.SYMTYPE)], False),
                 ([(member, b"", tarfile.LNKTYPE)], False),
                 ([(member, b"x" * 17, tarfile.REGTYPE)], False),
                 ([("ignored", b"x" * 65, tarfile.REGTYPE)], False),
                 ([(f"ignored{i}", b"", tarfile.REGTYPE) for i in range(5)], False),
                 ([], False)]
        for index, (entries, succeeds) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                path, output = root / "archive.gz", root / "promtool"
                archive(path, entries)
                if succeeds:
                    boot.extract_tool(path, output, item, limits, time.monotonic() + 10)
                    self.assertEqual(output.read_bytes(), b"tool")
                    self.assertEqual(output.stat().st_mode & 0o777, 0o700)
                else:
                    with self.assertRaises(boot.ToolError):
                        boot.extract_tool(path, output, item, limits, time.monotonic() + 10)
                self.assertFalse((root / "outside").exists())

    def test_private_owner_drains_on_success_copy_failure_and_download_failure(self):
        data = boot.manifest()
        payload = b"test-only non-executable bytes"
        item = data["platforms"]["darwin-arm64"]
        item.update(tool_size_bytes=len(payload), tool_sha256=hashlib.sha256(payload).hexdigest())
        reported = f"promtool, version 3.14.0 (revision: {data['source_revision']})\nplatform: darwin/arm64\n"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            existing = root / "existing"
            existing.write_bytes(payload)
            with mock.patch.object(boot, "manifest", return_value=data), \
                    mock.patch.object(boot, "current_platform", return_value="darwin-arm64"), \
                    mock.patch.object(boot.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, reported)), \
                    mock.patch.object(boot, "download", side_effect=boot.ToolError("injected download failure")) as download:
                with boot.prepared_tool(existing, root) as (tool, _):
                    owner = tool.parent
                    self.assertEqual(tool.read_bytes(), payload)
                    self.assertNotEqual(tool, existing)
                self.assertFalse(owner.exists())
                download.assert_not_called()
                with self.assertRaisesRegex(RuntimeError, "child failure"):
                    with boot.prepared_tool(existing, root):
                        raise RuntimeError("child failure")
                existing.write_bytes(payload[:-1] + b"X")
                with self.assertRaises(boot.ToolError):
                    with boot.prepared_tool(existing, root):
                        self.fail("modified bytes were admitted")
                with self.assertRaises(boot.ToolError):
                    with boot.prepared_tool(None, root):
                        self.fail("failed download was admitted")
            self.assertEqual(list(root.iterdir()), [existing])

    def test_child_receives_verified_override_and_failure_is_not_swallowed(self):
        @contextlib.contextmanager
        def prepared(existing, temporary):
            yield Path("/private/owned/promtool"), 900

        with mock.patch.object(boot, "prepared_tool", prepared), \
                mock.patch.dict(boot.os.environ, {"ANTFLY_GLINER25_PROMTOOL": "/unverified"}), \
                mock.patch.object(boot.subprocess, "run", return_value=subprocess.CompletedProcess([], 7)) as run:
            self.assertEqual(boot.main(["--", "python3", "local_contracts.py"]), 7)
            self.assertEqual(run.call_args.kwargs["env"]["ANTFLY_GLINER25_PROMTOOL"], "/private/owned/promtool")
            self.assertEqual(run.call_args.kwargs["timeout"], 900)

    def test_both_existing_ci_contract_steps_require_the_bootstrap(self):
        workflow = (boot.ROOT / ".github/workflows/zig-tests.yml").read_text()
        steps = workflow.split("- name: Test GLiNER2.5 artifact evaluation and training contracts")[1:]
        self.assertEqual(len(steps), 2)
        for body in steps:
            step = body.split("\n      - name:", 1)[0]
            self.assertIn('bootstrap_monitoring.py --temp-dir "$RUNNER_TEMP" --', step)
            self.assertIn("python3 scripts/run_model_contract_tests.py gliner25", step)
            self.assertNotIn("continue-on-error", step)


if __name__ == "__main__":
    unittest.main()
