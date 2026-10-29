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

from http.client import IncompleteRead
from io import BytesIO
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import create_github_release as github


class GitHubReadTests(unittest.TestCase):
    def test_truncated_read_restarts_get_and_decodes_only_complete_response(self):
        broken = mock.MagicMock()
        broken.__enter__.return_value.read.side_effect = IncompleteRead(b'{"id":', 3)
        with (
            mock.patch.object(
                github, "urlopen", side_effect=[broken, BytesIO(b'{"id":42}')]
            ) as opened,
            mock.patch.object(github.time, "sleep") as sleep,
        ):
            self.assertEqual(
                github.request_json("GET", "https://example.test/42", "token"),
                {"id": 42},
            )
        self.assertEqual(opened.call_count, 2)
        self.assertEqual(opened.call_args.kwargs["timeout"], 60)
        broken.__exit__.assert_called_once()
        sleep.assert_called_once_with(1)

    def test_read_retries_are_bounded(self):
        with (
            mock.patch.object(
                github, "urlopen", side_effect=ConnectionResetError("reset")
            ) as opened,
            mock.patch.object(github.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(github.GitHubError, "after 3 attempt"):
                github.download_bytes("https://example.test/asset", "token")
        self.assertEqual(opened.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_mutations_are_never_replayed_after_an_ambiguous_transport_result(self):
        for method in ("POST", "PATCH", "DELETE"):
            with (
                self.subTest(method=method),
                mock.patch.object(
                    github, "urlopen", side_effect=IncompleteRead(b"partial", 5)
                ) as opened,
                mock.patch.object(github.time, "sleep") as sleep,
            ):
                with self.assertRaises(github.GitHubError):
                    github.request_json(method, "https://example.test/release", "token")
            opened.assert_called_once()
            sleep.assert_not_called()
        with mock.patch.object(github, "urlopen", side_effect=TimeoutError()) as opened:
            with self.assertRaises(github.GitHubError):
                github.request_bytes(
                    "POST",
                    "https://example.test/asset",
                    "token",
                    b"asset",
                    "application/octet-stream",
                )
            opened.assert_called_once()

    def test_retry_after_and_terminal_statuses(self):
        error = HTTPError(
            "https://example.test", 429, "rate limited", {"Retry-After": "7"}, BytesIO()
        )
        with (
            mock.patch.object(github, "urlopen", side_effect=[error, BytesIO(b"[]")]),
            mock.patch.object(github.time, "sleep") as sleep,
        ):
            self.assertEqual(
                github.request_json("GET", "https://example.test", "token"), []
            )
        sleep.assert_called_once_with(7)
        for status in (401, 403, 404):
            with (
                self.subTest(status=status),
                mock.patch.object(
                    github,
                    "urlopen",
                    side_effect=HTTPError(
                        "https://example.test", status, "denied", {}, BytesIO()
                    ),
                ) as opened,
            ):
                with self.assertRaisesRegex(
                    github.GitHubError, f"failed with {status}"
                ):
                    github.request_json("GET", "https://example.test", "token")
                opened.assert_called_once()

    def test_multi_asset_draft_refreshes_once_by_id_and_checks_final_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = {"a.bin", "b.bin", "c.bin"}
            for name in names:
                (root / name).write_bytes(name.encode())
            uploaded = []
            reads = []
            release = {"id": 42, "tag_name": "v1.2.3", "draft": True}

            def api(method, repo, path, token):
                self.assertEqual(method, "GET")
                reads.append(path)
                if path == "/releases/42":
                    return release
                if path.startswith("/releases/42/assets?"):
                    return [{"id": i, "name": name} for i, name in enumerate(uploaded)]
                self.fail(f"unexpected release lookup: {path}")

            def upload(method, url, token, data, content_type):
                self.assertEqual(method, "POST")
                name = parse_qs(urlsplit(url).query)["name"][0]
                self.assertNotIn(name, uploaded)
                uploaded.append(name)
                return {"id": len(uploaded), "name": name}

            arguments = [
                "create_github_release.py",
                "--repo",
                "antflydb/antfly",
                "--tag",
                "v1.2.3",
                "--commit",
                "a" * 40,
                "--asset",
                str(root / "*.bin"),
                "--draft",
                "--immutable-assets",
                "--exact-assets",
            ]
            with (
                mock.patch("sys.argv", arguments),
                mock.patch.dict("os.environ", {"GITHUB_TOKEN": "token"}),
                mock.patch.object(github, "release_body", return_value="notes"),
                mock.patch.object(
                    github, "create_or_update_release", return_value=release
                ),
                mock.patch.object(
                    github,
                    "get_release_by_tag",
                    side_effect=AssertionError("draft lookup repeated"),
                ),
                mock.patch.object(github, "github_api", side_effect=api),
                mock.patch.object(github, "request_bytes", side_effect=upload),
            ):
                self.assertEqual(github.main(), 0)
            self.assertEqual(set(uploaded), names)
            self.assertEqual(reads.count("/releases/42"), 1)
            self.assertEqual(len(reads), 3)


if __name__ == "__main__":
    unittest.main()
