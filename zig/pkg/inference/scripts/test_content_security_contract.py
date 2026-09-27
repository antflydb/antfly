#!/usr/bin/env python3
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

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]


class ContentSecurityContractTest(unittest.TestCase):
    def test_allowlist_and_server_defaults_are_documented(self) -> None:
        shared = " ".join(
            (ROOT / "specs/openapi/shared/scraping.yaml").read_text().split()
        )
        self.assertIn("generic scraper treats omission as unrestricted", shared)
        self.assertIn("explicit empty list as deny-all", shared)
        self.assertIn("Antfly inference requires an explicit allowlist", shared)
        self.assertIn("Antfly inference requires explicit path allowlists", shared)

        for relative in (
            "specs/openapi/inference/api.yaml",
            "specs/openapi/inference/config.yaml",
            "openapi.yaml",
        ):
            text = " ".join((ROOT / relative).read_text().split())
            self.assertIn(
                "Omitted or empty policies deny HTTP(S), file, and S3",
                text,
            )
            self.assertIn(
                "omitted allowed_hosts and allowed_paths remain explicit deny-all lists",
                text,
            )

        for relative in (
            "specs/openapi/inference/api.yaml",
            "specs/openapi/inference/config.yaml",
        ):
            text = " ".join((ROOT / relative).read_text().split())
            self.assertIn("Remote URL byte potential is reserved before fetch", text)

        self.assertIn("Images are rejected rather than resized", shared)
        self.assertIn(
            "generate/chat, batch generation, dense embed, multimodal rerank", shared
        )
        self.assertIn(
            "Batch generation applies the same image-header admission", shared
        )
        self.assertIn(
            "non-inference scraping consumers do not enforce this setting", shared
        )


if __name__ == "__main__":
    unittest.main()
