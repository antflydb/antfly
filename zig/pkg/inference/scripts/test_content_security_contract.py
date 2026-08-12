#!/usr/bin/env python3

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]


class ContentSecurityContractTest(unittest.TestCase):
    def test_allowlist_and_server_defaults_are_documented(self) -> None:
        shared = " ".join((ROOT / "specs/openapi/shared/scraping.yaml").read_text().split())
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
            self.assertIn("omitted allowed_hosts and allowed_paths remain explicit deny-all lists", text)

        for relative in (
            "specs/openapi/inference/api.yaml",
            "specs/openapi/inference/config.yaml",
        ):
            text = " ".join((ROOT / relative).read_text().split())
            self.assertIn("Remote URL byte potential is reserved before fetch", text)

        self.assertIn("Images are rejected rather than resized", shared)
        self.assertIn("generate/chat, dense embed, multimodal rerank", shared)
        self.assertIn("Batch generation rejects multimodal content before fetch", shared)
        self.assertIn("non-inference scraping consumers do not enforce this setting", shared)


if __name__ == "__main__":
    unittest.main()
