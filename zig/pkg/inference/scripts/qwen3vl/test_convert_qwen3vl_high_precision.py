#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

import convert_qwen3vl_high_precision as conversion


class HighPrecisionConversionTests(unittest.TestCase):
    def write_source(self, root: Path) -> None:
        (root / "model.safetensors").write_bytes(b"weights")
        (root / "config.json").write_text(json.dumps({"model_type": "qwen3_vl"}))
        for name in conversion.REQUIRED_SIDECARS:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if name != "config.json":
                path.write_text("{}")

    def test_validate_source_binds_known_weight_and_required_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write_source(root)
            digest = hashlib.sha256(b"weights").hexdigest()
            with mock.patch.object(conversion, "MODEL_SIZE", len(b"weights")), mock.patch.object(
                conversion, "MODEL_SHA256", digest
            ):
                evidence = conversion.validate_source(root)
            self.assertEqual(digest, evidence["model"]["sha256"])
            self.assertEqual(set(conversion.REQUIRED_SIDECARS), set(evidence["sidecars"]))

    def test_validate_source_rejects_wrong_architecture(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.write_source(root)
            (root / "config.json").write_text(json.dumps({"model_type": "qwen2_vl"}))
            digest = hashlib.sha256(b"weights").hexdigest()
            with mock.patch.object(conversion, "MODEL_SIZE", len(b"weights")), mock.patch.object(
                conversion, "MODEL_SHA256", digest
            ):
                with self.assertRaisesRegex(conversion.ConversionError, "model_type"):
                    conversion.validate_source(root)

    def test_output_identity_is_explicitly_reference_only(self) -> None:
        self.assertEqual("bf16-reference-bundle-v1", conversion.OUTPUT_IDENTITY["variant"])
        self.assertNotEqual(
            conversion.OUTPUT_IDENTITY["variant"], "q4-k-m-bundle-v1"
        )

    def test_safe_relative_path_rejects_traversal(self) -> None:
        with self.assertRaises(conversion.ConversionError):
            conversion.safe_relative_path("../model.safetensors")


if __name__ == "__main__":
    unittest.main()
