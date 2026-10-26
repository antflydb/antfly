#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
WT = HERE.parents[3]
TOOL = HERE / "build_attention_mass_dynamic_cg_cli_v2_row_trace.py"
spec = importlib.util.spec_from_file_location("dynamic_link", TOOL)
assert spec and spec.loader
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class DynamicLinkReceiptTest(unittest.TestCase):
    def args(self, root: Path) -> argparse.Namespace:
        library = Path("/private/tmp/attention-matching-highs-v180-static-library-receipt-v2.json")
        return argparse.Namespace(
            zig=Path("/opt/homebrew/Cellar/zig/0.17.0/bin/zig"),
            zig_sha256="9fb480d3398506f5e2bef77f175766a8de0ee191dbdfbfb0c117a1150f141c91",
            driver=HERE / "attention_mass_highs_v180_dynamic_cg_cli_v5_row_trace.zig",
            driver_sha256=sha(HERE / "attention_mass_highs_v180_dynamic_cg_cli_v5_row_trace.zig"),
            lp=Path("/private/tmp/attention_mass_lp_v4.zig"),
            lp_sha256=m.PINNED["lp"],
            controller=HERE / "attention_mass_constraint_generation_v6_row_trace.zig",
            controller_sha256=m.PINNED["controller"],
            adapter=HERE / "attention_mass_highs_v180_adapter_v4.zig",
            adapter_sha256=m.PINNED["adapter"],
            library_receipt=library,
            library_receipt_sha256=sha(library),
            output_binary=root / "dynamic-cg",
            output_receipt=root / "executable.json",
            cache_dir=root / "cache",
            global_cache_dir=root / "global-cache",
            work_dir=root,
            target="aarch64-macos-native",
        )

    def test_actual_closure_builds_exact_receipt_with_injected_link(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args = self.args(root)
            args.cache_dir.mkdir()
            args.global_cache_dir.mkdir()
            calls = []

            def fake(argv, **kwargs):
                calls.append((argv, kwargs))
                args.output_binary.write_bytes(b"synthetic-link-output")
                return type("Result", (), {"returncode": 0})()

            value = m.run(args, runner=fake)
            self.assertEqual(1, len(calls))
            self.assertEqual("ReleaseSafe", value["optimize"])
            self.assertTrue(value["strip"])
            self.assertEqual(args.driver_sha256, value["sources"]["driver"]["sha256"])
            self.assertEqual(value, json.loads(args.output_receipt.read_text()))
            self.assertNotIn("CFLAGS", calls[0][1]["env"])
            self.assertEqual(str(args.output_binary.resolve()), value["binary"]["path"])

    def test_wrong_source_and_stale_output_fail_before_link(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args = self.args(root)
            args.cache_dir.mkdir()
            args.global_cache_dir.mkdir()
            args.driver_sha256 = "0" * 64
            with self.assertRaisesRegex(ValueError, "driver changed"):
                m.run(args, runner=lambda *_a, **_k: self.fail("link called"))
            args.driver_sha256 = sha(args.driver)
            args.output_binary.write_bytes(b"stale")
            with self.assertRaises(FileExistsError):
                m.run(args, runner=lambda *_a, **_k: self.fail("link called"))


if __name__ == "__main__":
    unittest.main()
