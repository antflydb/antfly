#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import struct
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
TOOL = HERE / "prepare_attention_mass_dynamic_cg_input_v1.py"
spec = importlib.util.spec_from_file_location("dynamic_input", TOOL)
assert spec and spec.loader
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class PortableManifestTest(unittest.TestCase):
    def fixture(self, root: Path, rows: int = 912, cols: int = 288, warm: bool = True):
        matrix = root / "matrix.f64le"
        with matrix.open("wb") as f:
            f.truncate(rows * cols * 8)
        source = root / "source.json"
        source.write_text('{"source":"synthetic-metadata-only"}\n')
        warm_path = root / "warm.u64le"
        if warm:
            warm_path.write_bytes(struct.pack("<QQQ", 8, 1, 7))
        output = root / "input.json"
        args = argparse.Namespace(
            query_rows=rows,
            retained_rows=cols,
            matrix=matrix,
            warm_rows=warm_path if warm else None,
            source=source,
            output=output,
        )
        return args

    def test_all_profiles_and_null_or_ordered_warm(self):
        for rows, cols in sorted(m.PROFILES):
            with self.subTest(rows=rows, cols=cols), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                args = self.fixture(root, rows, cols, warm=cols != 352)
                value = m.build(args)
                self.assertEqual([rows, cols], value["matrix"]["shape"])
                self.assertEqual(rows * cols * 8, value["matrix"]["bytes"])
                if args.warm_rows:
                    self.assertEqual(3, value["warm_rows"]["count"])
                else:
                    self.assertIsNone(value["warm_rows"])
                self.assertFalse(value["fit_invoked"])
                self.assertFalse(value["held_tensors_loaded"])

    def test_fresh_output_and_mutations_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args = self.fixture(root)
            value = m.build(args)
            m.write_fresh(args.output, value)
            self.assertEqual(value, json.loads(args.output.read_text()))
            with self.assertRaises(FileExistsError):
                m.write_fresh(args.output, value)
            args.matrix.write_bytes(b"short")
            with self.assertRaisesRegex(ValueError, "matrix byte length"):
                m.build(argparse.Namespace(**{**vars(args), "output": root / "other.json"}))

    def test_outside_owner_and_bad_geometry_reject(self):
        with tempfile.TemporaryDirectory() as td, tempfile.TemporaryDirectory() as other:
            root = Path(td)
            args = self.fixture(root)
            with self.assertRaisesRegex(ValueError, "unsupported profile"):
                m.build(argparse.Namespace(**{**vars(args), "retained_rows": 351}))
            with self.assertRaisesRegex(ValueError, "below manifest"):
                m.build(argparse.Namespace(**{**vars(args), "output": Path(other) / "input.json"}))


if __name__ == "__main__":
    unittest.main()
