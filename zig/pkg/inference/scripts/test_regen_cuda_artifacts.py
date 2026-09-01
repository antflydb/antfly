#!/usr/bin/env python3

import os
import pathlib
import re
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).resolve().with_name("regen-cuda-artifacts.sh")
FLASH_SYMBOLS = (
    "antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1",
    "antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1",
)


class RegenCudaArtifactsTest(unittest.TestCase):
    def test_artifact_modes_require_fresh_codegen_before_nvcc(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_zig = pathlib.Path(temporary) / "zig"
            fake_zig.write_text(
                '#!/bin/sh\nprintf "zig-args:%s\\n" "$*"\nexit 37\n',
                encoding="utf-8",
            )
            fake_zig.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                ZIG_BIN=str(fake_zig),
                NVCC=str(pathlib.Path(temporary) / "nvcc-must-not-run"),
            )
            completed = subprocess.run(
                [str(SCRIPT), "--check", "--portable"],
                check=False,
                capture_output=True,
                env=environment,
                text=True,
            )

        self.assertEqual(37, completed.returncode)
        self.assertIn("zig-args:build quant-kernel-codegen -- --check", completed.stdout)
        self.assertNotIn("nvcc not found", completed.stderr)

    def test_default_zig_resolution_prefers_the_repo_pinned_toolchain(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        pinned = 'elif [ -x "$repo_dir/.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig" ]'
        path_lookup = "elif command -v zig >/dev/null 2>&1"
        self.assertIn(pinned, source)
        self.assertIn(path_lookup, source)
        self.assertLess(source.index(pinned), source.index(path_lookup))

    def test_flash_symbols_are_required_for_every_canonical_artifact(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        match = re.search(r"required_symbols=\(\n(?P<body>.*?)\n\)", source, re.DOTALL)
        self.assertIsNotNone(match)
        required_symbols = match.group("body").split()
        for symbol in FLASH_SYMBOLS:
            with self.subTest(symbol=symbol):
                self.assertEqual(1, required_symbols.count(symbol))

        for artifact in ("tmp_ptx", "tmp_fatbin", "tmp_sm89"):
            with self.subTest(artifact=artifact):
                self.assertEqual(1, source.count(f'check_required_symbols "${artifact}"'))


if __name__ == "__main__":
    unittest.main()
