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

import os
import pathlib
import re
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).resolve().with_name("regen-cuda-artifacts.sh")
ARTIFACTS = SCRIPT.parent.parent / "src" / "ops" / "cuda" / "artifacts"
FLASH_SYMBOLS = (
    "antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1",
    "antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1",
)


class RegenCudaArtifactsTest(unittest.TestCase):
    def test_jit_identity_covers_transitive_local_headers_and_launch_contracts(self) -> None:
        inference = SCRIPT.parent.parent
        identity = (inference / "build" / "jit_identity.zig").read_text()
        pending = [ARTIFACTS / "inference_cuda_kernels.cu"]
        visited = set()
        while pending:
            source = pending.pop().resolve()
            if source in visited:
                continue
            visited.add(source)
            for include in re.findall(r'^\s*#include\s+"([^"\n]+)"', source.read_text(), re.MULTILINE):
                header = (source.parent / include).resolve()
                relative = header.relative_to(inference.resolve()).as_posix()
                self.assertIn('"' + relative + '"', identity, f"untracked native include: {relative}")
                pending.append(header)
        for dependency in ("src/ops/cuda/launch_abi.zig", "src/ops/kernel_descriptor.zig", "src/ops/kernel_geometry.zig"):
            self.assertIn('"' + dependency + '"', identity)

    def test_preference_training_ptx_matches_source_launch_abi(self) -> None:
        source = (ARTIFACTS / "inference_cuda_kernels.cu").read_text()
        ptx = (ARTIFACTS / "inference_cuda_kernels.ptx").read_text()
        header = (ARTIFACTS.parent / "kernels" / "kernel_abi.cuh").read_text()
        macros = dict(re.findall(r"^#define (ANTFLY_CUDA_\w+_PARAMS) (.*)$",
                                 header.replace("\\\n", ""), re.MULTILINE))
        # These entrypoints changed with preference training. A stale artifact
        # can load successfully yet interpret launch arguments using the old
        # layout (batched dot silently ignored lhs_contract_last in CI).
        for symbol in (
            "termite_primitive_batched_dot_f32",
            "termite_embedding_lookup_i32_bf16_weight_f32",
            "termite_primitive_gather_bf16_f32",
            "termite_selected_token_logprobs_f32",
        ):
            with self.subTest(symbol=symbol):
                declaration = re.search(
                    rf'extern "C" __global__ void {symbol}\((.*?)\)',
                    source,
                    re.DOTALL,
                )
                entry = re.search(
                    rf"\.visible \.entry {symbol}\((.*?)\)", ptx, re.DOTALL
                )
                self.assertIsNotNone(declaration, f"missing CUDA source: {symbol}")
                self.assertIsNotNone(entry, f"missing compiled PTX: {symbol}")
                signature = declaration.group(1).strip()
                if signature.startswith("ANTFLY_CUDA_"):
                    self.assertIn(signature, macros, f"missing native signature: {symbol}")
                    signature = macros[signature]
                self.assertEqual(
                    len(signature.split(",")),
                    len(re.findall(r"\.param\b", entry.group(1))),
                    f"stale launch ABI for {symbol}; regenerate CUDA artifacts",
                )

    def test_native_scalar_parameter_macros_match_ptx_wire_layouts(self) -> None:
        source = (ARTIFACTS / "inference_cuda_kernels.cu").read_text()
        header = (ARTIFACTS.parent / "kernels" / "kernel_abi.cuh").read_text()
        macros = dict(re.findall(r"^#define (ANTFLY_CUDA_\w+_PARAMS) (.*)$",
                                 header.replace("\\\n", ""), re.MULTILINE))
        ptx = (ARTIFACTS / "inference_cuda_kernels.ptx").read_text()
        signatures = re.findall(r'extern "C" __global__ (?:__launch_bounds__\([^)]*\) )?void (\w+)\((ANTFLY_CUDA_\w+_PARAMS)\)', source)
        self.assertGreater(len(signatures), 20)
        checked = 0
        for symbol, macro in signatures:
            self.assertIn(macro, macros)
            parameters = macros[macro].split(",")
            # By-value native structs have their own size/offset contracts;
            # these launch records admit only scalar wires and device addresses.
            if any("*" not in parameter and not re.match(r"\s*(?:unsigned int|unsigned long long|int|float)\s+\w+\s*$", parameter)
                   for parameter in parameters):
                continue
            checked += 1
            with self.subTest(symbol=symbol):
                entry = re.search(rf"\.visible \.entry {symbol}\((.*?)\)", ptx, re.DOTALL)
                self.assertIsNotNone(entry, f"missing compiled PTX: {symbol}")
                native = ["u64" if "*" in parameter or "unsigned long long" in parameter else "f32" if "float" in parameter else "u32"
                          for parameter in parameters]
                wires = re.findall(r"\.param\s+\.(\w+)", entry.group(1))
                self.assertEqual(native, wires, f"stale native wire layout: {symbol}")
        self.assertGreater(checked, 20)

    def test_retired_unreachable_epilogues_have_no_source_slot_or_image(self) -> None:
        source = (ARTIFACTS / "inference_cuda_kernels.cu").read_text()
        slots = (ARTIFACTS.parent / "kernels.zig").read_text()
        script = SCRIPT.read_text()
        required = re.search(r"required_symbols=\(\n(.*?)\n\)", script, re.DOTALL).group(1).split()
        for symbol in (
            "termite_embedding_gemma2_gelu_ple_bf16",
            "termite_linear_q4_0_bias_f32_tc_hmma",
            "termite_linear_q4_0_bias_gelu_f32_tc_hmma",
            "termite_linear_q4_0_bias_add_f32_tc_hmma",
            "termite_linear_q4_0_bias_f32_tc_hmma_bf16",
            "termite_linear_q4_0_bias_gelu_f32_tc_hmma_bf16",
            "termite_linear_q4_0_bias_add_f32_tc_hmma_bf16",
        ):
            with self.subTest(symbol=symbol):
                self.assertNotIn(symbol, source)
                self.assertNotRegex(slots, rf"(?m)^    {symbol.removeprefix('termite_')}: driver_mod.CUfunction")
                self.assertNotIn(symbol, required)
                for filename in ("inference_cuda_kernels.ptx", "inference_cuda_kernels.fatbin", "inference_cuda_kernels_sm89.cubin"):
                    self.assertFalse(symbol.encode() in (ARTIFACTS / filename).read_bytes(), f"{filename} retains {symbol}")

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
        self.assertIn(
            "zig-args:build quant-kernel-codegen tile-kernel-codegen -- --check", completed.stdout
        )
        self.assertNotIn("nvcc not found", completed.stderr)

    def test_binary_export_checks_decode_compressed_symbol_tables(self) -> None:
        checker = re.search(r"(?ms)^check_required_symbols\(\) \{\n.*?^\}", SCRIPT.read_text()).group()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            image = root / "compressed.fatbin"
            image.write_bytes(b"opaque compressed payload, live name is not an export")
            tool = root / "cuobjdump"
            shell = root / "check.sh"
            shell.write_text('set -eu\nrequired_symbols=(live)\nretired_symbols=(retired)\ncuobjdump="$1"\n' + checker + '\ncheck_required_symbols "$2"\n')
            for table, succeeds in (
                ("STT_FUNC STB_GLOBAL STO_ENTRY live", True),
                ("STT_OBJECT STB_GLOBAL STV_DEFAULT live", False),
                ("STT_FUNC STB_GLOBAL STV_DEFAULT live", False),
                ("STT_FUNC STB_LOCAL STO_ENTRY live", False),
                ("STT_FUNC STB_GLOBAL STO_ENTRY other", False),
                ("STT_FUNC STB_GLOBAL STO_ENTRY live\nSTT_FUNC STB_GLOBAL STO_ENTRY retired", False),
            ):
                tool.write_text("#!/bin/sh\ncat <<'SYMBOLS'\n" + table + "\nSYMBOLS\n")
                tool.chmod(0o755)
                result = subprocess.run(["bash", str(shell), str(tool), str(image)], capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, succeeds, result.stderr)
            result = subprocess.run(["bash", str(shell), "", str(image)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("cuobjdump is required", result.stderr)
            ptx = root / "portable.ptx"
            ptx.write_text("// live\n.visible .entry other() {}\n")
            result = subprocess.run(["bash", str(shell), "", str(ptx)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            ptx.write_text(".visible .entry live() {}\n")
            result = subprocess.run(["bash", str(shell), "", str(ptx)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_fatbin_build_compresses_all_profiles_and_decodes_exports(self) -> None:
        source = SCRIPT.read_text()
        required = re.search(r"required_symbols=\(\n(.*?)\n\)", source, re.DOTALL).group(1).split()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            capture = root / "nvcc-args"
            scratch = root / "nvcc-scratch"
            zig = root / "zig"
            zig.write_text("#!/bin/sh\nexit 0\n")
            compiler = root / "nvcc"
            compiler.write_text(
                '#!/bin/sh\nif [ "$1" = --version ]; then echo "release 13.2"; exit 0; fi\n'
                + 'printf "%s\\n" "$@" > "' + str(capture) + '"\n'
                + 'printf "%s\\n" "$TMPDIR" > "' + str(scratch) + '"\n'
                + 'while [ "$#" -gt 0 ]; do if [ "$1" = -o ]; then shift; printf opaque > "$1"; exit 0; fi; shift; done\n'
            )
            dump = root / "cuobjdump"
            table = "\n".join("STT_FUNC STB_GLOBAL STO_ENTRY " + symbol for symbol in required)
            arches = "\n".join("ELF sm_" + arch for arch in ("75", "80", "89", "90", "100", "110", "120"))
            dump.write_text('#!/bin/sh\ncase "$1" in\n--dump-elf-symbols) cat <<EOF\n' + table + '\nEOF\n;;\n--list-elf) cat <<EOF\n' + arches + '\nEOF\n;;\n--list-ptx) echo "PTX sm_75" ;;\n*) exit 42 ;;\nesac\n')
            for tool in (zig, compiler, dump):
                tool.chmod(0o755)
            env = os.environ.copy()
            env.update(ZIG_BIN=str(zig), NVCC=str(compiler), CUOBJDUMP=str(dump))
            result = subprocess.run([str(SCRIPT), "--check", "--fatbin"], capture_output=True, text=True, env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("fatbin artifact is stale", result.stderr)
            args = capture.read_text().splitlines()
            self.assertIn("--compress-mode=speed", args)
            self.assertIn("-Xfatbin=-compress-all", args)
            profiles = [arg for arg in args if arg.startswith("-gencode=")]
            self.assertEqual(len(profiles), 8)
            for arch in ("75", "80", "89", "90", "100", "110", "120"):
                self.assertIn(f"-gencode=arch=compute_{arch},code=sm_{arch}", profiles)
            self.assertIn("-gencode=arch=compute_75,code=compute_75", profiles)
            pathlib.Path(scratch.read_text().strip()).relative_to(SCRIPT.parents[4] / ".zig-cache" / "cuda-artifacts")

    def test_default_zig_resolution_prefers_the_repo_pinned_toolchain(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        pinned = 'elif [ -x "$repo_dir/.tools/zig-x86_64-linux-0.17.0/zig" ]'
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
                self.assertEqual(
                    1, source.count(f'check_required_symbols "${artifact}"')
                )


if __name__ == "__main__":
    unittest.main()
