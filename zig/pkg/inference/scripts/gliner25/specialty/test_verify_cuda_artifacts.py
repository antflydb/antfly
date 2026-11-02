# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Artifact evidence must distinguish a real target body from symbols or stubs."""
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import verify_cuda_artifacts as gate


def sass(arch="sm_80", omitted=None, stub=None):
    lines = [f"arch = {arch}"]
    for kernel in gate.KERNELS:
        if kernel != omitted:
            lines.append(f"Function : {kernel}")
            for i in range(8):
                instruction = 'STG.E [R0], R1' if i == 6 and kernel != stub else 'NOP'
                lines.append(f"/*{i * 16:04x}*/ {instruction};")
    return "\n".join(lines)


class ArtifactEvidenceTests(unittest.TestCase):
    def check(self, text):
        with mock.patch.object(gate.subprocess, "run", return_value=SimpleNamespace(stdout=text)):
            return gate.check_image(Path("cuobjdump"), Path("kernels.fatbin"), "sm_80")

    def test_requires_every_specialty_kernel_in_the_requested_architecture(self):
        self.check(sass().replace("arch = sm_80", "code for sm_80"))
        result = self.check(sass())
        self.assertEqual(set(gate.KERNELS), set(result["instruction_counts"]))
        with self.assertRaisesRegex(ValueError, "missing sm_80 image"):
            self.check(sass(arch="sm_89"))

    def test_rejects_missing_or_stubbed_gru_instead_of_accepting_symbol_presence(self):
        kernel = "termite_gliner_gru_combine_f32"
        for text in (sass(omitted=kernel), sass(stub=kernel)):
            with self.assertRaisesRegex(ValueError, "missing or stub kernel " + kernel):
                self.check(text)


if __name__ == "__main__":
    unittest.main()
