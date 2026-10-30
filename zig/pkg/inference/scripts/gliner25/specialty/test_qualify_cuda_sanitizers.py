# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hermetic tests for fail-closed sanitizer evidence parsing; never uses CUDA."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest

import qualify_cuda_sanitizers as gate

FILTER = gate.SUITES["gru"]["filter"]
TEST_OUTPUT = f"1/316 ops.cuda.cuda_compute.test.{FILTER}...OK\n1 selected; 1 passed; 0 skipped.\n"
BANNER = "========= COMPUTE-SANITIZER\n"
CLEAN = "========= LEAK SUMMARY: 0 bytes leaked in 0 allocations\n========= ERROR SUMMARY: 0 errors\n"
CLEAN_RACE = "========= RACECHECK SUMMARY: 0 hazards displayed (0 errors, 0 warnings)\n"


class SanitizerEvidenceTests(unittest.TestCase):
    def assess(self, text, tool="memcheck", returncode=0, timed_out=False):
        return gate.assess_log(text, tool, FILTER, returncode, timed_out)

    def test_requires_executed_test_and_explicit_clean_tool_summary(self):
        for tool in gate.TOOLS:
            with self.subTest(tool=tool):
                clean = CLEAN_RACE if tool == "racecheck" else CLEAN
                self.assertTrue(self.assess(BANNER + TEST_OUTPUT + clean, tool)["pass"])
                self.assertFalse(self.assess(BANNER + TEST_OUTPUT, tool)["pass"])
                self.assertFalse(self.assess(BANNER + clean, tool)["pass"])
                self.assertFalse(self.assess(TEST_OUTPUT + clean, tool)["pass"])

    def test_rejects_skips_empty_selection_and_ambiguous_test_execution(self):
        for summary in ["1 selected; 0 passed; 1 skipped.\n", "0 selected; 0 passed; 0 skipped.\n",
                        "2 selected; 2 passed; 0 skipped.\n", "1 selected; 1 passed; 0 skipped; 0 failed; 1 leaked.\n"]:
            text = BANNER + TEST_OUTPUT.replace("1 selected; 1 passed; 0 skipped.\n", summary) + CLEAN
            self.assertFalse(self.assess(text)["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + TEST_OUTPUT + CLEAN)["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT.replace(FILTER, "unrelated native test") + CLEAN)["pass"])

    def test_rejects_error_hidden_by_clean_summary_or_zero_exit(self):
        for dirty in ["========= ERROR SUMMARY: 2 errors\n", "========= Warning: device not supported\n",
                      "========= Error: target application terminated before first instrumented API call\n"]:
            self.assertFalse(self.assess(BANNER + TEST_OUTPUT + dirty + CLEAN)["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + CLEAN, returncode=99)["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + CLEAN, timed_out=True)["pass"])

    def test_memcheck_requires_explicit_zero_byte_and_allocation_leak_summary(self):
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + "========= ERROR SUMMARY: 0 errors\n")["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + CLEAN.replace("0 bytes leaked", "4 bytes leaked"))["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + CLEAN.replace("0 allocations", "1 allocation"))["pass"])

    def test_racecheck_requires_zero_hazards_and_warnings(self):
        for dirty in ["========= RACECHECK SUMMARY: 1 hazard displayed (0 errors, 1 warning)\n",
                      "========= RACECHECK SUMMARY: 1 hazard displayed (1 error, 0 warnings)\n"]:
            self.assertFalse(self.assess(BANNER + TEST_OUTPUT + dirty + CLEAN_RACE, "racecheck")["pass"])
        self.assertFalse(self.assess(BANNER + TEST_OUTPUT + CLEAN, "racecheck")["pass"])

    def test_commands_instrument_all_cuda_without_kernel_or_launch_skips(self):
        for tool in gate.TOOLS:
            command = gate.sanitizer_command(Path("/sanitizer"), tool, Path("/test"), FILTER)
            self.assertIn("--require-cuda-init", command)
            self.assertEqual("all", command[command.index("--target-processes") + 1])
            self.assertEqual("99", command[command.index("--error-exitcode") + 1])
            self.assertEqual(["/test", "--test-filter", FILTER], command[-3:])
            for forbidden in ["--kernel-name", "--launch-skip", "--launch-count", "--suppressions"]:
                self.assertNotIn(forbidden, command)
            if tool == "memcheck":
                self.assertEqual("full", command[command.index("--leak-check") + 1])

    def test_http_receipts_require_gpu_dispatch_and_memory_within_reservation(self):
        receipt = {"profile": "pii", "backend": "cuda", "cases": 12, "kernel_launches": 1,
                   "gru_calls": 2, "gru_no_residual_calls": 2,
                   "initial_device_memory": {"live": 100, "peak": 120},
                   "final_device_memory": {"live": 130, "peak": 150},
                   "max_reserved_scratch_bytes": 100, "reserved_physical_peak_bytes": 200,
                   "idle_admission": {"host_scratch_bytes": 0, "backend_scratch_bytes": 0}}
        gate.validate_http_receipt(receipt, "pii", 12)
        for key, value in [("backend", "native"), ("kernel_launches", 0), ("gru_no_residual_calls", 0),
                           ("cases", 1), ("reserved_physical_peak_bytes", 140)]:
            changed = copy.deepcopy(receipt)
            changed[key] = value
            with self.assertRaises(ValueError):
                gate.validate_http_receipt(changed, "pii", 12)
        changed = copy.deepcopy(receipt)
        changed["profile"] = "gliguard"
        changed["cases"] = 11
        changed["gru_calls"] = changed["gru_no_residual_calls"] = 0
        gate.validate_http_receipt(changed, "gliguard", 11)
        changed["gru_calls"] = 1
        with self.assertRaises(ValueError):
            gate.validate_http_receipt(changed, "gliguard", 11)

    def test_timeout_terminates_and_waits_for_owned_process_group(self):
        # This is an ordinary sleeping Python process, never a CUDA workload.
        with tempfile.TemporaryDirectory() as directory:
            receipt = gate.run_logged([sys.executable, "-c", "import time; time.sleep(60)"],
                Path(directory), dict(os.environ), Path(directory) / "timeout.log", .05)
        self.assertTrue(receipt["timed_out"])
        self.assertNotEqual(0, receipt["returncode"])
        self.assertLess(receipt["elapsed_seconds"], 5)


class KernelScopeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.binary = Path(self.temporary.name) / 'test-binary'
        self.symbols = {name for values in gate.REQUIRED_KERNELS.values() for name in values}
        self.binary.write_bytes(b'\0'.join(name.encode() for name in sorted(self.symbols)))

    def test_filter_is_explicit_correct_tool_syntax_and_defaults_remain_unfiltered(self):
        command = gate.sanitizer_command(Path('/sanitizer'), 'racecheck', self.binary, FILTER, '^termite_.*')
        self.assertEqual('regex=^termite_.*', command[command.index('--kernel-name') + 1])
        self.assertIn('--print-session-details', command)
        default = gate.sanitizer_command(Path('/sanitizer'), 'racecheck', self.binary, FILTER)
        self.assertNotIn('--kernel-name', default)

    def test_rejects_bad_or_nonportable_regex_and_patterns_excluding_required_launches(self):
        for pattern in ['', '[', '(?i)termite', r'\Atermite', 'termite\n', 'termite\0', '.*+']:
            with self.subTest(pattern=pattern):
                with self.assertRaises(ValueError):
                    gate.compile_kernel_regex(pattern)
        for pattern in ['never_matches_any_kernel', '^vendor_.*', '^termite_gliner_gru_combine_f32$']:
            with self.subTest(pattern=pattern):
                with self.assertRaises(ValueError):
                    gate.kernel_scope(pattern, gate.BASE_SUITES, self.symbols, self.binary)
        scope = gate.kernel_scope('^termite_gliner_gru_combine_f32$', ['gru'], self.symbols, self.binary)
        self.assertEqual(['termite_gliner_gru_combine_f32'], scope['required_symbols_by_suite']['gru'])

    def test_project_filter_matches_real_ptx_entry_inventory_and_requires_embedded_names(self):
        inventory = gate.ptx_kernel_symbols((gate.PACKAGE / 'src/ops/cuda/artifacts/inference_cuda_kernels.ptx').read_text())
        self.assertTrue(self.symbols.issubset(inventory))
        scope = gate.kernel_scope('^termite_.*', gate.SUITES, inventory, self.binary)
        self.assertFalse(scope['all_kernel_coverage_claimed'])
        self.assertIn('vendor-library coverage is not claimed', scope['vendor_library_scope'])
        self.binary.write_bytes(b'no CUDA entry names')
        with self.assertRaisesRegex(ValueError, 'absent from test binary'):
            gate.kernel_scope('^termite_.*', ['gru'], inventory, self.binary)
        with self.assertRaisesRegex(ValueError, 'absent from PTX'):
            gate.kernel_scope('^termite_.*', ['gru'], set(), self.binary)

    def test_filtered_scope_never_claims_all_kernel_or_complete_campaign_coverage(self):
        for pattern in ['^termite_.*', '.*']:
            coverage = gate.coverage_flags(True, gate.TOOLS, gate.SUITES, None, pattern)
            self.assertTrue(coverage['full_tool_coverage'])
            self.assertTrue(coverage['full_head_case_coverage'])
            self.assertFalse(coverage['all_kernel_coverage'])
            self.assertFalse(coverage['vendor_library_coverage_claimed'])
            self.assertFalse(coverage['complete_sanitizer_campaign'])
        self.assertTrue(gate.coverage_flags(True, gate.TOOLS, gate.SUITES, None, None)['complete_sanitizer_campaign'])
        self.assertFalse(gate.coverage_flags(False, gate.TOOLS, gate.SUITES, None, None)['complete_sanitizer_campaign'])

    def test_missing_runtime_kernel_listing_is_unavailable_not_zero_or_invented(self):
        receipt = gate.checked_kernel_log_receipt(BANNER + TEST_OUTPUT + CLEAN_RACE, '^termite_.*')
        self.assertFalse(receipt['available'])
        self.assertIsNone(receipt['kernel_names'])
        receipt = gate.checked_kernel_log_receipt('========= Checking kernel termite_gliner_gru_combine_f32\n', '^termite_.*')
        self.assertEqual(['termite_gliner_gru_combine_f32'], receipt['kernel_names'])
        with self.assertRaises(ValueError):
            gate.checked_kernel_log_receipt('========= Instrumenting kernel vendor_matmul\n', '^termite_.*')

    def test_invalid_ignored_filter_and_zero_checked_kernel_diagnostics_cannot_pass(self):
        for diagnostic in ['========= Invalid kernel filter; ignored\n',
                           '========= kernel regex is unsupported\n', '========= No kernels were checked\n']:
            result = gate.assess_log(BANNER + TEST_OUTPUT + diagnostic + CLEAN_RACE, 'racecheck', FILTER, 0)
            self.assertFalse(result['pass'])


if __name__ == "__main__":
    unittest.main()
