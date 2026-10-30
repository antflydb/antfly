import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest
import json


ROOT = Path(__file__).resolve().parents[5]
SCRIPTS = ROOT / "zig/pkg/inference/scripts/gemma4"
STAGING = Path(__file__).resolve().parent
sys.path[:0] = [str(STAGING), str(SCRIPTS)]


def load(name):
    spec = importlib.util.spec_from_file_location(name, STAGING / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


cancellation = load("qualify_native_cancellation")
sse_abort = load("qualify_native_sse_abort")
import qualify_native_checkpoint_http as http


class ReliabilityMetalTests(unittest.TestCase):
    def test_legacy_cuda_header_attestation_accepts_two_arguments(self):
        response = types.SimpleNamespace(getheaders=lambda: [
            ("X-Antfly-Inference-Backend", "cuda"),
            ("X-Antfly-Inference-Pid", "17"),
        ])
        headers = http.response_headers(response, {"worker_pid": 17})
        self.assertEqual(headers["x-antfly-inference-backend"], "cuda")

    def test_metal_header_attestation_requires_the_selected_backend(self):
        response = types.SimpleNamespace(getheaders=lambda: [
            ("X-Antfly-Inference-Backend", "metal"),
            ("X-Antfly-Inference-Pid", "17"),
        ])
        record = {"worker_pid": 17}
        headers = http.response_headers(response, record, types.SimpleNamespace(backend="metal"))
        self.assertEqual(headers["x-antfly-inference-backend"], "metal")
        with self.assertRaisesRegex(AssertionError, "missing cuda attestation"):
            http.response_headers(response, record)

    def test_current_prefill_marker_uses_the_actual_ledger_prefix(self):
        row = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "phase": "prefill_started", "request": 7, "owner_id": 9,
            "owner_available": True, "physical_available": True,
            "combined_available": True,
        }
        with tempfile.TemporaryDirectory(dir=ROOT / ".qualification") as directory:
            log = Path(directory) / "worker.log"
            log.write_text("old diagnostic\n")
            offset = log.stat().st_size
            with log.open("a") as output:
                output.write("metal_checkpoint_qualification " + json.dumps(row) + "\n")
            actual = cancellation.wait_prefill_route(
                types.SimpleNamespace(backend="metal"), log, offset, .2,
            )
        self.assertEqual(actual, row)

    def test_current_test_runner_fitting_diagnostics_preserve_strict_counts(self):
        data = (
            b"[default] (err): attention_match_fit_error backend=metal error=Cancelled\n"
            b"[default] (err): attention_match_fit_rejected backend=metal phase=fit elapsed_ns=9 "
            b"donor_layer=0 head=1 chunk_start=0 attempted_chunks=2 completed_chunks=1 "
            b"mass_error=0 output_rmse=0 nnls_sweeps=1 validating_storage=0 "
            b"rounded_mass_error=0 rounded_output_rmse=0 validating_head=0 validated_heads=0\n"
        )
        proof = cancellation.fitting_cancellation_evidence(data, "metal")
        self.assertEqual(proof["attempted_chunks"], 2)
        self.assertEqual(proof["completed_chunks"], 1)
        with self.assertRaisesRegex(AssertionError, "ambiguous"):
            cancellation.fitting_cancellation_evidence(data + data, "metal")

    def test_metal_prefill_requires_fresh_positive_marker(self):
        args = types.SimpleNamespace(backend="metal")
        with tempfile.TemporaryDirectory(dir=ROOT / ".qualification") as directory:
            log = Path(directory) / "worker.log"
            log.write_text("old\n")
            offset = log.stat().st_size
            ledger = {
                "schema": "antfly.metal-checkpoint-phase.v1",
                "phase": "prefill_started",
                "request": 7,
                "owner_id": 9,
                "owner_available": True,
                "physical_available": True,
                "combined_available": True,
            }
            with log.open("a") as output:
                output.write(__import__("json").dumps(ledger) + "\n")
            marker = cancellation.wait_prefill_route(args, log, offset, 0.2)
        self.assertEqual(marker, ledger)

    def test_fitting_evidence_is_backend_neutral_and_strict(self):
        data = (
            b"error: attention_match_fit_error backend=metal error=Cancelled\n"
            b"error: attention_match_fit_rejected backend=metal phase=fit elapsed_ns=9 "
            b"donor_layer=0 head=1 chunk_start=0 attempted_chunks=2 "
            b"completed_chunks=1 mass_error=0 output_rmse=0 nnls_sweeps=1 "
            b"validating_storage=0 rounded_mass_error=0 rounded_output_rmse=0 "
            b"validating_head=0 validated_heads=0\n"
        )
        proof = cancellation.fitting_cancellation_evidence(data, "metal")
        self.assertEqual(proof["attempted_chunks"], 2)
        with self.assertRaisesRegex(AssertionError, "fresh metal fitting"):
            cancellation.fitting_cancellation_evidence(b"", "metal")

    def test_tracked_metal_source_owns_fitting_telemetry(self):
        source = (ROOT / "zig/pkg/inference/src/pipelines/generation.zig").read_text()
        metal = source.split("fn prepareMetalAttentionMatchFit(", 1)[1].split(
            "fn probeAttentionMatchFit(", 1
        )[0]
        self.assertIn("attention_match_fit_rejected backend=metal", metal)
        self.assertIn("attention_match_fit_error backend=metal", metal)
        self.assertIn('phase = "fit";', metal)
        self.assertIn("binding_representation", source)
        self.assertIn('"prefill_started"', source)

    def test_metal_memory_dispatch_preserves_unified_guard(self):
        args = types.SimpleNamespace(backend="metal")
        valid = {
            "scope": "unified_process_group",
            "max_combined_rss_bytes": 8 * 1024**3,
            "samples": 2,
            "errors": [],
            "peak_group_rss_bytes": 1,
            "minimum_free_memory_percent": 15,
            "minimum_disk_free_mib": 4096,
            "swapout_growth_bytes": 0,
        }
        cancellation.validate_campaign_memory(args, valid)
        invalid = dict(valid, swapout_growth_bytes=1)
        with self.assertRaisesRegex(AssertionError, "unified-memory"):
            cancellation.validate_campaign_memory(args, invalid)

    def test_cuda_memory_gates_are_unchanged(self):
        args = types.SimpleNamespace(backend="cuda")
        valid = {
            "samples": 2,
            "errors": [],
            "gpu_uuids": ["GPU-test"],
            "peak_host_rss_bytes": 1,
            "peak_gpu_memory_bytes": 1,
        }
        cancellation.validate_campaign_memory(args, valid)
        with self.assertRaisesRegex(AssertionError, "memory cap"):
            cancellation.validate_campaign_memory(
                args, dict(valid, peak_host_rss_bytes=8 * 1024**3 + 1)
            )

    def test_sse_parser_rejects_terminal_event_before_content(self):
        response = types.SimpleNamespace(readline=lambda size: b"data: [DONE]\n")
        with self.assertRaisesRegex(AssertionError, "completed before content"):
            sse_abort.first_content_delta(response)


if __name__ == "__main__":
    unittest.main()
