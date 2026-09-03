#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent


def load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


profile = load("profile_metal_gemma4_a4b", "profile_metal_gemma4_a4b.py")
parity = load("benchmark_metal_gemma4_parity", "benchmark_metal_gemma4_parity.py")


class A4BProfileParserTest(unittest.TestCase):
    def write_log(self, body: str) -> Path:
        temporary = tempfile.NamedTemporaryFile("w", delete=False)
        temporary.write(body)
        temporary.close()
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        return Path(temporary.name)

    @staticmethod
    def legacy_prefix(*, frame_gpu: int = 100) -> str:
        if frame_gpu == 100:
            components = (
                "attention=10 ffn_unclassified=20 ple=0 tail=10 embedding=0 other=0 "
                "moe_gate_up=25 moe_activation=5 moe_down=25 moe_reduce=5"
            )
        elif frame_gpu == 1_000:
            components = (
                "attention=100 ffn_unclassified=100 ple=50 tail=50 embedding=0 other=100 "
                "moe_gate_up=200 moe_activation=100 moe_down=200 moe_reduce=100"
            )
        else:
            raise AssertionError("unsupported test frame")
        return (
            "metal_a4b_specialized_id: enabled=1\n"
            "token_ids: 1 2 3\n"
            f"metal_stage_detail_ns: regime=decode layer=15 frame_gpu={frame_gpu} "
            f"{components}\n"
            "metal_dispatch_profile_pipeline: rank=0 dispatches=30 threadgroups=1 "
            "threads=0 grid_items=1 label=termite_q4_0_linear_id_a4b_gate_up\n"
            "metal_dispatch_profile_pipeline: rank=1 dispatches=30 threadgroups=1 "
            "threads=0 grid_items=1 label=termite_q4_0_linear_id_a4b_down\n"
        )

    @staticmethod
    def roofline_suffix(*, frame_gpu: int = 1_000, unattributed: int = 100) -> str:
        return (
            "metal_roofline_op: schema=antfly.metal_roofline_op.v1 regime=decode "
            "frame=8 layer=0 layer_kind=local kv_position=29 op=attention_q "
            "shape=1x2816x4096 gpu_ns=600 logical_bytes=6000 dispatches=1 "
            "pipeline=termite_q4_0_linear_1x_reduce barrier_before=0 "
            "barrier_after=1 barrier_scope=encoder barrier_reason=dependency\n"
            "metal_roofline_op: schema=antfly.metal_roofline_op.v1 regime=decode "
            "frame=8 layer=5 layer_kind=global kv_position=29 op=attention_q_global "
            "shape=1x2816x8192 gpu_ns=300 logical_bytes=6000 dispatches=1 "
            "pipeline=termite_q4_0_linear_1x_reduce barrier_before=0 "
            "barrier_after=0 barrier_scope=none barrier_reason=none source=capture\n"
            "metal_roofline_frame: schema=antfly.metal_roofline_frame.v1 regime=decode "
            f"frame=8 frame_gpu_ns={frame_gpu} unattributed_gpu_ns={unattributed} "
            "barrier_count=1 planned_barrier_count=1 nonplanned_barrier_count=0 "
            "timing=frame_apportioned_encoder_counter_ticks "
            "logical_convention=encoded_source_access_estimate\n"
        )

    def test_specialized_detail_record_is_fail_closed(self) -> None:
        path = self.write_log(self.legacy_prefix())
        parsed = profile.parse_log(path, 15, True)
        self.assertEqual(parsed["token_count"], 3)
        self.assertEqual(parsed["samples"][0]["moe_down"], 25)
        self.assertNotIn("roofline", parsed)

    def test_legacy_summary_retains_v1_schema(self) -> None:
        parsed = profile.parse_log(self.write_log(self.legacy_prefix()), 15, True)
        summary = profile.summarize([parsed], True)
        self.assertEqual(summary["schema"], "antfly.gemma4_a4b_metal_profile.v1")
        self.assertNotIn("roofline", summary)

    def test_live_runner_requires_complete_roofline_capture(self) -> None:
        body = (
            self.legacy_prefix()
            + "metal-roofline: drop frame=8 reason=invalid-capture "
            "invalid_reason=op-shape valid=0 regime=2 has_kv=1 "
            "barrier_hooks=1 ops=23 barriers=0\n"
        )
        with self.assertRaisesRegex(
            profile.ProfileError,
            "no complete roofline capture.*invalid_reason=op-shape",
        ):
            profile.parse_log(
                self.write_log(body),
                15,
                True,
                require_roofline=True,
            )

    def test_runtime_roofline_contract_is_opt_in_and_fail_closed(self) -> None:
        script_source = (SCRIPT_DIR / "profile_metal_gemma4_a4b.py").read_text()
        metal_source = (
            SCRIPT_DIR.parents[1] / "src" / "backends" / "metal_kernels.m"
        ).read_text()
        zig_source = (
            SCRIPT_DIR.parents[1] / "src" / "ops" / "metal_compute.zig"
        ).read_text()
        self.assertIn('"TERMITE_METAL_STAGE_TIMING_ROOFLINE": "1"', script_source)
        self.assertIn('"TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE": "1"', script_source)
        self.assertIn('"--mode",\n        "compiled"', script_source)
        self.assertIn('"--compiled-target",\n        "whole-model"', script_source)
        self.assertIn('getenv("TERMITE_METAL_STAGE_TIMING_ROOFLINE")', metal_source)
        self.assertIn(
            "allow_roofline && regime == TERMITE_METAL_FRAME_REGIME_DECODE",
            metal_source,
        )
        self.assertIn(
            "metal_roofline_op: schema=antfly.metal_roofline_op.v1", metal_source
        )
        self.assertIn(
            "metal_roofline_frame: schema=antfly.metal_roofline_frame.v1", metal_source
        )
        for reason in (
            "incomplete-layer-coverage",
            "incomplete-layer-operations",
            "incomplete-model-tail",
            "barrier-reconciliation",
            "zero-operation-time",
        ):
            self.assertIn(reason, metal_source)
        for op in (
            ".token_embedding",
            ".kv_write",
            ".paged_attention",
            ".attention_output_linear",
            ".parallel_ffn_post_residual",
            ".layer_output_scale",
        ):
            self.assertIn(op, zig_source)
        for op in (
            "TERMITE_METAL_ROOFLINE_OP_MOE_GATE_UP",
            "TERMITE_METAL_ROOFLINE_OP_MOE_ACTIVATION",
            "TERMITE_METAL_ROOFLINE_OP_MOE_DOWN",
            "TERMITE_METAL_ROOFLINE_OP_MOE_REDUCE",
            "TERMITE_METAL_ROOFLINE_OP_TAIL_FINAL_NORM",
            "TERMITE_METAL_ROOFLINE_OP_TAIL_LM_HEAD",
            "TERMITE_METAL_ROOFLINE_OP_TAIL_ARGMAX",
        ):
            self.assertIn(op, metal_source)

    def test_roofline_ledger_parses_shape_rate_barriers_and_coverage(self) -> None:
        path = self.write_log(
            self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()
        )
        parsed = profile.parse_log(path, 15, True)
        roofline = parsed["roofline"]
        self.assertEqual(roofline["schema"], "antfly.gemma4_a4b_roofline_ledger.v1")
        self.assertEqual(roofline["operations"][0]["shape_dimensions"], [1, 2816, 4096])
        self.assertEqual(roofline["operations"][0]["gpu_us"], 0.6)
        self.assertEqual(roofline["operations"][0]["effective_gbps"], 10.0)
        self.assertEqual(roofline["operations"][1]["metadata"], {"source": "capture"})
        self.assertEqual(roofline["coverage"]["layers"], [0, 5])
        self.assertEqual(roofline["coverage"]["local_layers"], [0])
        self.assertEqual(roofline["coverage"]["global_layers"], [5])
        self.assertEqual(roofline["coverage"]["kv_positions"], [29])
        self.assertFalse(roofline["coverage"]["complete_30_layer_coverage"])
        self.assertEqual(roofline["barriers"]["count"], 1)
        self.assertEqual(roofline["barriers"]["before_count"], 0)
        self.assertEqual(roofline["barriers"]["after_count"], 1)
        self.assertEqual(roofline["barriers"]["reasons"], {"dependency": 1})
        self.assertEqual(roofline["reconciliation"]["delta_ns"], 0)
        self.assertTrue(roofline["reconciliation"]["legacy_stage_detail_crosscheck"])
        self.assertFalse(roofline["timing_contract"]["direct_per_operation_timestamps"])
        self.assertEqual(
            roofline["timing_contract"]["methods"],
            ["frame_apportioned_encoder_counter_ticks"],
        )

    def test_roofline_summary_uses_v2_and_weighted_totals(self) -> None:
        first = profile.parse_log(
            self.write_log(
                self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()
            ),
            15,
            True,
        )
        second = profile.parse_log(
            self.write_log(
                self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()
            ),
            15,
            True,
        )
        first["label"] = "first"
        second["label"] = "second"
        summary = profile.summarize([first, second], True)
        self.assertEqual(summary["schema"], "antfly.gemma4_a4b_metal_profile.v2")
        roofline = summary["roofline"]
        self.assertFalse(roofline["timing_contract"]["direct_per_operation_timestamps"])
        self.assertEqual(roofline["coverage"]["sample_count"], 2)
        self.assertEqual(roofline["coverage"]["frame_count"], 2)
        self.assertEqual(roofline["reconciliation"]["frame_gpu_ns"], 2_000)
        self.assertEqual(roofline["reconciliation"]["delta_ns"], 0)
        self.assertEqual(
            roofline["reconciliation"]["operation_accounted_fraction"], 0.9
        )
        local_total = next(
            total
            for total in roofline["operation_totals"]
            if total["op"] == "attention_q"
        )
        self.assertEqual(local_total["occurrences"], 2)
        self.assertEqual(local_total["dispatches"], 2)
        self.assertEqual(local_total["gpu_us"], 1.2)
        self.assertEqual(local_total["median_gpu_us"], 0.6)
        self.assertEqual(local_total["effective_gbps"], 10.0)
        self.assertEqual(local_total["layers"], [0])
        self.assertEqual(local_total["kv_positions"], [29])
        json.dumps(summary, allow_nan=False)

    def test_roofline_timing_must_reconcile(self) -> None:
        path = self.write_log(
            self.legacy_prefix(frame_gpu=1_000)
            + self.roofline_suffix(frame_gpu=1_001, unattributed=100)
        )
        with self.assertRaisesRegex(profile.ProfileError, "timing does not reconcile"):
            profile.parse_log(path, 15, True)

    def test_roofline_barrier_count_must_reconcile(self) -> None:
        body = (self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()).replace(
            "barrier_count=1", "barrier_count=0"
        )
        with self.assertRaisesRegex(
            profile.ProfileError, "barrier count does not reconcile"
        ):
            profile.parse_log(self.write_log(body), 15, True)

    def test_roofline_barrier_metadata_is_fail_closed(self) -> None:
        fields = profile.parse_fields(
            "schema=antfly.metal_roofline_op.v1 regime=decode frame=8 layer=0 "
            "layer_kind=local kv_position=29 op=attention_q shape=1x2816x4096 "
            "gpu_ns=1 logical_bytes=1 dispatches=1 pipeline=p barrier_before=1 "
            "barrier_after=0 barrier_scope=none barrier_reason=none"
        )
        with self.assertRaisesRegex(
            profile.ProfileError, "scope and reason are required"
        ):
            profile.parse_roofline_operation(fields, Path("test.log"))

    def test_roofline_frame_rejects_mixed_kv_positions(self) -> None:
        body = (self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()).replace(
            "frame=8 layer=5 layer_kind=global kv_position=29",
            "frame=8 layer=5 layer_kind=global kv_position=30",
        )
        with self.assertRaisesRegex(profile.ProfileError, "mixes KV positions"):
            profile.parse_log(self.write_log(body), 15, True)

    def test_roofline_crosschecks_legacy_frame_gpu_time(self) -> None:
        path = self.write_log(self.legacy_prefix() + self.roofline_suffix())
        with self.assertRaisesRegex(profile.ProfileError, "does not match legacy"):
            profile.parse_log(path, 15, True)

    def test_roofline_rejects_layer_kind_and_shape_drift(self) -> None:
        base_fields = profile.parse_fields(
            "schema=antfly.metal_roofline_op.v1 regime=decode frame=8 layer=5 "
            "layer_kind=local kv_position=29 op=attention_q shape=1x2816x8192 "
            "gpu_ns=1 logical_bytes=1 dispatches=1 pipeline=p barrier_before=0 "
            "barrier_after=0 barrier_scope=none barrier_reason=none"
        )
        with self.assertRaisesRegex(profile.ProfileError, "requires layer_kind=global"):
            profile.parse_roofline_operation(base_fields, Path("test.log"))
        base_fields["layer_kind"] = "global"
        base_fields["shape"] = "2816-by-8192"
        with self.assertRaisesRegex(profile.ProfileError, "invalid roofline shape"):
            profile.parse_roofline_operation(base_fields, Path("test.log"))

    def test_roofline_rejects_unsupported_schema_and_partial_records(self) -> None:
        fields = profile.parse_fields(
            "schema=antfly.metal_roofline_op.v9 regime=decode frame=8 layer=0 "
            "layer_kind=local kv_position=29 op=attention_q shape=1x2816x4096 "
            "gpu_ns=1 logical_bytes=1 dispatches=1 pipeline=p barrier_before=0 "
            "barrier_after=0 barrier_scope=none barrier_reason=none"
        )
        with self.assertRaisesRegex(
            profile.ProfileError, "unsupported roofline operation"
        ):
            profile.parse_roofline_operation(fields, Path("test.log"))
        body = (
            self.legacy_prefix(frame_gpu=1_000)
            + self.roofline_suffix().split("metal_roofline_frame:")[0]
        )
        with self.assertRaisesRegex(profile.ProfileError, "records are incomplete"):
            profile.parse_log(self.write_log(body), 15, True)

    def test_summary_rejects_mixed_legacy_and_roofline_formats(self) -> None:
        legacy = profile.parse_log(self.write_log(self.legacy_prefix()), 15, True)
        roofline = profile.parse_log(
            self.write_log(
                self.legacy_prefix(frame_gpu=1_000) + self.roofline_suffix()
            ),
            15,
            True,
        )
        with self.assertRaisesRegex(profile.ProfileError, "mixed legacy and roofline"):
            profile.summarize([legacy, roofline], True)

    def test_detail_record_must_reconcile(self) -> None:
        path = self.write_log(
            "token_ids: 1 2\n"
            "metal_stage_detail_ns: regime=decode layer=0 frame_gpu=99 "
            "attention=10 ffn_unclassified=20 ple=0 tail=10 embedding=0 other=0 "
            "moe_gate_up=25 moe_activation=5 moe_down=25 moe_reduce=5\n"
            "metal_dispatch_profile_pipeline: rank=0 dispatches=60 threadgroups=1 "
            "threads=0 grid_items=1 label=termite_q4_0_linear_id\n"
        )
        with self.assertRaises(profile.ProfileError):
            profile.parse_log(path, 0, False)

    def test_summary_requires_deterministic_tokens(self) -> None:
        first = {
            "token_ids_sha256": "a",
            "token_count": 2,
            "samples": [
                {
                    "moe_gate_up": 10,
                    "moe_activation": 1,
                    "moe_down": 9,
                    "moe_reduce": 1,
                }
            ],
        }
        second = {**first, "token_ids_sha256": "b"}
        with self.assertRaises(profile.ProfileError):
            profile.summarize([first, second], True)

    def test_parity_token_contract(self) -> None:
        path = self.write_log("token_ids: 10 20 30\n")
        count, digest = parity.token_contract(path.read_text(), path)
        self.assertEqual(count, 3)
        self.assertEqual(len(digest), 64)

    def test_parity_harness_separates_default_and_prepared_a4b(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            out_dir = Path(temporary)
            args = type(
                "Args",
                (),
                {
                    "out_dir": out_dir,
                    "antfly_bin": Path("/tmp/antfly-inference"),
                    "output_tokens": 3,
                    "control_output_tokens": 3,
                    "budget_mb": 16_384,
                    "max_a4b_rss_mb": 18_000.0,
                },
            )()
            captured_environments: list[dict[str, str]] = []

            def fake_run(command, *, env, stdout, **_kwargs):
                captured_environments.append(env)
                prepared = "TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE" in env
                json_path = Path(command[command.index("--json-timing") + 1])
                payload = {
                    "backend": "metal",
                    "finish_reason": "length",
                    "tokens": 3,
                    "token_ids": [1, 2, 3],
                    "timing_ms": {"decode_inner": 50.0},
                    "metal": {
                        "resident_mapped": {
                            "dispatches": {"down": 1, "reduce": 1, "fused_gate_up": 2},
                            "model_buffer": {
                                "prepare_successes": 1,
                                "prepare_failures": 0,
                            },
                            "residency_set": {"allocated_bytes": 14_937_423_872},
                        },
                        "residency": {
                            "runtime_mapped_fallbacks": 0,
                            "runtime_mapped_failures": 0,
                        },
                        "q4_0_policy": {"mmv_variant_fallbacks": 0},
                        "attention_dispatch": {
                            "paged_1x": 0,
                            "decode_gqa_split": 0,
                            "generated_flash_prefill": 0,
                            "generated_flash_prefill_hd512": 5,
                        },
                        "frame_fallbacks": {
                            "decode_fallback": 0,
                            "decode_success": 3 if prepared else 0,
                        },
                        "prepared_frame": {
                            "fast_path": 3 if prepared else 0,
                            "fallback": 0,
                        },
                    },
                }
                json_path.write_text(json.dumps(payload))
                stdout.write(
                    "token_ids: 1 2 3\n"
                    "metal_a4b_specialized_id: enabled=1\n"
                    "metal_a4b_route_select_tg: enabled=1\n"
                    "metal_a4b_lm_head_nbodd: enabled=1\n"
                    "metal_a4b_argmax_tg: enabled=1\n"
                    "metal_a4b_decode_gqa_split_frame_scratch: enabled=1 slots=2 "
                    "rollback=TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH\n"
                    "metal_a4b_route_select_register: enabled=1 barriers=3 class=A "
                    "rollback=TERMITE_METAL_DISABLE_A4B_ROUTE_SELECT_REGISTER\n"
                    "metal_a4b_lm_head_nr4_nsg1: enabled=1 rows_per_tg=4 class=B "
                    "rollback=TERMITE_METAL_DISABLE_A4B_LM_HEAD_NR4_NSG1\n"
                    + (
                        "metal_pipelined_decode_frame: enabled=1 owner=executor "
                        "rollback=TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME\n"
                        "metal_a4b_concurrent_hazard: enabled=1 dispatch=concurrent "
                        "barrier_mode=range_tracked "
                        "rollback=TERMITE_METAL_DISABLE_A4B_CONCURRENT_HAZARD\n"
                        "generate-setup: live whole-model executor handled request\n"
                        if prepared
                        else "generate-setup: live whole-model executor skipped\n"
                    )
                    + "1048576 maximum resident set size\n"
                )
                return type("Completed", (), {"returncode": 0})()

            with mock.patch.object(parity.subprocess, "run", side_effect=fake_run):
                baseline = parity.run_antfly(
                    args,
                    label="baseline",
                    model=Path("/tmp/model.gguf"),
                    prompt="prompt",
                    a4b=True,
                    specialized=True,
                )
                prepared = parity.run_antfly(
                    args,
                    label="prepared",
                    model=Path("/tmp/model.gguf"),
                    prompt="prompt",
                    a4b=True,
                    specialized=True,
                    prepared_a4b=True,
                )

            self.assertFalse(baseline["prepared_a4b"])
            self.assertTrue(prepared["prepared_a4b"])
            self.assertNotIn(
                "TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE",
                captured_environments[0],
            )
            self.assertEqual(
                captured_environments[1]["TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE"],
                "1",
            )


if __name__ == "__main__":
    unittest.main()
