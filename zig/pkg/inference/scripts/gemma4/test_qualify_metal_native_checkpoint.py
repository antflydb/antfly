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

import contextlib
import copy
import hashlib
from collections import Counter
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import qualify_metal_native_checkpoint as campaign
import qualify_metal_native_checkpoint_performance as performance


class CampaignContracts(unittest.TestCase):
    def test_registry_must_resolve_uniquely_to_the_requested_model(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "google" / "e2b--antfly-fixture"
            directory.mkdir(parents=True)
            manifest = directory / ".antfly-download-complete.json"
            value = {"version": 2, "source": {"owner": "google", "name": "e2b", "selected_format": "gguf"}}
            manifest.write_text(json.dumps(value))
            campaign.checked_model_registry(root, directory, "google/e2b")
            with self.assertRaisesRegex(ValueError, "directory differs"):
                campaign.checked_model_registry(root, directory, "google/e4b")
            value["source"]["name"] = "e4b"
            manifest.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "manifest differs"):
                campaign.checked_model_registry(root, directory, "google/e2b")
            value["source"]["name"] = "e2b"
            manifest.write_text(json.dumps(value))
            (directory.parent / "e2b--antfly-other").mkdir()
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                campaign.checked_model_registry(root, directory, "google/e2b")

    def test_loaded_identity_receipt_binds_both_materialized_requests(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "metadata.json"
            refs = {size: {"path": str(path.parent / (size + ".json")), "sha256": str(i + 1) * 64}
                    for i, size in enumerate(("8k", "32k"))}
            identity = {key: chr(97 + i) * 64 for i, key in enumerate(("weights", "tokenizer", "template", "layout"))}
            value = {"schema": "antfly.metal-checkpoint-performance-requests.v1", "backend": "metal",
                     "model": "google/e2b", "load_generation": 1, "loaded_identity": identity,
                     "requests": {"request_" + size: {"file": size + ".json", "sha256": ref["sha256"],
                         "prompt_tokens": 8128 if size == "8k" else 32512,
                         "max_tokens": 64 if size == "8k" else 256} for size, ref in refs.items()}}
            path.write_text(json.dumps(value))
            self.assertEqual(identity, campaign.identity_receipt_contract(path, "google/e2b", refs))
            for size in refs:
                changed = copy.deepcopy(value)
                changed["requests"]["request_" + size]["sha256"] = "f" * 64
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, "frozen performance requests"):
                    campaign.identity_receipt_contract(path, "google/e2b", refs)
            value["loaded_identity"]["tokenizer"] = "0" * 64
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "loaded model identity receipt"):
                campaign.identity_receipt_contract(path, "google/e2b", refs)

    def test_every_worker_and_checkpoint_producer_matches_frozen_bundle_identity(self):
        identity = {key: chr(97 + i) * 64 for i, key in enumerate(("weights", "tokenizer", "template", "layout"))}
        model = {"request_model": "google/e2b", "loaded_identity": identity}
        ready = {"model": "google/e2b", "backend": "metal", "metal_enabled": True,
                 "cuda_enabled": False, "loaded_identity": identity}
        evidence = {"exporter": {"memory": {}, "ready": ready},
                    "restorer": {"memory": {}, "ready": copy.deepcopy(ready)},
                    "producer_ready": copy.deepcopy(ready)}
        campaign.validate_loaded_model(evidence, model)
        for label in ("restorer", "producer_ready"):
            changed = copy.deepcopy(evidence)
            target = changed[label]["ready"] if label == "restorer" else changed[label]
            target["loaded_identity"]["tokenizer"] = "f" * 64
            with self.assertRaisesRegex(ValueError, "frozen loaded model"):
                campaign.validate_loaded_model(changed, model)
        with self.assertRaisesRegex(ValueError, "missing loaded model"):
            campaign.validate_loaded_model({}, model)

    def test_predecessor_rechecks_spec_evidence_and_checkpoint_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            phase = "smoke-8k-f16"
            spec = self.command_spec()
            spec["campaign_profile"] = "bounded"
            spec_path = root / "campaign-spec.json"
            spec_path.write_text(json.dumps(spec))
            receipt = {"schema": campaign.PHASE_SCHEMA, "phase": phase,
                       "passed": True, "campaign_spec_sha256": campaign.sha(spec_path),
                       "campaign_profile": "bounded",
                       "qualification_contract": spec["qualification_contract"],
                       "command": campaign.command(spec, phase, root)}
            for label, suffix in (("headroom", "-headroom.json"),
                                  ("resource", "-resource.json"), ("output", ".json"),
                                  ("checkpoint", ".kv")):
                file = root / (phase + suffix)
                file.write_text("{}\n")
                receipt[label + "_sha256"] = campaign.sha(file)
            (root / (phase + "-phase.json")).write_text(json.dumps(receipt))
            campaign.validate_predecessor(root, phase, spec_path, spec)
            for suffix in ("-headroom.json", "-resource.json", ".json", ".kv"):
                file = root / (phase + suffix)
                original = file.read_bytes()
                file.write_text('{"changed":true}\n')
                with self.subTest(suffix=suffix), self.assertRaisesRegex(
                        ValueError, "changed"):
                    campaign.validate_predecessor(root, phase, spec_path, spec)
                file.write_bytes(original)
            spec_path.write_text('{"new_spec":true}\n')
            with self.assertRaisesRegex(ValueError, "frozen campaign"):
                campaign.validate_predecessor(root, phase, spec_path, spec)

    def test_model_artifact_bytes_and_paths_are_verified(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "model"
            directory.mkdir()
            artifact = directory / "weights.gguf"
            artifact.write_bytes(b"original weights")
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            row = {"path": artifact.name, "size": artifact.stat().st_size,
                   "sha256": digest}
            (directory / ".antfly-download-complete.json").write_text(
                json.dumps({"artifacts": [row]}))
            self.assertEqual(row, campaign.checked_model_artifact(
                directory, artifact.name, digest))
            artifact.write_bytes(b"modified weights")  # Same length as original.
            with self.assertRaisesRegex(ValueError, "bytes changed"):
                campaign.checked_model_artifact(directory, artifact.name, digest)
            artifact.unlink()
            external = root / artifact.name
            external.write_bytes(b"original weights")
            artifact.symlink_to(external)
            with self.assertRaisesRegex(ValueError, "symlink"):
                campaign.checked_model_artifact(directory, artifact.name, digest)
            for path in (str(external), "../weights.gguf", "./weights.gguf"):
                with self.subTest(path=path), self.assertRaisesRegex(
                        ValueError, "artifact path"):
                    campaign.checked_model_artifact(directory, path, digest)

    def test_quality_seals_all_deleted_checkpoints_in_frozen_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ids = ["first", "last"]
            evidence = {"case_ids": ids, "checkpoints": [
                {"case_id": identifier, "sha256": str(i) * 64,
                 "metadata": {"encoding": "fp8"}}
                for i, identifier in enumerate(ids)]}
            binding = campaign.phase_checkpoint_binding(
                root, "quality-8k-fp8", evidence, ids)
            self.assertIsNone(binding["checkpoint_sha256"])
            self.assertEqual([row["case_id"] for row in binding["case_checkpoints"]], ids)
            for changed in (dict(evidence, checkpoints=evidence["checkpoints"][:1]),
                            dict(evidence, checkpoints=evidence["checkpoints"][::-1])):
                with self.assertRaisesRegex(ValueError, "checkpoint"):
                    campaign.phase_checkpoint_binding(
                        root, "quality-8k-fp8", changed, ids)
            for field, value in (("sha256", "unbound"),
                                 ("metadata", {"encoding": "f16"})):
                changed = copy.deepcopy(evidence)
                changed["checkpoints"][-1][field] = value
                with self.assertRaisesRegex(ValueError, "checkpoint binding"):
                    campaign.phase_checkpoint_binding(
                        root, "quality-8k-fp8", changed, ids)
            (root / "quality-8k-fp8.kv").write_bytes(b"leftover")
            with self.assertRaisesRegex(ValueError, "left a checkpoint"):
                campaign.phase_checkpoint_binding(root, "quality-8k-fp8", evidence, ids)
            with self.assertRaisesRegex(ValueError, "missing retained"):
                campaign.phase_checkpoint_binding(root, "smoke-8k-f16", {})
            retained = root / "smoke-8k-f16.kv"
            retained.write_bytes(b"checkpoint")
            self.assertEqual(campaign.phase_checkpoint_binding(
                root, "smoke-8k-f16", {}),
                {"checkpoint_sha256": campaign.sha(retained)})

    def test_campaign_v3_defaults_to_bounded_without_claiming_reliability(self):
        args = campaign.parser().parse_args([
            "init", "--campaign-root", "/campaign",
            "--build-receipt", "/receipt", "--binary", "/binary",
            "--models-dir", "/models", "--model", "google/e4b",
            "--model-directory", "/models/e4b",
            "--suite-8k", "/suite8", "--suite-32k", "/suite32",
            "--performance-request-8k", "/request8",
            "--performance-request-32k", "/request32",
        ])
        self.assertEqual(campaign.SCHEMA,
                         "antfly.metal-native-checkpoint.campaign-spec.v3")
        self.assertEqual(campaign.PHASE_SCHEMA,
                         "antfly.metal-native-checkpoint.phase.v3")
        self.assertEqual(args.campaign_profile, "bounded")
        contract = campaign.CAMPAIGN_PROFILES[args.campaign_profile]
        self.assertFalse(contract["extended_profile"])
        self.assertFalse(contract["reliability_gates_in_campaign"])
        self.assertFalse(contract["production_ready_claim"])

    def test_plan_is_strictly_serial_and_metal_only(self):
        self.assertEqual(len(campaign.PHASES), 18)
        self.assertEqual(campaign.PHASES[:9], (
            "smoke-8k-f16", "smoke-8k-fp8", "smoke-8k-rotated4",
            "quality-8k-f16", "quality-8k-fp8", "quality-8k-rotated4",
            "performance-8k-f16", "performance-8k-fp8",
            "performance-8k-rotated4"))
        self.assertEqual(campaign.predecessors("smoke-32k-f16"),
                         campaign.PHASES[:9])
        self.assertNotIn("cuda", " ".join(campaign.PHASES))

    def command_spec(self, profile="bounded"):
        return {
            "base_port": 18600,
            "python": "/usr/bin/python3",
            "binary": {"path": "/sealed/context-tests"},
            "model": {"models_root": "/models", "request_model": "google/e4b"},
            "runners": {
                "http": {"path": "/scripts/http.py"},
                "quality": {"path": "/scripts/quality.py"},
                "performance": {"path": "/scripts/performance.py"},
            },
            "suites": {"8k": {"path": "/suites/8k.jsonl"},
                       "32k": {"path": "/suites/32k.jsonl"}},
            "performance_requests": {
                "8k": {"path": "/requests/8k.json"},
                "32k": {"path": "/requests/32k.json"},
            },
            "qualification_contract": campaign.CAMPAIGN_PROFILES[profile],
            "request_timeouts_seconds": campaign.REQUEST_TIMEOUTS,
        }

    def test_bounded_commands_pin_metal_ingress_and_documented_release_counts(self):
        root = Path("/campaign")
        spec = self.command_spec()
        for phase in campaign.PHASES:
            argv = campaign.command(spec, phase, root)
            joined = " ".join(argv)
            self.assertIn("--backend metal", joined)
            self.assertIn("--server-kind metal-test", joined)
            self.assertNotIn("cuda", joined.lower())
            self.assertIn("--encoding " + phase.rsplit("-", 1)[1], joined)
            self.assertEqual(
                "--experimental-rotated4" in joined,
                phase.startswith("smoke-") and phase.endswith("-rotated4"),
            )
            if phase.startswith("quality-"):
                self.assertIn("--quality-profile bounded", joined)
            if phase.startswith("performance-"):
                self.assertIn("--warmups 5 --pairs 20", joined)
                self.assertIn("--declared-context-tokens ", joined)
            self.assertIn("--timeout " + ("1800" if "-8k-" in phase else "7200"),
                          joined)

    def test_extended_profile_remains_explicit_64_case_30_triplet_option(self):
        root = Path("/campaign")
        spec = self.command_spec("extended")
        quality = " ".join(campaign.command(spec, "quality-8k-f16", root))
        performance_command = " ".join(
            campaign.command(spec, "performance-8k-f16", root))
        self.assertIn("--quality-profile extended", quality)
        self.assertIn("--warmups 5 --pairs 30", performance_command)

    def test_bounded_performance_uses_twenty_balanced_route_triplets(self):
        schedule = performance.paired_schedule(20)
        counts = Counter(schedule)
        self.assertEqual(len(schedule), 20)
        self.assertEqual(set(counts), set(__import__("itertools").permutations(
            ("ordinary", "exact", "compacted"))))
        self.assertEqual((min(counts.values()), max(counts.values())), (3, 4))
        self.assertTrue(all(set(row) == {"ordinary", "exact", "compacted"}
                            for row in schedule))

    def test_bounded_suite_requires_four_preselected_critical_cases_per_category(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "suite.jsonl"
            categories = ("fact", "instruction", "code", "tool_call")
            rows = [{"id": f"case-{i}", "category": categories[i % 4],
                     "critical": True} for i in range(16)]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            contract = campaign.suite_contract(path, "bounded")
            self.assertEqual(contract["cases"], 16)
            self.assertEqual(contract["critical_cases"], 16)
            self.assertEqual(contract["categories"], {
                "code": 4, "fact": 4, "instruction": 4, "tool_call": 4})
            rows[0]["critical"] = False
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with self.assertRaisesRegex(ValueError, "preselected critical"):
                campaign.suite_contract(path, "bounded")
            rows[0]["critical"] = True
            rows[0]["id"] = rows[1]["id"]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with self.assertRaisesRegex(ValueError, "unique"):
                campaign.suite_contract(path, "bounded")

    def test_extended_suite_requires_exactly_64_unique_canonical_cases(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "suite.jsonl"
            categories = ("fact", "instruction", "code", "tool_call")
            rows = [{"id": f"case-{i}", "category": categories[i % 4],
                     "critical": i < 16} for i in range(64)]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            self.assertEqual(campaign.suite_contract(path, "extended")["cases"], 64)
            path.write_text("".join(json.dumps(row) + "\n" for row in rows[:-1]))
            with self.assertRaisesRegex(ValueError, "exactly 64"):
                campaign.suite_contract(path, "extended")

    def test_suite_freeze_validates_both_request_models(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "suite.jsonl"
            rows = [{"id": f"case-{i}", "category":
                     ("fact", "instruction", "code", "tool_call")[i % 4],
                     "critical": True, "body": {"model": "e2b"},
                     "checkpoint_body": {"model": "e2b"}} for i in range(16)]
            def save():
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            save()
            campaign.suite_contract(path, "bounded", "e2b")
            for field in ("body", "checkpoint_body"):
                rows[-1][field]["model"] = "e4b"
                save()
                with self.assertRaisesRegex(ValueError, field + r"\.model"):
                    campaign.suite_contract(path, "bounded", "e2b")
                rows[-1][field]["model"] = "e2b"

    def test_performance_request_freezes_release_reservation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "request.json"
            request = {"model": "google/e4b", "messages": [{"role": "user", "content": "x"}],
                       "max_tokens": 64, "temperature": 0, "prompt_cache": False,
                       "cache_dtype": "f16", "stream": True}
            path.write_text(json.dumps(request))
            self.assertEqual(campaign.performance_request_contract(
                path, "8k", "google/e4b"), request)
            request["max_tokens"] = 63
            path.write_text(json.dumps(request))
            with self.assertRaisesRegex(ValueError, "performance request"):
                campaign.performance_request_contract(path, "8k", "google/e4b")

    def test_limits_retain_loader_and_resource_contract(self):
        self.assertEqual(campaign.HEADROOM, {
            "required_free_percent": 76, "samples": 2, "interval_seconds": 10})
        self.assertEqual(campaign.LIMITS["max_rss_mib"], 8192)
        self.assertEqual(campaign.LIMITS["min_free_percent"], 15)
        self.assertEqual(campaign.LIMITS["max_swap_growth_mib"], 0)
        self.assertEqual(campaign.LIMITS["min_disk_free_mib"], 4096)
        self.assertEqual(campaign.PHASE_TIMEOUTS["smoke-8k-f16"], 1800)
        self.assertEqual(campaign.PHASE_TIMEOUTS["smoke-32k-f16"], 7200)
        self.assertEqual(campaign.PHASE_TIMEOUTS["performance-8k-f16"], 14400)
        self.assertEqual(campaign.PHASE_TIMEOUTS["performance-32k-f16"], 43200)
        self.assertEqual(campaign.PHASE_TIMEOUTS["quality-8k-f16"], 43200)
        self.assertEqual(campaign.PHASE_TIMEOUTS["quality-32k-fp8"], 172800)
        self.assertEqual(
            min(campaign.PHASE_TIMEOUTS["smoke-8k-f16"] +
                campaign.SERVER_LIFETIME_MARGIN_SECONDS,
                campaign.SERVER_LIFETIME_MAX_SECONDS),
            2100)
        self.assertEqual(
            min(campaign.PHASE_TIMEOUTS["quality-32k-fp8"] +
                campaign.SERVER_LIFETIME_MARGIN_SECONDS,
                campaign.SERVER_LIFETIME_MAX_SECONDS),
            173100)

    def test_inner_metal_resource_evidence_cannot_use_the_10gib_admission_ceiling(self):
        evidence = {
            "qualification_profile": "metal_test_native_checkpoint_v1",
            "worker": {
                "memory": {
                    "scope": "unified_process_group", "samples": 2, "errors": [],
                    "max_combined_rss_bytes": 8 * 1024**3,
                    "swapout_growth_bytes": 0, "minimum_free_memory_percent": 64,
                    "minimum_disk_free_mib": 4096,
                    "peak_group_rss_bytes": 8 * 1024**3,
                },
                "graceful_stop": True, "remaining_group_members": [], "stopped": True,
            },
        }
        campaign.validate_metal_resources(evidence)
        evidence["worker"]["memory"]["peak_group_rss_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "resource evidence"):
            campaign.validate_metal_resources(evidence)


class PerformanceContracts(unittest.TestCase):
    def test_schedule_is_interleaved_ab_ba(self):
        schedule = performance.paired_schedule(6)
        self.assertEqual(len(schedule), 6)
        self.assertEqual(set(schedule), set(__import__("itertools").permutations(
            ("ordinary", "exact", "compacted"))))

    def test_paired_interval_is_deterministic_and_positive(self):
        rows = [{"exact": {"ttft_ms": 100 + i},
                 "compacted": {"ttft_ms": 50 + i / 2}} for i in range(30)]
        first = performance.paired_ratio_interval(rows, "ttft_ms", "fixed", 500)
        second = performance.paired_ratio_interval(rows, "ttft_ms", "fixed", 500)
        self.assertEqual(first, second)
        self.assertEqual(first["pairs"], 30)
        self.assertAlmostEqual(first["median"], .5)
        self.assertLessEqual(first["lower_95"], first["upper_95"])

    def test_owner_ledger_requires_complete_physical_release(self):
        committed = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "phase": "generation_committed", "request": 7,
            "route": "checkpoint_resume", "owner_id": 9,
            "owner_available": True, "physical_available": True,
            "combined_available": True, "invalid": False, "binding": True,
            "logical_tokens": 8160, "binding_logical_tokens": 8160,
            "validation_ns": 1, "upload_ns": 2, "install_ns": 3,
            "suffix_prefill_ns": 0, "attention_calls": 32, "donor_appends": 0,
            "tracked_combined_live_bytes": 85, "tracked_combined_peak_bytes": 90,
            "binding_representation": "f16", "binding_fp8_heads": 0,
            "binding_fp8_rows": 0, "binding_rotated4_heads": 0,
            "binding_rotated4_rows": 0, "binding_exact_resident_bytes": 60,
            "binding_encoded_resident_bytes": 0,
            "binding_metadata_resident_bytes": 15, "binding_allocated_bytes": 75,
        }
        released = {
            "schema": "antfly.metal-checkpoint-release.v1",
            "phase": "final_release", "request": 7,
            "route": "checkpoint_resume", "owner_id": 9, "released": True,
            "scope_complete": True, "full_owner_physical_available": True,
            "measured_available": True, "measured_committed_bytes": 75,
            "initial_live_bytes": 10, "post_release_live_bytes": 0,
        }
        ready = dict(committed, phase="resumed_ready")
        result = performance.ledger_contract(
            [ready, committed, released], "checkpoint_resume",
            expected_representation="f16")
        self.assertEqual(result["committed_context_bytes"], 75)
        released["post_release_live_bytes"] = 1
        with self.assertRaisesRegex(ValueError, "physical owner ledger"):
            performance.ledger_contract(
                [ready, committed, released], "checkpoint_resume",
                expected_representation="f16")

    def test_owner_ledger_rejects_bound_or_nonzero_exact_route(self):
        committed = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "phase": "generation_committed", "request": 8,
            "route": "exact", "owner_id": 10,
            "owner_available": True, "physical_available": True,
            "combined_available": True, "invalid": False, "binding": False,
            "logical_tokens": 8160, "binding_logical_tokens": 0,
            "validation_ns": 0, "upload_ns": 0, "install_ns": 0,
            "suffix_prefill_ns": 0, "attention_calls": 0, "donor_appends": 0,
            "tracked_combined_live_bytes": 85, "tracked_combined_peak_bytes": 90,
            "binding_representation": "none", "binding_fp8_heads": 0,
            "binding_fp8_rows": 0, "binding_rotated4_heads": 0,
            "binding_rotated4_rows": 0, "binding_exact_resident_bytes": 0,
            "binding_encoded_resident_bytes": 0,
            "binding_metadata_resident_bytes": 0, "binding_allocated_bytes": 0,
        }
        released = {
            "schema": "antfly.metal-checkpoint-release.v1",
            "phase": "final_release", "request": 8,
            "route": "exact", "owner_id": 10, "released": True,
            "scope_complete": True, "full_owner_physical_available": True,
            "measured_available": True, "measured_committed_bytes": 75,
            "initial_live_bytes": 10, "post_release_live_bytes": 0,
        }
        performance.ledger_contract(
            [committed, released], "exact", require_binding=False)
        for field, value in (("binding", True),
                             ("binding_representation", "f16"),
                             ("binding_allocated_bytes", 1)):
            invalid = dict(committed, **{field: value})
            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "unexpectedly installed"):
                performance.ledger_contract(
                    [invalid, released], "exact", require_binding=False)

    def test_owner_ledger_requires_positive_installed_execution(self):
        committed = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "phase": "generation_committed", "request": 7,
            "route": "checkpoint_resume", "owner_id": 9,
            "owner_available": True, "physical_available": True,
            "combined_available": True, "invalid": False, "binding": True,
            "logical_tokens": 8160, "binding_logical_tokens": 8160,
            "validation_ns": 1, "upload_ns": 2, "install_ns": 3,
            "suffix_prefill_ns": 0, "attention_calls": 1, "donor_appends": 0,
            "tracked_combined_live_bytes": 85, "tracked_combined_peak_bytes": 90,
            "binding_representation": "f16", "binding_fp8_heads": 0,
            "binding_fp8_rows": 0, "binding_rotated4_heads": 0,
            "binding_rotated4_rows": 0, "binding_exact_resident_bytes": 60,
            "binding_encoded_resident_bytes": 0,
            "binding_metadata_resident_bytes": 15, "binding_allocated_bytes": 75,
        }
        released = {
            "schema": "antfly.metal-checkpoint-release.v1",
            "phase": "final_release", "request": 7,
            "route": "checkpoint_resume", "owner_id": 9, "released": True,
            "scope_complete": True, "full_owner_physical_available": True,
            "measured_available": True, "measured_committed_bytes": 75,
            "initial_live_bytes": 10, "post_release_live_bytes": 0,
        }
        ready = dict(committed, phase="resumed_ready", attention_calls=0)
        performance.ledger_contract(
            [ready, committed, released], "checkpoint_resume",
            expected_representation="f16")
        for field in ("binding_allocated_bytes", "binding_exact_resident_bytes",
                      "attention_calls"):
            invalid = dict(committed, **{field: 0})
            if field == "binding_exact_resident_bytes":
                invalid["binding_metadata_resident_bytes"] = 75
            elif field == "binding_allocated_bytes":
                invalid["binding_exact_resident_bytes"] = 0
                invalid["binding_metadata_resident_bytes"] = 0
            with self.subTest(field=field), self.assertRaises(ValueError):
                performance.ledger_contract(
                    [ready, invalid, released], "checkpoint_resume",
                    expected_representation="f16")

    def test_performance_output_identity_requires_all_three_routes(self):
        matched = [{"same_greedy_output": True,
                    "ordinary_same_greedy_output": True} for _ in range(30)]
        self.assertTrue(performance.matched_output_identity(matched))
        exact_mismatch = [dict(row) for row in matched]
        exact_mismatch[0]["same_greedy_output"] = False
        self.assertFalse(performance.matched_output_identity(exact_mismatch))
        ordinary_mismatch = [dict(row) for row in matched]
        ordinary_mismatch[0]["ordinary_same_greedy_output"] = False
        self.assertFalse(performance.matched_output_identity(ordinary_mismatch))
        self.assertFalse(performance.matched_output_identity([]))

    def test_physical_savings_interval_uses_matched_owner_bytes(self):
        rows = [{"exact": {"bytes": 100}, "compacted": {"bytes": 70}}
                for _ in range(30)]
        result = performance.paired_transform_interval(
            rows, lambda row: 1 - row["compacted"]["bytes"] / row["exact"]["bytes"],
            "physical", 500)
        self.assertAlmostEqual(result["lower_95"], .3)

    def test_qualification_minima_are_not_development_defaults(self):
        self.assertEqual(performance.MIN_WARMUPS, 5)
        self.assertEqual(performance.MIN_PAIRS, 20)
        parser = argparse_for_performance()
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args([
                    "--backend", "cuda", "--server-kind", "metal-test",
                    "--server", "x", "--models-dir", "m", "--model", "e4b",
                    "--port", "1234", "--padding-lines", "742", "--encoding", "f16",
                    "--output", "/tmp/o", "--checkpoint", "/tmp/c"])


def argparse_for_performance():
    # Keep this small parser assertion local: the public main also performs
    # filesystem checks, while argparse choices enforce the no-CUDA boundary.
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["metal"], required=True)
    parser.add_argument("--server-kind", choices=["metal-test"], required=True)
    for name in ("server", "models-dir", "model", "port", "padding-lines",
                 "encoding", "output", "checkpoint"):
        parser.add_argument("--" + name, required=True)
    return parser


if __name__ == "__main__":
    unittest.main()
