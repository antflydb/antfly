from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import oracle
import download_model
import generate_pipeline_cases
import fixture_support


class ProvenanceTest(unittest.TestCase):
    def test_all_nine_configuration_fixtures_match_immutable_hashes(self):
        report = oracle.verify_config_fixtures()
        self.assertEqual(9, len(report["files"]))
        self.assertFalse(report["real_model_qualified"])

    def test_checked_in_references_match_pinned_generators_and_content(self):
        report = oracle.verify_reference_fixtures()
        self.assertGreater(len(report["files"]), 20)
        self.assertFalse(report["native_runtime_qualified"])

    def test_external_references_verify_present_bytes_and_report_missing_coverage(self):
        report = fixture_support.verify_external_fixtures()
        self.assertEqual(set(fixture_support.external_inventory()), set(report["files"]) | set(report["missing"]))
        self.assertEqual(not report["missing"], report["complete"])

    def test_external_policy_skips_only_declared_absence_and_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(oracle, "FIXTURES", Path(tmp)):
            pin = {"size_bytes": 8, "sha256": hashlib.sha256(b"original").hexdigest()}
            manifest = {"format_version": 1, "upstream_commit": oracle.UPSTREAM_COMMIT,
                        "files": {}, "external_files": {"payload": pin}}
            oracle.write_json(Path(tmp) / "reference_manifest.json", manifest)
            with self.assertRaises(unittest.SkipTest):
                fixture_support.require_fixtures("payload")
            with self.assertRaises(oracle.ContractError):
                fixture_support.require_fixtures("unknown")
            path = Path(tmp) / "payload"
            path.write_bytes(b"original")
            fixture_support.require_fixtures("payload")
            self.assertTrue(fixture_support.verify_external_fixtures()["complete"])
            path.write_bytes(b"replaced")
            with self.assertRaisesRegex(oracle.ContractError, "SHA256 mismatch"):
                fixture_support.require_fixtures("payload")
            with self.assertRaises(oracle.ContractError):
                fixture_support.verify_external_fixtures()
            path.unlink()
            path.symlink_to(Path(tmp) / "absent")
            with self.assertRaises(oracle.ContractError):
                fixture_support.require_fixtures("payload")

    def test_same_size_substitution_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "weights"
            path.write_bytes(b"original")
            expected = {"size_bytes": 8, "sha256": hashlib.sha256(b"original").hexdigest()}
            oracle.verify_file(path, expected)
            path.write_bytes(b"replaced")
            with self.assertRaisesRegex(oracle.ContractError, "SHA256 mismatch"):
                oracle.verify_file(path, expected)

    def test_missing_file_never_becomes_a_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(oracle.ContractError, "missing"):
                oracle.verify_file(Path(tmp) / "absent", {"size_bytes": 1, "sha256": "0" * 64})

    def test_snapshot_symlink_is_verified_by_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            blob = Path(tmp) / "blob"
            blob.write_bytes(b"fixed")
            link = Path(tmp) / "snapshot"
            link.symlink_to(blob)
            oracle.verify_file(link, {"size_bytes": 5, "sha256": hashlib.sha256(b"fixed").hexdigest()})

    def test_duplicate_and_nonfinite_json_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.json"
            for value, reason in [("{\"id\":1,\"id\":2}", "duplicate"), ("{\"score\":NaN}", "non-finite")]:
                path.write_text(value)
                with self.subTest(value=value), self.assertRaisesRegex(oracle.ContractError, reason):
                    oracle.read_json(path)

    def test_dependency_mismatch_is_rejected_before_import(self):
        expected = oracle.load_manifest()["runtime"]
        versions = dict(expected["packages"])
        versions["peft"] = "wrong"
        with mock.patch.object(oracle.platform, "python_version", return_value=expected["python"]), \
             mock.patch.object(oracle.importlib.metadata, "version", side_effect=versions.__getitem__), \
             self.assertRaisesRegex(oracle.ContractError, "peft=wrong"):
            oracle.verify_dependencies()

    def test_import_from_another_checkout_is_rejected(self):
        module = types.SimpleNamespace(__file__="/private/tmp/unpinned/gliner2/__init__.py")
        with self.assertRaisesRegex(oracle.ContractError, "unpinned source"):
            oracle.verify_import_source(module, Path("/private/tmp/pinned"))

    def test_namespace_packages_must_have_only_pinned_search_locations(self):
        module = types.SimpleNamespace(__file__=None, __path__=["/private/tmp/pinned/gliner2/utils"])
        oracle.verify_import_source(module, Path("/private/tmp/pinned"))
        module.__path__.append("/private/tmp/unpinned/utils")
        with self.assertRaisesRegex(oracle.ContractError, "verifiable source"):
            oracle.verify_import_source(module, Path("/private/tmp/pinned"))

    def test_model_bundle_rejects_tokenizer_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            config = directory / "config.json"
            config.write_text(json.dumps({"architecture": "boundary", "architecture_version": 1, "token_pooling": "first"}))
            manifest = {"models": {"small": {"model_id": "pinned", "revision": "fixed", "files": {
                "config.json": {"size_bytes": config.stat().st_size, "sha256": oracle.sha256_file(config)}
            }}}}
            with mock.patch.object(oracle, "load_manifest", return_value=manifest):
                oracle.verify_model_dir("small", directory)
                (directory / "added_tokens.json").write_text("{}")
                with self.assertRaisesRegex(oracle.ContractError, "unmanifested"):
                    oracle.verify_model_dir("small", directory)

    def test_failed_capture_leaves_no_completed_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "result"
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                with oracle.atomic_output_directory(destination) as staging:
                    (staging / "partial").write_text("incomplete")
                    raise RuntimeError("interrupted")
            self.assertFalse(destination.exists())
            self.assertEqual([], list(Path(tmp).iterdir()))

    def test_capture_never_overwrites_existing_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "result"
            destination.mkdir()
            (destination / "receipt").write_text("existing")
            with self.assertRaisesRegex(oracle.ContractError, "overwrite"):
                with oracle.atomic_output_directory(destination):
                    self.fail("existing output was admitted")
            self.assertEqual("existing", (destination / "receipt").read_text())

    def test_failed_download_does_not_publish_a_model_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "model"
            with mock.patch.object(download_model.subprocess, "run", side_effect=subprocess.CalledProcessError(22, "curl")), \
                 self.assertRaises(subprocess.CalledProcessError):
                download_model.download("small", destination)
            self.assertFalse(destination.exists())
            self.assertEqual([], list(Path(tmp).iterdir()))


class CheckoutTest(unittest.TestCase):
    def test_wrong_dirty_and_ignored_source_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "gliner2").mkdir()
            source = root / "gliner2" / "__init__.py"
            source.write_text('__version__ = "2.0.0"\n')
            (root / ".gitignore").write_text("ignored.py\n")

            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                               stderr=subprocess.DEVNULL).strip()

            git("init", "-q")
            git("add", ".")
            git("-c", "user.name=Oracle Test", "-c", "user.email=oracle@example.invalid", "commit", "-qm", "fixture")
            commit = git("rev-parse", "HEAD")
            self.assertEqual(commit, oracle.verify_upstream_checkout(root, commit)["commit"])
            with self.assertRaisesRegex(oracle.ContractError, "pinned"):
                oracle.verify_upstream_checkout(root, "0" * 40)
            source.write_text("changed\n")
            with self.assertRaisesRegex(oracle.ContractError, "clean"):
                oracle.verify_upstream_checkout(root, commit)
            git("checkout", "--", "gliner2/__init__.py")
            (root / "ignored.py").write_text("shadow = True\n")
            self.assertEqual("", git("status", "--porcelain"))
            with self.assertRaisesRegex(oracle.ContractError, "ignored"):
                oracle.verify_upstream_checkout(root, commit)


class PipelineAdaptationTest(unittest.TestCase):
    def test_each_published_variant_retains_its_own_bundle_and_edge_identity(self):
        for variant in ("small", "base", "multi"):
            with self.subTest(model=variant):
                result = generate_pipeline_cases.generate(variant, oracle.FIXTURES / f"{variant}_reference", oracle.FIXTURES / "requests.json")
                pinned = oracle.load_manifest()["models"][variant]
                self.assertEqual(pinned["model_id"], result["model_id"])
                self.assertEqual(pinned["revision"], result["revision"])
                self.assertEqual(pinned["files"]["tokenizer.json"]["sha256"], result["tokenizer_sha256"])
                self.assertEqual(pinned["files"]["model.safetensors"]["sha256"], result["model_sha256"])
                self.assertEqual(10, len(result["cases"]))
                fixture_name = "pipeline_cases.json" if variant == "small" else f"pipeline_cases_{variant}.json"
                checked_in = oracle.read_json(oracle.FIXTURES / fixture_name)
                self.assertEqual(result, checked_in)
                # Prompt construction preserves structure-map insertion order;
                # dictionary equality alone would miss a sorted-key writer.
                for generated, stored in zip(result["cases"], checked_in["cases"]):
                    self.assertEqual(json.dumps(generated["schema"], ensure_ascii=False), json.dumps(stored["schema"], ensure_ascii=False))
                joint = next(case for case in result["cases"] if case["id"] == "joint_ie")
                if variant != "small":
                    self.assertEqual(2, len(joint["expected"]["relations"]))
                    for edge in joint["expected"]["relations"]:
                        self.assertEqual(0, edge["head_entity_type"])
                        self.assertEqual(1, edge["tail_entity_type"])
                        self.assertGreater(edge["confidence"], 0.5)

    def test_variant_substitution_is_rejected_before_adapting_outputs(self):
        with self.assertRaisesRegex(oracle.ContractError, "different model variant"):
            generate_pipeline_cases.generate("multi", oracle.FIXTURES / "base_reference", oracle.FIXTURES / "requests.json")

    def test_modified_capture_tensor_is_rejected_by_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            original = oracle.FIXTURES / "small_reference"
            (directory / "capture.json").write_bytes((original / "capture.json").read_bytes())
            report = oracle.read_json(directory / "capture.json")
            filename = report["requests"][0]["tensor_capture"]["file"]
            payload = bytearray((original / filename).read_bytes())
            payload[-1] ^= 1
            (directory / filename).write_bytes(payload)
            with self.assertRaisesRegex(oracle.ContractError, "SHA256 mismatch"):
                generate_pipeline_cases.generate("small", directory, oracle.FIXTURES / "requests.json")

    def test_output_adaptation_uses_explicit_retention_without_rewriting_capture(self):
        for variant in ("base", "multi"):
            with self.subTest(model=variant), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                original = oracle.FIXTURES / f"{variant}_reference"
                raw = (original / "capture.json").read_bytes()
                (directory / "capture.json").write_bytes(raw)
                retained = {name: pin for name, pin in generate_pipeline_cases.reference_inventory().items()
                            if not (name.startswith(f"{variant}_reference/") and name.endswith(".safetensors"))}
                with mock.patch.object(generate_pipeline_cases, "reference_inventory", return_value=retained):
                    generated = generate_pipeline_cases.generate(variant, directory, oracle.FIXTURES / "requests.json")
                expected = oracle.read_json(oracle.FIXTURES / f"pipeline_cases_{variant}.json")
                self.assertEqual(json.dumps(expected, ensure_ascii=False), json.dumps(generated, ensure_ascii=False))
                self.assertEqual(raw, (directory / "capture.json").read_bytes())

    def test_missing_retained_attachment_still_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            original = oracle.FIXTURES / "small_reference"
            (directory / "capture.json").write_bytes((original / "capture.json").read_bytes())
            with self.assertRaisesRegex(oracle.ContractError, "required regular file is missing"):
                generate_pipeline_cases.generate("small", directory, oracle.FIXTURES / "requests.json")

    def test_optional_diagnostics_do_not_allow_modified_public_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            raw = (oracle.FIXTURES / "base_reference/capture.json").read_bytes()
            changed = raw.replace(b'"John"', b'"Joan"', 1)
            self.assertNotEqual(raw, changed)
            (directory / "capture.json").write_bytes(changed)
            with self.assertRaisesRegex(oracle.ContractError, "SHA256 mismatch"):
                generate_pipeline_cases.generate("base", directory, oracle.FIXTURES / "requests.json")

    def test_retention_cannot_omit_capture_or_substitute_retained_tensor_identity(self):
        retained = generate_pipeline_cases.reference_inventory()
        for name, value in (("small_reference/capture.json", None),
                            ("small_reference/mixed_tasks.safetensors", {"size_bytes": 1, "sha256": "0" * 64})):
            changed = dict(retained)
            if value is None:
                del changed[name]
            else:
                changed[name] = value
            with self.subTest(path=name), \
                 mock.patch.object(generate_pipeline_cases, "reference_inventory", return_value=changed), \
                 self.assertRaises(oracle.ContractError):
                generate_pipeline_cases.generate("small", oracle.FIXTURES / "small_reference", oracle.FIXTURES / "requests.json")

    def test_generator_request_or_profile_drift_never_becomes_reference(self):
        original_read = oracle.read_json
        original = original_read(oracle.FIXTURES / "small_reference/capture.json")
        for mutate in (lambda report: report.update(generator_sha256="0" * 64),
                       lambda report: report.update(requests_sha256="0" * 64),
                       lambda report: report["provenance"].update(dtype="float16"),
                       lambda report: report.update(native_runtime_qualified=True)):
            report = json.loads(json.dumps(original))
            mutate(report)
            with mock.patch.object(oracle, "read_json", side_effect=lambda path: report if Path(path).name == "capture.json" else original_read(path)), \
                 self.assertRaises(oracle.ContractError):
                generate_pipeline_cases.generate("small", oracle.FIXTURES / "small_reference", oracle.FIXTURES / "requests.json")


class LegacyParityDiscoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = oracle.HERE.parents[4]
        path = cls.repo / "zig" / "e2e" / "inference" / "test_gliner2_lora_parity.py"
        spec = importlib.util.spec_from_file_location("legacy_gliner2_parity_discovery", path)
        cls.module = importlib.util.module_from_spec(spec)
        with mock.patch.dict(os.environ, {"TERMITE_GLINER2_REQUIRE_PARITY": "0"}):
            spec.loader.exec_module(cls.module)

    def test_current_helper_and_repo_are_found(self):
        self.assertEqual(self.repo, self.module.REPO_ROOT)
        self.assertTrue(self.module.COMPARE_SCRIPT.is_file())
        self.assertEqual("gliner2", self.module.COMPARE_SCRIPT.parent.name)

    def test_missing_helper_is_an_error_not_arbitrary_root_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            start = Path(tmp) / "zig/e2e/inference/test.py"
            with self.assertRaisesRegex(RuntimeError, "cannot locate"):
                self.module._find_repo_root(start)


if __name__ == "__main__":
    unittest.main()
