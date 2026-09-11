from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import venv
from unittest import mock
from types import SimpleNamespace

import capture_massive_execution_registry as capture
import evaluate
import evaluate_massive11 as runner
import evaluation_contract as facts
import massive_eval_contract as contract
import oracle
import prepare_massive11 as massive


def synthetic_fixture(profile_name="entities_en-US", index=0):
    """Synthetic texts exercise hashing only; no synthetic gold reaches a worker."""
    registry = copy.deepcopy(contract.load_registry())
    entry = contract.profile_entry(profile_name, registry)
    profile = entry["profile"]
    schema, _ = massive.schema_and_metrics(profile, oracle.read_json(massive.MANIFEST))
    options = {"best_effort": False, "overlap": "flat", "threshold": 0.5, "word_splitter": profile["word_splitter"]}
    start, end = contract.RANGES[index]
    cases = []
    for row in range(start, end):
        source_id = f'massive/1.1/{profile["locale"]}/test/{row}'
        text = f'İ😀\t明天  café "{row}"\n'
        request = {"text": text, "schema": schema, "options": options, "offset_unit": "utf8_bytes"}
        cases.append({"id": facts.digest(facts.encoded({"lock": entry["lock_sha256"], "id": source_id})),
            "source_id": source_id, "request_sha256": facts.digest(facts.encoded(request)), "text": text})
    shard = entry["shards"][index]
    shard["request_ids_sha256"] = facts.digest(facts.encoded([row["id"] for row in cases]))
    shard["request_sha256s_sha256"] = facts.digest(facts.encoded([row["request_sha256"] for row in cases]))
    fixture = {"format_version": 2, "scope": contract.WORKER_SCOPE, "qualification": False,
        "source_commit": oracle.UPSTREAM_COMMIT, "model": "small", "source_files": registry["models"]["small"]["source_files"],
        "registry_sha256": oracle.sha256_file(contract.REGISTRY), "profile": profile_name,
        **{key: entry[key] for key in ("lock_sha256", "prepared_sha256", "requests_sha256")},
        "adapter_sha256": registry["adapter_sha256"], "harness_sha256": registry["frozen_helpers"]["evaluation_contract.py"],
        "policy": contract.POLICY.copy(), "limits": contract.LIMITS.copy(), "schema": schema, "options": options, "offset_unit": "utf8_bytes",
        "transport": {"global_records": contract.RECORDS, "transport_sha256": entry["transport_sha256"], **shard}, "cases": cases}
    return registry, fixture


class EnvelopeTest(unittest.TestCase):
    def test_real_locked_first_request_hashes_keep_preparation_field_order(self):
        # Blinded source test-row zero from four audited profiles; no gold.
        vectors = (
            ("entities_ar-SA", "صحيني خمسة الفجر هذا الأسبوع", "a92a4811ee2b0eba6a958880614e14ed5a8fada1f462758651b46722e279381c", "c55a2f60bbf2f0ed44935ba151a0cabed2e9fda375efd4941ad5e9e71c89e650"),
            ("entities_zh-CN_char", "这周五点叫我起床", "5767e37345a928d05a02e998bf64ad90e1376d632ca50dd5ee59a4b231ece34a", "7a6998522d1a0909f9de8f88c8a94b81edc14e5248c4f5b3da05f6ec184e5793"),
            ("intent_en-US", "wake me up at five am this week", "d228c0dda1eb1c72e8d6bad23558e6ed36b2f328522b721a0062c13d9726eab3", "72b979ae5253861c7521b874142b825f8d7c45d61b8511d81714282a038404d7"),
            ("intent_scenario_en-US", "wake me up at five am this week", "4fcd65c921ea54db0610648336c8888795dda8e58979608ca9487f7dabab84b6", "8c37bfab8b28d352f26e3de121508f1f6fe3466a66a48d4ef17cc0b4c6fda748"),
        )
        for name, text, expected_id, expected_request in vectors:
            entry = contract.profile_entry(name)
            schema, _ = massive.schema_and_metrics(entry["profile"], oracle.read_json(massive.MANIFEST))
            fixture = {"schema": schema, "options": {"threshold": .5, "overlap": "flat", "best_effort": False,
                "word_splitter": entry["profile"]["word_splitter"]}, "offset_unit": "utf8_bytes"}
            source_id = f'massive/1.1/{entry["profile"]["locale"]}/test/0'
            self.assertEqual(expected_id, facts.digest(facts.encoded({"lock": entry["lock_sha256"], "id": source_id})))
            self.assertEqual(expected_request, facts.digest(facts.encoded(contract.request_for(fixture, {"text": text}))))

    def test_all_ten_fixed_schemas_splitters_and_full_three_shard_ranges(self):
        registry = contract.load_registry()
        self.assertEqual(10, len(registry["profiles"]))
        for row in registry["profiles"]:
            for index in range(3):
                admitted, fixture = synthetic_fixture(row["profile"]["id"], index)
                contract.validate_fixture(fixture, admitted)
                self.assertLess(len(facts.encoded(fixture)), contract.MAX_FIXTURE_BYTES)
                self.assertEqual((1024, 1024, 926)[index], len(fixture["cases"]))
        self.assertEqual({"char", "whitespace"}, {row["profile"]["word_splitter"] for row in registry["profiles"]})

    def test_mutation_rehash_does_not_authorize_new_text_schema_order_or_source_identity(self):
        registry, original = synthetic_fixture()
        changes = [lambda x: x["cases"].reverse(), lambda x: x["cases"].pop(),
                   lambda x: x["cases"][0].update(gold={"label": "person"}),
                   lambda x: x["cases"][0].update(text="replacement"),
                   lambda x: x["cases"][0].update(source_id="massive/1.1/en-US/train/0"),
                   lambda x: x["schema"]["entities"].reverse(),
                   lambda x: x["options"].update(word_splitter="char"),
                   lambda x: x["policy"].update(max_words=4096),
                   lambda x: x["limits"].update(beam_width=64),
                   lambda x: x["transport"].update(start=1),
                   lambda x: x["source_files"][0].update(sha256="0" * 64)]
        for change in changes:
            fixture = copy.deepcopy(original); change(fixture)
            with self.subTest(change=change), self.assertRaises(ValueError):
                contract.validate_fixture(fixture, registry)
        fixture = copy.deepcopy(original)
        fixture["cases"][0]["text"] = "replacement"
        fixture["cases"][0]["request_sha256"] = facts.digest(facts.encoded(contract.request_for(fixture, fixture["cases"][0])))
        fixture["transport"]["request_sha256s_sha256"] = facts.digest(facts.encoded([row["request_sha256"] for row in fixture["cases"]]))
        with self.assertRaisesRegex(ValueError, "shard identity"):
            contract.validate_fixture(fixture, registry)

    def test_embedded_worker_pins_match_audited_registry(self):
        source = (contract.HERE.parents[1] / "src/bench/gliner25_evaluation_worker.zig").read_text()
        actual = source.split("// BEGIN MASSIVE EXECUTION REGISTRY PINS\n")[1].split("// END MASSIVE EXECUTION REGISTRY PINS")[0]
        self.assertEqual(capture.zig_pins(contract.load_registry(), oracle.sha256_file(contract.REGISTRY)), actual)

    def test_frozen_crossner_identity_and_v1_fixture_unchanged(self):
        registry = contract.load_registry()
        self.assertEqual(6, len(registry["frozen_helpers"]))
        fixture = {"format_version": 1, "scope": evaluate.WORKER_SCOPE, "qualification": False,
            "source_commit": oracle.UPSTREAM_COMMIT, "model": "small", "source_files": evaluate.source_identity("small")["source_files"],
            **{key: "1" * 64 for key in ("lock_sha256", "prepared_sha256", "requests_sha256")},
            "adapter_sha256": registry["frozen_helpers"]["prepare_crossner_ai.py"],
            "harness_sha256": registry["frozen_helpers"]["evaluation_contract.py"], "policy": evaluate.POLICY, "cases": []}
        request = {"text": "Alice", "schema": {"entities": oracle.read_json(evaluate.adapter.MANIFEST)["entity_types"]},
                   "options": evaluate.REQUEST_OPTIONS.copy(), "offset_unit": "utf8_bytes"}
        fixture["cases"] = [{"id": "0" * 64, "request_sha256": facts.digest(facts.encoded(request)), **request}]
        before = facts.encoded(fixture)
        evaluate.validate_fixture(fixture)
        self.assertEqual(before, facts.encoded(fixture))
        fixture["cases"][0]["options"]["word_splitter"] = "char"
        with self.assertRaises(ValueError): evaluate.validate_fixture(fixture)


class OutputTest(unittest.TestCase):
    def fixture(self, profile_name):
        profile = contract.profile_entry(profile_name)["profile"]
        schema, _ = massive.schema_and_metrics(profile, oracle.read_json(massive.MANIFEST))
        return {"profile": profile_name, "schema": schema, "options": {"threshold": .5, "overlap": "flat", "best_effort": False,
            "word_splitter": profile["word_splitter"]}, "offset_unit": "utf8_bytes"}, profile

    def test_python_routes_ordinary_and_constrained_schemas_without_ignored_fields(self):
        class Builder:
            def __init__(self): self.calls = []
            def classification(self, **kwargs): self.calls.append(("classification", kwargs))
            def single(self, *args, **kwargs): self.calls.append(("single", args, kwargs))
            def constrain(self, value): self.calls.append(("constrain", value))
        modules = {"gliner2": SimpleNamespace(Schema=Builder),
                   "gliner2.classification": SimpleNamespace(ClassificationSchema=Builder),
                   "gliner2.classification.constraints": SimpleNamespace(constraint_from_dict=copy.deepcopy)}
        with mock.patch.dict("sys.modules", modules):
            ordinary, _ = self.fixture("intent_en-US")
            built = runner.python_schema(ordinary["schema"])
            self.assertEqual([("classification", {"task": "intent", "labels": ordinary["schema"]["classifications"][0]["labels"],
                "multi_label": False, "cls_threshold": .5, "class_act": "softmax"})], built.calls)
            constrained, _ = self.fixture("intent_scenario_en-US")
            built = runner.python_schema(constrained["schema"])
            self.assertEqual(2, sum(row[0] == "single" for row in built.calls))
            self.assertEqual(constrained["schema"]["classification_constraints"], [row[1] for row in built.calls if row[0] == "constrain"])

    def test_entity_unicode_source_offsets_and_confidence_parity(self):
        fixture, profile = self.fixture("entities_zh-CN_char")
        request = {"text": "😀明天", **{key: fixture[key] for key in ("schema", "options", "offset_unit")}}
        source = {"entities": {name: [] for name in fixture["schema"]["entities"]}}
        source["entities"]["date"] = [{"text": "明天", "start": 1, "end": 3, "confidence": .9}]
        native = {"entities": [{"name": name, "values": []} for name in fixture["schema"]["entities"]]}
        next(group for group in native["entities"] if group["name"] == "date")["values"] = [
            {"text": "明天", "source": {"start": 4, "end": 10, "unit": "utf8_bytes"}, "confidence": .9}]
        left = contract.canonical_output(request, source, "python", profile)
        right = contract.canonical_output(request, native, "native", profile)
        self.assertEqual(left, right)
        native["entities"].pop()
        with self.assertRaises(ValueError): contract.canonical_output(request, native, "native", profile)

    def test_constrained_labels_and_real_solver_status_are_required(self):
        fixture, profile = self.fixture("intent_scenario_en-US")
        request = {"text": "Set an alarm", **{key: fixture[key] for key in ("schema", "options", "offset_unit")}}
        source = {"intent": {"value": "alarm_set", "confidence": .8}, "scenario": {"value": "alarm", "confidence": .9},
                  "_meta": {"feasible": True, "violations": [], "exact": True}}
        native = {"classifications": [{"name": name, "multi_label": False, "labels": [{"label": row["value"], "confidence": row["confidence"]}]}
                    for name, row in source.items() if name != "_meta"],
                  "classification_solver": {"status": "optimal", "exhausted": False}}
        self.assertEqual(contract.canonical_output(request, source, "python", profile), contract.canonical_output(request, native, "native", profile))
        native["classification_solver"]["exhausted"] = True
        with self.assertRaisesRegex(ValueError, "exhausted"): contract.canonical_output(request, native, "native", profile)
        source["scenario"]["value"] = "music"
        with self.assertRaisesRegex(ValueError, "invalid witness"): contract.canonical_output(request, source, "python", profile)

    def test_capacity_error_is_a_result_with_all_fixed_metrics_and_cannot_pass_parity(self):
        fixture, _ = self.fixture("intent_en-US")
        case = {"id": "a" * 64, "request_sha256": "b" * 64, "text": "test"}
        event = {"event": "error", "case_id": case["id"], "request_sha256": case["request_sha256"],
                 "error_code": "BoundarySequenceLimitExceeded", "input_ids": None}
        prediction, canonical = contract.response_prediction(fixture, case, event, "native")
        self.assertIsNone(canonical)
        self.assertEqual(61, len(prediction["metrics"]))
        self.assertTrue(all(value == [] for value in prediction["metrics"].values()))
        self.assertFalse(contract.compare_response(fixture, case, event, "metal", event, "native")["parity_pass"])
        event["input_ids"] = [1]
        with self.assertRaisesRegex(ValueError, "malformed"): contract.response_prediction(fixture, case, event, "native")

    def test_exact_token_and_unchanged_confidence_tolerance(self):
        fixture, _ = self.fixture("intent_en-US")
        case = {"id": "a" * 64, "request_sha256": "b" * 64, "text": "test"}
        event = {"event": "result", "case_id": case["id"], "request_sha256": case["request_sha256"], "input_ids": [2, 3],
            "output": {"classifications": [{"name": "intent", "multi_label": False, "labels": [{"label": "alarm_set", "confidence": .9}]}]}}
        other = copy.deepcopy(event)
        self.assertTrue(contract.compare_response(fixture, case, event, "metal", other, "native")["parity_pass"])
        other["input_ids"] = [2, 4]
        self.assertFalse(contract.compare_response(fixture, case, event, "metal", other, "native")["parity_pass"])
        other = copy.deepcopy(event); other["output"]["classifications"][0]["labels"][0]["confidence"] -= .00051
        self.assertFalse(contract.compare_response(fixture, case, event, "metal", other, "native")["parity_pass"])


class AggregateTest(unittest.TestCase):
    def reports(self):
        entry = contract.profile_entry("intent_en-US")
        identity = {"backend": "python", "model": "small", "artifact": {}, "binary_sha256": "a" * 64,
            "profile": "intent_en-US", "registry_sha256": oracle.sha256_file(contract.REGISTRY), "lock_sha256": entry["lock_sha256"],
            "prepared_sha256": entry["prepared_sha256"], "contract_files": contract.contract_files(), "policy": contract.POLICY,
            "limits": contract.LIMITS, "resource_policy": {"threads": 1}, "source_reference_report_sha256": None,
            "native_reference_report_sha256": None, "python_runtime": runner.python_runtime_identity()}
        reports = [{"scope": contract.SHARD_SCOPE, "status": "complete", "qualification": False, **identity,
            "transport": {"global_records": contract.RECORDS, "transport_sha256": entry["transport_sha256"], **row},
            "denominator": row["end"] - row["start"], "processed": row["end"] - row["start"], "unprocessed": 0, "errors": 0}
            for row in entry["shards"]]
        return entry, reports

    def test_all_three_complete_shards_are_required_with_identical_execution(self):
        entry, reports = self.reports()
        runner.validate_shard_set(reports, entry)
        for changed in (reports[:2], list(reversed(reports)), [reports[0], reports[0], reports[2]]):
            with self.assertRaises(ValueError): runner.validate_shard_set(changed, entry)
        for key, value in (("status", "incomplete"), ("binary_sha256", "b" * 64), ("artifact", {"precision": "q8_0"}),
                           ("unprocessed", 1), ("processed", 0), ("resource_policy", {"threads": 2}),
                           ("python_runtime", {"prefix": "/another-environment"})):
            changed = copy.deepcopy(reports); changed[1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): runner.validate_shard_set(changed, entry)
        reports[1]["errors"] = 1
        runner.validate_shard_set(reports, entry)  # complete transport; error remains in denominator

    def test_receipt_publish_never_overwrites_and_cleans_failed_staging(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            contract.atomic_json(path, {"complete": True})
            before = path.read_bytes()
            with self.assertRaises(FileExistsError): contract.atomic_json(path, {"changed": True})
            self.assertEqual(before, path.read_bytes())
            self.assertEqual([path], list(path.parent.iterdir()))
            with mock.patch.object(os := contract.os, "fsync", side_effect=OSError("disk full")):
                with self.assertRaises(OSError): contract.atomic_json(path.parent / "second.json", {"complete": True})
            self.assertEqual([path], list(path.parent.iterdir()))

    def test_consumed_evidence_hash_and_json_protocol_cannot_be_substituted(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "responses.jsonl"
            path.write_bytes(b'{"event":"error"}\n')
            expected = contract.pin(path)
            self.assertEqual([{"event": "error"}], runner.evidence_rows(path.parent / "report.json", expected, 1024))
            path.write_bytes(b'{"event":"other"}\n')
            with self.assertRaises(ValueError): runner.evidence_rows(path.parent / "report.json", expected, 1024)
            path.write_bytes(b'{"a":1,"a":2}\n')
            with self.assertRaises(ValueError): runner.evidence_rows(path.parent / "report.json", contract.pin(path), 1024)

    def test_shard_output_budget_and_cli_profile_controls_are_explicit(self):
        target = io.BytesIO()
        with mock.patch.object(contract, "MAX_SHARD_BYTES", 4):
            with self.assertRaisesRegex(ValueError, "budget"): runner.write_line(target, {"too": "large"})
        self.assertEqual(b"", target.getvalue())
        args = runner.parser().parse_args(["run-shard", "--prepared-root", "/corpus", "--profile", "entities_ja-JP_char",
            "--model", "multi", "--model-dir", "/model", "--backend", "metal", "--shard", "2", "--output-dir", "/output"])
        self.assertEqual(("entities_ja-JP_char", 2, "metal"), (args.profile, args.shard, args.backend))
        self.assertEqual(125, args.response_timeout)


class PythonInvocationTest(unittest.TestCase):
    def test_worker_command_preserves_real_symlink_venv_and_hashes_executable_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            environment = Path(temporary).resolve() / "selected-venv"
            venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
            invocation = environment / "bin/python"
            self.assertTrue(invocation.is_symlink())
            site_packages = environment / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
            marker = site_packages / "massive_venv_only.py"
            marker.write_text("VALUE = 'selected environment'\n")
            probe = Path(temporary) / "probe.py"
            probe.write_text("import json, sys\n"
                f"sys.path.insert(0, {str(Path(runner.__file__).parent)!r})\n"
                "import evaluate_massive11 as runner\n"
                "from pathlib import Path\n"
                "import massive_venv_only\n"
                "identity = runner.python_runtime_identity()\n"
                "command = runner.python_worker_command(Path('/fixture'), Path('/model'), Path('/upstream'), identity)\n"
                "print(json.dumps({'identity': identity, 'command': command, 'marker': massive_venv_only.VALUE}))\n")
            env = os.environ.copy()
            env.pop("PYTHONHOME", None)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            def run(binary):
                result = subprocess.run([str(binary), str(probe)], env=env, capture_output=True, text=True,
                                        timeout=15, check=True)
                return json.loads(result.stdout)
            first = run(invocation)
            identity = first["identity"]
            self.assertEqual(str(invocation), identity["invocation"])
            self.assertEqual(str(invocation.resolve()), identity["executable"])
            self.assertNotEqual(identity["invocation"], identity["executable"])
            self.assertEqual(oracle.sha256_file(invocation.resolve()), identity["executable_sha256"])
            self.assertEqual(str(environment), identity["prefix"])
            self.assertNotEqual(identity["prefix"], identity["base_prefix"])
            self.assertEqual(oracle.sha256_file(environment / "pyvenv.cfg"), identity["pyvenv_cfg"]["sha256"])
            self.assertEqual(first["command"][-1], facts.digest(facts.encoded(identity)))
            second = run(first["command"][0])
            self.assertEqual(first, second)  # The exact production command retains the venv-only import.
            with (environment / "pyvenv.cfg").open("a") as target:
                target.write("# external configuration change\n")
            changed = run(first["command"][0])
            self.assertNotEqual(identity["pyvenv_cfg"], changed["identity"]["pyvenv_cfg"])
            self.assertNotEqual(first["command"][-1], changed["command"][-1])
            self.assertEqual(identity["executable_sha256"], changed["identity"]["executable_sha256"])

    def test_wrong_interpreter_identity_rejects_before_fixture_or_runtime_load(self):
        args = SimpleNamespace(python_runtime_sha256="0" * 64)
        with mock.patch.object(runner.oracle, "prepare_runtime") as runtime, \
             mock.patch.object(runner.execution, "read_json") as fixture:
            with self.assertRaisesRegex(ValueError, "before model loading"):
                runner.python_worker(args)
            fixture.assert_not_called()
            runtime.assert_not_called()


class TransportLifecycleTest(unittest.TestCase):
    def exercise(self, temporary: Path, *, cancel=False, corrupt=False):
        """Drive the real protocol and aggregator with a disposable fake worker."""
        registry = copy.deepcopy(contract.load_registry())
        entry = contract.profile_entry("intent_en-US", registry)
        profile = entry["profile"]
        schema, metrics = massive.schema_and_metrics(profile, oracle.read_json(massive.MANIFEST))
        options = {"best_effort": False, "overlap": "flat", "threshold": .5, "word_splitter": "whitespace"}
        prepared = temporary / profile["id"] / "prepared"
        prepared.mkdir(parents=True)
        requests, gold, cases = [], [], []
        for index in range(3):
            source = f"massive/1.1/en-US/test/{index}"
            request = {"text": f"Alarm {index}", "schema": schema, "options": options, "offset_unit": "utf8_bytes"}
            common = {"request_id": facts.digest(facts.encoded({"lock": entry["lock_sha256"], "id": source})),
                      "request_sha256": facts.digest(facts.encoded(request))}
            requests.append({**common, "request": request})
            gold.append({**common, "family_id": f"test-family-{index}", "language": "en",
                         "metrics": massive.gold_facts({"intent": "alarm_set"}, profile, oracle.read_json(massive.MANIFEST))})
            cases.append({"id": common["request_id"], "source_id": source, "request_sha256": common["request_sha256"], "text": request["text"]})
            entry["shards"][index] = {"index": index, "start": index, "end": index + 1, "shard_sha256": str(index) * 64,
                "request_ids_sha256": facts.digest(facts.encoded([common["request_id"]])),
                "request_sha256s_sha256": facts.digest(facts.encoded([common["request_sha256"]]))}
        for name, values in (("requests", requests), ("gold", gold)):
            (prepared / f"{name}.jsonl").write_bytes(b"".join(facts.encoded(row) + b"\n" for row in values))
        entry["requests_sha256"] = oracle.sha256_file(prepared / "requests.jsonl")
        receipt = {"scope": "gliner25_blinded_evaluation/v1", "status": "complete", "qualification": False,
            "harness_sha256": registry["frozen_helpers"]["evaluation_contract.py"], "metrics": metrics,
            "requests_sha256": entry["requests_sha256"], "gold_sha256": oracle.sha256_file(prepared / "gold.jsonl"),
            "lock_sha256": entry["lock_sha256"], "records": 3}
        oracle.write_json(prepared / "prepared.json", receipt)
        entry["prepared_sha256"] = oracle.sha256_file(prepared / "prepared.json")
        artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                    **registry["models"]["small"]}
        owners = []

        class FakeGuard:
            def __init__(self, *_): self.peak_rss_bytes = 1234
            def check(self): pass

        class FakeWorker:
            def __init__(self, backend, command, env, directory, guard):
                self.closed = False; self.buffer = bytearray(); self.calls = 0
                self.process = SimpleNamespace(poll=lambda: 0, returncode=0, stdout=io.BytesIO())
                fixture = oracle.read_json(directory / "requests.fixture.json")
                self.ready = {"event": "ready", "scope": contract.WORKER_SCOPE, "backend": backend, "qualification": False,
                    "artifact_kind": "source_fp32", "receipt": None, "source_files": fixture["source_files"], "model": "small",
                    "source_commit": oracle.UPSTREAM_COMMIT, "fixture_sha256": oracle.sha256_file(directory / "requests.fixture.json"),
                    "lock_sha256": fixture["lock_sha256"], "profile": fixture["profile"], "transport": fixture["transport"],
                    "registry_sha256": fixture["registry_sha256"], "word_splitter": "whitespace", "math_policy": "torch_f32_cpu_v1",
                    "weight_precision": "fp32", "activation_precision": "f32", "accumulation_precision": "f32", "head_precision": "f32",
                    "threads": 1, "interop_threads": 1, "provenance": {"commit": oracle.UPSTREAM_COMMIT},
                    "python_runtime": runner.python_runtime_identity()}
                case = fixture["cases"][0]
                self.response = {"event": "result", "case_id": case["id"], "request_sha256": case["request_sha256"],
                    "input_ids": [1, 2], "output": {"intent": {"label": "alarm_set", "confidence": .9}}}
                errors = int(fixture["transport"]["index"] == 1)
                if errors:
                    self.response = {"event": "error", "case_id": case["id"], "request_sha256": case["request_sha256"],
                        "input_ids": None, "error_code": "BoundarySequenceLimitExceeded"}
                self.done = {"event": "complete", "cases": 1, "errors": errors, "qualification": False}
                owners.append(self)
            def receive(self, _timeout):
                self.calls += 1
                if cancel and self.calls == 2: raise KeyboardInterrupt()
                return (self.ready, self.response, self.done)[self.calls - 1]
            def close(self): self.closed = True

        with mock.patch.object(contract, "RECORDS", 3), mock.patch.object(contract, "RANGES", ((0, 1), (1, 2), (2, 3))), \
             mock.patch.object(contract, "load_registry", return_value=registry), \
             mock.patch.object(contract, "admit_profile", return_value=(entry, requests, cases)), \
             mock.patch.object(evaluate, "model_artifact", return_value=artifact), \
             mock.patch.object(runner.bench, "ResourceGuard", FakeGuard), mock.patch.object(runner.bench, "Worker", FakeWorker):
            reports = []
            for index in range(1 if cancel else 3):
                args = runner.parser().parse_args(["run-shard", "--prepared-root", str(temporary), "--profile", profile["id"],
                    "--model", "small", "--model-dir", "/unused", "--backend", "python", "--shard", str(index),
                    "--output-dir", str(temporary / f"run-{index}")])
                report = runner.run_shard(args)
                self.assertTrue(owners[-1].closed)
                reports.append(temporary / f"run-{index}" / "report.json")
            if cancel:
                self.assertEqual("incomplete", report["status"])
                self.assertEqual(1, report["unprocessed"])
                self.assertEqual("KeyboardInterrupt", report["driver_error"]["type"])
                self.assertNotIn("metrics", report)
                return
            if corrupt:
                path = temporary / "run-0/predictions.jsonl"
                value = oracle.read_json(path); value["metrics"]["intent_exact"][0]["label"] = "music_query"
                path.write_bytes(facts.encoded(value) + b"\n")
                report = oracle.read_json(reports[0]); report["files"]["predictions"] = contract.pin(path)
                oracle.write_json(reports[0], report)
            args = runner.parser().parse_args(["aggregate", "--prepared-root", str(temporary), "--profile", profile["id"],
                "--output-dir", str(temporary / "aggregate"), *[item for path in reports for item in ("--shard-report", str(path))]])
            if corrupt:
                with self.assertRaisesRegex(ValueError, "substituted"): runner.aggregate(args)
                self.assertFalse((temporary / "aggregate/report.json").exists())
                failure = oracle.read_json(temporary / "aggregate/failure.json")
                self.assertEqual("incomplete", failure["status"])
                self.assertNotIn("metrics", failure)
            else:
                report = runner.aggregate(args)
                self.assertEqual(("complete", 3, 1, 2), (report["status"], report["denominator"], report["errors"], report["successful_results"]))
                self.assertEqual(3, report["metrics"]["intent_exact"]["support"])
                self.assertEqual(2 / 3, report["direct_document_rates"]["intent_exact"])
                self.assertFalse(report["qualification"])
                self.assertEqual(61, len(report["metrics"]))
                reference = runner.reference_report(temporary / "aggregate/report.json", entry, temporary, "python")
                self.assertEqual(1, reference["report"]["errors"])
                self.assertEqual(3, len(reference["responses"]))

    def test_complete_error_transport_keeps_every_gold_denominator(self):
        with tempfile.TemporaryDirectory() as temporary: self.exercise(Path(temporary))

    def test_interruption_closes_worker_and_never_publishes_metrics(self):
        with tempfile.TemporaryDirectory() as temporary: self.exercise(Path(temporary), cancel=True)

    def test_rehashed_prediction_substitution_keeps_aggregate_uncommitted(self):
        with tempfile.TemporaryDirectory() as temporary: self.exercise(Path(temporary), corrupt=True)


if __name__ == "__main__":
    unittest.main()
