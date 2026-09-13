from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import benchmark_cpu as bench
import evaluate as runner
import evaluation_contract as evaluation
import oracle
import prepare_crossner_ai as adapter


STUB = r'''
import hashlib,json,sys
from pathlib import Path
fixture_path,backend,mode,artifact = sys.argv[1:]
artifact=json.loads(artifact)
path=Path(fixture_path)
f=json.loads(path.read_text())
def emit(v): print(json.dumps(v,ensure_ascii=False),flush=True)
emit({'event':'ready','scope':f['scope'],'backend':backend,'qualification':False,
      'artifact_kind':artifact['kind'],'receipt':artifact['receipt'],'source_files':f['source_files'],
      'model':f['model'],'source_commit':f['source_commit'],'fixture_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
      'lock_sha256':f['lock_sha256'],'math_policy':'torch_f32_cpu_v1' if backend=='python' else 'strict_f32_activations_v1',
      'weight_precision':artifact['precision'],'activation_precision':'f32','accumulation_precision':'f32','head_precision':'f32',
      'threads':1,'interop_threads':1,'provenance':{'commit':f['source_commit']}})
errors=0
for i,c in enumerate(f['cases']):
    if mode=='omit' and i==0: continue
    if mode=='oversize' and i==0:
        emit({'event':'error','case_id':c['id'],'request_sha256':c['request_sha256'],'error_code':'BoundarySequenceLimitExceeded','input_ids':None})
        errors+=1
        continue
    names=c['schema']['entities']
    surface=c['text'].split()[-1]
    cpstart=c['text'].rfind(surface)
    if backend=='python':
        groups={name:[] for name in names}
        groups['person']=[{'text':surface,'start':cpstart,'end':len(c['text']),'confidence':.9}]
        output={'entities':groups}
    else:
        groups=[{'name':name,'values':[]} for name in names]
        groups[names.index('person')]['values']=[{'text':surface,'source':{'unit':'utf8_bytes','start':len(c['text'][:cpstart].encode()),'end':len(c['text'].encode())},'confidence':.901 if mode=='confidence_drift' else .9}]
        output={'entities':groups}
    ids=[1,i+2,3]
    if mode=='token_drift' and i==0: ids[-1]=4
    emit({'event':'result','case_id':c['id'],'request_sha256':c['request_sha256'],'input_ids':ids,'output':output})
emit({'event':'complete','cases':len(f['cases']),'errors':errors,'qualification':False})
'''


def corpus(root: Path):
    names = oracle.read_json(adapter.MANIFEST)["entity_types"]
    manifest = oracle.read_json(adapter.MANIFEST)
    manifest.update(source_documents={"train": 1, "dev": 1, "test": 3}, excluded_ids=[])
    labels = ["O", *[prefix + name for name in names for prefix in ("B-", "I-")]]
    files = {"ner_data/ai/train.txt": b"Train\tO\n", "ner_data/ai/dev.txt": b"Dev\tO\n",
             "ner_data/ai/test.txt": "İ\tO\n李\tB-person\n\nTwo\tO\nJohn\tB-person\n\nThird\tO\nAlice\tB-person\n".encode(),
             "src/dataloader.py": ("ai_labels = " + repr(labels) + "\n").encode(),
             "LICENSE": b"Synthetic contract test\n", "README.md": b"No public corpus text in this fixture\n"}
    manifest["files"] = []
    raw = root / "raw"
    for name, data in files.items():
        path = raw / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        manifest["files"].append({"path": name, "size_bytes": len(data), "sha256": evaluation.digest(data),
                                  "git_blob_sha1": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()})
    manifest_path = root / "corpus.json"
    oracle.write_json(manifest_path, manifest)
    return raw, manifest_path


class EvaluationRunnerTests(unittest.TestCase):
    def args(self, root, backend, source=None, native=None, suffix=None):
        return argparse.Namespace(backend=backend, max_rss_mib=256, startup_timeout=10, response_timeout=10,
            lock=root / "locked/lock.json", prepared_dir=root / "locked/prepared", model="small", model_dir=root / "model",
            source_reference_report=source, native_reference_report=native, output_dir=root / (suffix or backend),
            upstream=root / "upstream", binary=root / "stub.py")

    def test_full_blinded_protocol_cpu_metal_and_complete_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw, manifest = corpus(root)
            stub = root / "stub.py"
            stub.write_text(STUB)
            artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                        **runner.source_identity("small")}
            worker_class = bench.Worker
            workers = []

            def start(arm, command, env, output, guard):
                fixture = command[command.index("--evaluation-fixture") + 1]
                worker = worker_class(arm, [sys.executable, str(stub), fixture, arm, "ok", json.dumps(artifact)], env, output, guard)
                workers.append(worker)
                return worker

            with mock.patch.object(adapter, "MANIFEST", manifest), mock.patch.object(runner, "model_artifact", return_value=artifact), mock.patch.object(bench, "Worker", side_effect=start):
                adapter.prepare(raw, root / "locked")
                source = runner.run(self.args(root, "python"))
                native = runner.run(self.args(root, "native", root / "python/report.json"))
                metal = runner.run(self.args(root, "metal", root / "python/report.json", root / "native/report.json"))
                for report in (source, native, metal):
                    self.assertEqual(report["status"], "complete")
                    self.assertEqual(report["denominator"], 3)
                    self.assertEqual(report["completed_results"], 3)
                    self.assertEqual(report["errors"], 0)
                    self.assertEqual(report["metrics"]["entity_exact"]["micro_f1"], 1)
                    self.assertEqual(report["metrics"]["entity_type/algorithm"]["zero_gold_documents"], 3)
                    self.assertFalse(report["qualification"])
                self.assertTrue(native["required_parity"]["pass"])
                self.assertTrue(metal["required_parity"]["pass"])
                self.assertEqual(metal["source_fp32_quality_delta"]["entity_exact"]["micro_f1_loss"], 0)
                fixture = oracle.read_json(root / "metal/requests.fixture.json")
                self.assertNotIn("gold", fixture["cases"][0])
                wrong_artifact = {**artifact, "precision": "q8_0"}
                with self.assertRaisesRegex(evaluation.EvaluationError, "identical bundle"):
                    runner.load_reference(root / "native/report.json", fixture, root / "locked/prepared", "native", wrong_artifact)
                for worker in workers:
                    self.assertEqual(worker.process.poll(), 0)
                changed = oracle.read_json(root / "locked/prepared/prepared.json")
                requests = list(evaluation.rows(root / "locked/prepared/requests.jsonl"))
                requests[0]["request"]["text"] = "Substituted heldout"
                request_path = root / "locked/prepared/requests.jsonl"
                request_path.write_bytes(b"".join(evaluation.encoded(row) + b"\n" for row in requests))
                changed["requests_sha256"] = oracle.sha256_file(request_path)
                oracle.write_json(root / "locked/prepared/prepared.json", changed)
                with self.assertRaisesRegex(evaluation.EvaluationError, "reproduce"):
                    runner.prepare_fixture(root / "locked/lock.json", root / "locked/prepared", "small")

    def test_oversize_and_missing_outputs_block_metrics_without_exclusions(self):
        for mode in ("oversize", "omit"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                raw, manifest = corpus(root)
                stub = root / "stub.py"
                stub.write_text(STUB)
                artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                            **runner.source_identity("small")}
                worker_class = bench.Worker
                workers = []

                def start(arm, command, env, output, guard):
                    fixture = command[command.index("--evaluation-fixture") + 1]
                    worker = worker_class(arm, [sys.executable, str(stub), fixture, arm, mode, json.dumps(artifact)], env, output, guard)
                    workers.append(worker)
                    return worker

                with mock.patch.object(adapter, "MANIFEST", manifest), mock.patch.object(runner, "model_artifact", return_value=artifact), mock.patch.object(bench, "Worker", side_effect=start):
                    adapter.prepare(raw, root / "locked")
                    report = runner.run(self.args(root, "python"))
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["denominator"], 3)
                self.assertGreater(report["errors"], 0)
                self.assertFalse((root / "python/metrics.json").exists())
                if mode == "oversize":
                    self.assertEqual(report["completed_results"], 2)
                    self.assertEqual(report["errors"], 1)
                    self.assertEqual(report["unprocessed"], 0)
                for worker in workers:
                    self.assertIsNotNone(worker.process.poll())

    def test_token_drift_blocks_scoring_and_confidence_drift_never_widens_parity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw, manifest = corpus(root)
            stub = root / "stub.py"
            stub.write_text(STUB)
            artifact = {"kind": "source_fp32", "precision": "fp32", "receipt": None, "receipt_sha256": None,
                        **runner.source_identity("small")}
            worker_class = bench.Worker
            mode = ["ok"]

            def start(arm, command, env, output, guard):
                fixture = command[command.index("--evaluation-fixture") + 1]
                return worker_class(arm, [sys.executable, str(stub), fixture, arm, mode[0], json.dumps(artifact)], env, output, guard)

            with mock.patch.object(adapter, "MANIFEST", manifest), mock.patch.object(runner, "model_artifact", return_value=artifact), mock.patch.object(bench, "Worker", side_effect=start):
                adapter.prepare(raw, root / "locked")
                runner.run(self.args(root, "python"))
                mode[0] = "token_drift"
                drift = runner.run(self.args(root, "native", root / "python/report.json", suffix="token-drift"))
                self.assertEqual(drift["status"], "failed")
                self.assertIn("token IDs differ", drift["driver_error"]["message"])
                self.assertFalse((root / "token-drift/metrics.json").exists())
                mode[0] = "confidence_drift"
                drift = runner.run(self.args(root, "native", root / "python/report.json", suffix="confidence-drift"))
                self.assertEqual(drift["status"], "complete")
                self.assertEqual(drift["metrics"]["entity_exact"]["micro_f1"], 1)
                self.assertFalse(drift["required_parity"]["pass"])
                self.assertEqual(drift["required_parity"]["confidence_absolute_tolerance"], 5e-4)

    def test_fixture_rejects_gold_unknown_queries_options_and_case_overflow(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw, manifest = corpus(root)
            with mock.patch.object(adapter, "MANIFEST", manifest):
                adapter.prepare(raw, root / "locked")
                fixture = runner.prepare_fixture(root / "locked/lock.json", root / "locked/prepared", "small")
                for change in ("gold", "schema", "options", "limit"):
                    broken = copy.deepcopy(fixture)
                    if change == "gold":
                        broken["cases"][0]["gold"] = []
                    elif change == "schema":
                        broken["cases"][0]["schema"]["entities"].pop()
                    elif change == "options":
                        broken["cases"][0]["options"]["threshold"] = .4
                    else:
                        broken["cases"] *= 342
                    with self.subTest(change=change), self.assertRaises(evaluation.EvaluationError):
                        runner.validate_fixture(broken)

    def test_python_limits_are_checked_before_encoder_execution(self):
        names = oracle.read_json(adapter.MANIFEST)["entity_types"]
        case = {"id": "a" * 64, "request_sha256": "b" * 64, "text": "Words", "schema": {"entities": names},
                "options": runner.REQUEST_OPTIONS, "offset_unit": "utf8_bytes"}
        processor = SimpleNamespace(word_splitter=lambda *_args, **_kwargs: range(129))
        with self.assertRaisesRegex(evaluation.EvaluationError, "BoundaryTextLimitExceeded"):
            runner.execute_python_case(SimpleNamespace(processor=processor), case, None)
        processor.word_splitter = lambda *_args, **_kwargs: range(1)
        processor.collate_fn_inference = lambda *_args, **_kwargs: SimpleNamespace(text_tokens=[["word"]], input_ids=SimpleNamespace(shape=(1, 513)))
        with mock.patch.object(oracle, "build_extract_schema", return_value=SimpleNamespace(build=lambda: {})):
            with self.assertRaisesRegex(evaluation.EvaluationError, "BoundarySequenceLimitExceeded"):
                runner.execute_python_case(SimpleNamespace(processor=processor), case, None)

    def test_quality_loss_is_a_signed_measured_delta_not_a_pass(self):
        source = {"records": 3, "lock_sha256": "a" * 64, "metrics": {"entities": {"micro_f1": .8, "support": 7}}}
        actual = copy.deepcopy(source)
        actual["metrics"]["entities"]["micro_f1"] = .7
        delta = runner.quality_delta(source, actual)["entities"]
        self.assertAlmostEqual(delta["micro_f1_loss"], .1)
        self.assertAlmostEqual(delta["micro_f1_delta"], -.1)
        self.assertIsNone(delta["quality_floor"])
        self.assertFalse(delta["quality_qualified"])


if __name__ == "__main__":
    unittest.main()
