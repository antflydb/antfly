import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import zig_vopr_soak as soak


class SoakTests(unittest.TestCase):
    def test_timeout_kills_and_reaps_a_child_that_ignores_termination(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            child = root / "child.py"
            child.write_text(
                "import os, signal, time\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "print(os.getpid(), flush=True)\n"
                "time.sleep(60)\n"
            )
            with (root / "log").open("w") as log:
                result = soak.run_process(
                    [sys.executable, str(child)], stdout=log, timeout=0.5, grace=0.1
                )
            self.assertEqual(result.returncode, 124)
            self.assertEqual(result.status, "timeout")
            self.assertLess(result.elapsed_seconds, 5)
            pid = int((root / "log").read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_interrupted_campaign_keeps_partial_evidence_and_durable_status(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "vopr"
            binary.write_text(
                f"#!{sys.executable}\n"
                "import os, signal, sys, time\n"
                "from pathlib import Path\n"
                "output = Path(sys.argv[sys.argv.index('--artifact-dir') + 1])\n"
                "(output / 'partial.flight.json').write_text('{}')\n"
                "os.kill(os.getppid(), signal.SIGTERM)\n"
                "time.sleep(60)\n"
            )
            binary.chmod(0o755)
            output = root / "run"
            self.assertEqual(
                soak.run_shard(binary, "raft", 1, 1, root / "empty", output), 130
            )
            status = json.loads((output / "run.json").read_text())
            self.assertEqual(status["status"], "interrupted")
            self.assertFalse(status["completed"])
            self.assertTrue((output / "partial.flight.json").exists())

    def test_liveness_failure_is_distinct_from_safety_and_replay_failures(self):
        result = subprocess.CompletedProcess([], 1)
        report = {
            "properties": [
                {"name": "production-ha-scaling.history-completes", "status": "fail"}
            ]
        }
        self.assertEqual(soak.result_status(result, report), "incomplete_history")
        report["properties"].append(
            {"name": "production-ha-scaling.owners-quiesce", "status": "fail"}
        )
        self.assertEqual(soak.result_status(result, report), "property_failure")
        report["histories"] = {"replay_divergences": 1}
        self.assertEqual(soak.result_status(result, report), "replay_diverged")

    def test_qualification_requires_actual_consumption_of_restored_entries(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            corpus = root / "corpus"
            corpus.mkdir()
            (corpus / "seed.voprtrace").write_text("incompatible trace")
            output = root / "run"

            def campaign(command, **kwargs):
                (output / "results.json").write_text('{"corpus": {"seeded": 0}}')
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=campaign):
                self.assertEqual(
                    soak.run_shard(
                        Path("vopr"), "raft", 1, 1, corpus, output, require_seed=True
                    ),
                    1,
                )
            self.assertEqual(
                json.loads((output / "run.json").read_text())["status"],
                "corpus_not_consumed",
            )

    def test_workflow_resolves_scheduled_and_dispatch_history_budgets(self):
        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-vopr-soak.yml"
        ).read_text()
        step = workflow.split("      - name: Run retained-corpus campaign\n", 1)[1]
        step = step.split("      - name:", 1)[0]
        command = textwrap.dedent(step.split("        run: |\n", 1)[1])
        # Execute the workflow's actual shell; intercept only the expensive
        # campaign launch so the CLI boundary sees exactly what CI would pass.
        capture = "python3() { printf '%s\\0' \"$@\"; }\n"
        for scenario, default in (
            ("ha", 1000),
            ("raft", 1000),
            ("distributed-data", 12),
            ("ha-scaling", 2),
        ):
            for override in ("", "0", "7", "-1"):
                with self.subTest(scenario=scenario, override=override):
                    result = subprocess.run(
                        ["bash", "-e", "-c", capture + command],
                        env={
                            **os.environ,
                            "SCENARIO": scenario,
                            "SHARD": "1",
                            "RUN_NUMBER": "2",
                            "RUNNER_TEMP": "/tmp/vopr soak contract",
                            "HISTORY_BUDGET": override,
                            "DEFAULT_HISTORY_BUDGET": str(default),
                        },
                        check=True,
                        capture_output=True,
                    )
                    arguments = result.stdout.decode().rstrip("\0").split("\0")
                    expected = str(default) if override in ("", "0") else override
                    self.assertEqual(
                        arguments[arguments.index("--histories") + 1], expected
                    )
                    self.assertEqual(
                        arguments[arguments.index("--scenario") + 1], scenario
                    )

    def test_finding_retains_seed_and_failure_evidence_and_fails(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            corpus, output = root / "corpus", root / "run"
            corpus.mkdir()
            (corpus / "retained.voprtrace").write_bytes(b"retained history")
            (corpus / "results.json").write_text("stale report")

            def campaign(command, **kwargs):
                self.assertIn("--fail-on-findings", command)
                self.assertIn("--defer-diagnostics", command)
                self.assertEqual(command[command.index("--workers") + 1], "1")
                self.assertFalse((output / "results.json").exists())
                (output / "history-failure.voprtrace").write_bytes(b"finding")
                (output / "results.json").write_text('{"failed": 1}')
                return subprocess.CompletedProcess(command, 1)

            with patch.object(soak, "run_process", side_effect=campaign):
                self.assertEqual(
                    soak.run_shard(Path("vopr"), "raft", 42, 1, corpus, output), 1
                )
            self.assertTrue((output / "history-failure.voprtrace").exists())
            self.assertEqual(len(list(output.glob("seed-*.voprtrace"))), 1)
            provenance = json.loads((output / "run.json").read_text())
            self.assertEqual(provenance["exit_code"], 1)
            self.assertEqual(provenance["seed"], 42)
            with self.assertRaises(ValueError):
                soak.run_shard(Path("vopr"), "raft", 42, 1, corpus, output)

    def test_missing_aggregate_is_not_success(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with patch.object(
                soak, "run_process", return_value=subprocess.CompletedProcess([], 0)
            ):
                self.assertEqual(
                    soak.run_shard(
                        Path("vopr"), "ha", 1, 1, root / "empty", root / "run"
                    ),
                    1,
                )

    def test_merge_uses_fresh_authority_and_deduplicates_across_shards(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            inputs, output = root / "shards", root / "merged"
            for shard in ("a", "b"):
                (inputs / shard).mkdir(parents=True)
                (inputs / shard / "seed-old.voprtrace").write_bytes(b"old version")
                (inputs / shard / "history-0.voprtrace").write_bytes(b"current history")

            def merge(command, **kwargs):
                self.assertEqual(command[1], "corpus-merge")
                self.assertNotIn("--base", command)
                self.assertTrue(
                    Path(command[command.index("--trace") + 1]).name.startswith(
                        "history-"
                    )
                )
                self.assertEqual(command.count("--trace"), 2)
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": []}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=merge):
                self.assertEqual(soak.merge_corpus(Path("vopr"), inputs, output), 0)

    def test_merge_replay_divergence_is_retained_and_fails(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            inputs, output = root / "shards", root / "merged"
            inputs.mkdir()
            (inputs / "history-0.voprtrace").write_bytes(b"current history")

            def merge(command, **kwargs):
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": [{"reason": "replay_diverged"}]}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=merge):
                self.assertEqual(soak.merge_corpus(Path("vopr"), inputs, output), 1)
            self.assertTrue((output / "index.json").exists())

    def test_divergent_first_history_is_quarantined_with_valid_history_retained(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            inputs, output, retained = (
                root / "shards",
                root / "merged",
                root / "retained",
            )
            inputs.mkdir()
            bad = inputs / "history-0.voprtrace"
            good = inputs / "history-1.voprtrace"
            bad.write_bytes(b"divergent")
            good.write_bytes(b"current history")

            def merge(command, **kwargs):
                self.assertEqual(command[1], "corpus-merge")
                self.assertEqual(
                    command[-4:], ["--trace", str(bad), "--trace", str(good)]
                )
                (output / "trace-good.voprtrace").write_bytes(good.read_bytes())
                (output / "quarantine").mkdir()
                (output / "quarantine" / "bad.voprquarantine").write_bytes(
                    bad.read_bytes()
                )
                (output / "index.json").write_text(
                    json.dumps(
                        {
                            "scenario": "raft-group",
                            "artifacts": [{"path": "trace-good.voprtrace"}],
                            "quarantine": [{"reason": "replay_diverged"}],
                        }
                    )
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=merge):
                self.assertEqual(
                    soak.merge_corpus(Path("vopr"), inputs, output, retained), 1
                )
            self.assertEqual(
                (retained / "trace-good.voprtrace").read_bytes(), good.read_bytes()
            )
            self.assertEqual(
                (output / "quarantine" / "bad.voprquarantine").read_bytes(),
                bad.read_bytes(),
            )

    def test_duplicate_only_campaign_uses_current_replayable_seed_authority(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            corpus, run, output = root / "corpus", root / "run", root / "merged"
            corpus.mkdir()
            (corpus / "current.voprtrace").write_bytes(b"current history")
            (corpus / "old.voprtrace").write_bytes(b"old version")

            def campaign(command, **kwargs):
                (run / "results.json").write_text('{"failed": 0}')
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=campaign):
                self.assertEqual(
                    soak.run_shard(Path("vopr"), "raft", 42, 1, corpus, run), 0
                )
            self.assertEqual(list(run.glob("history-*.voprtrace")), [])

            def merge(command, **kwargs):
                self.assertEqual(command[1], "corpus-merge")
                candidates = [
                    Path(command[i + 1]).read_bytes()
                    for i, arg in enumerate(command)
                    if arg == "--trace"
                ]
                self.assertEqual(set(candidates), {b"current history", b"old version"})
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": [{"reason": "scenario_version_changed"}]}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak, "run_process", side_effect=merge):
                self.assertEqual(soak.merge_corpus(Path("vopr"), run, output), 0)

    def test_no_replayable_authority_fails_without_publishing_a_corpus(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            inputs, output, retained = (
                root / "shards",
                root / "merged",
                root / "retained",
            )
            inputs.mkdir()
            bad = inputs / "history-0.voprtrace"
            bad.write_bytes(b"divergent")

            def replay(command, **kwargs):
                self.assertEqual(command[1], "corpus-merge")
                return subprocess.CompletedProcess(command, 1)

            with patch.object(soak, "run_process", side_effect=replay):
                self.assertEqual(
                    soak.merge_corpus(Path("vopr"), inputs, output, retained), 1
                )
            self.assertFalse(
                json.loads((output / "merge.json").read_text())["completed"]
            )
            self.assertFalse((output / "index.json").exists())
            self.assertFalse(retained.exists())
            self.assertEqual(bad.read_bytes(), b"divergent")

    def test_working_corpus_bounds_clean_traces_and_preserves_unique_findings(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            output, retained = root / "merged", root / "retained"
            output.mkdir()
            artifacts = []
            for index, fingerprints in enumerate(((), (), (), (7,), (7, 8), (9,))):
                name = f"trace-{index}.voprtrace"
                content = "".join(
                    json.dumps(
                        {"type": "failure", "fingerprint": value}, separators=(",", ":")
                    )
                    + "\n"
                    for value in fingerprints
                )
                (output / name).write_text(content)
                artifacts.append({"path": name})
            soak.retain_working_corpus(
                output,
                retained,
                {
                    "scenario": "production-ha-scaling",
                    "artifacts": artifacts,
                },
            )
            self.assertEqual(
                sorted(path.name for path in retained.iterdir()),
                [f"trace-{index}.voprtrace" for index in (0, 1, 3, 4, 5)],
            )
            selection = json.loads((output / "retention.json").read_text())
            self.assertEqual(selection["unique_findings"], 3)
            self.assertEqual(selection["archived"], 1)
            self.assertEqual(len(list(output.glob("*.voprtrace"))), 6)


if __name__ == "__main__":
    unittest.main()
