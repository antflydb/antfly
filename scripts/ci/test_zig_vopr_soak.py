import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import zig_vopr_soak as soak


class SoakTests(unittest.TestCase):
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

            with patch.object(soak.subprocess, "run", side_effect=campaign):
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
                soak.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)
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
                if command[1] == "replay":
                    self.assertTrue(Path(command[-1]).name.startswith("history-"))
                    return subprocess.CompletedProcess(command, 0)
                self.assertTrue(
                    Path(command[command.index("--base") + 1]).name.startswith(
                        "history-"
                    )
                )
                self.assertEqual(command.count("--trace"), 1)
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": []}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak.subprocess, "run", side_effect=merge):
                self.assertEqual(soak.merge_corpus(Path("vopr"), inputs, output), 0)

    def test_merge_replay_divergence_is_retained_and_fails(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            inputs, output = root / "shards", root / "merged"
            inputs.mkdir()
            (inputs / "history-0.voprtrace").write_bytes(b"current history")

            def merge(command, **kwargs):
                if command[1] == "replay":
                    return subprocess.CompletedProcess(command, 0)
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": [{"reason": "replay_diverged"}]}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak.subprocess, "run", side_effect=merge):
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
            replayed = []

            def merge(command, **kwargs):
                if command[1] == "replay":
                    path = Path(command[-1])
                    replayed.append(path)
                    return subprocess.CompletedProcess(command, int(path == bad))
                self.assertEqual(command[command.index("--base") + 1], str(good))
                self.assertEqual(command[command.index("--trace") + 1], str(bad))
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

            with patch.object(soak.subprocess, "run", side_effect=merge):
                self.assertEqual(
                    soak.merge_corpus(Path("vopr"), inputs, output, retained), 1
                )
            self.assertEqual(replayed, [bad, good])
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

            with patch.object(soak.subprocess, "run", side_effect=campaign):
                self.assertEqual(
                    soak.run_shard(Path("vopr"), "raft", 42, 1, corpus, run), 0
                )
            self.assertEqual(list(run.glob("history-*.voprtrace")), [])

            def merge(command, **kwargs):
                if command[1] == "replay":
                    compatible = Path(command[-1]).read_bytes() == b"current history"
                    return subprocess.CompletedProcess(command, 0 if compatible else 1)
                self.assertEqual(
                    Path(command[command.index("--base") + 1]).read_bytes(),
                    b"current history",
                )
                self.assertEqual(
                    Path(command[command.index("--trace") + 1]).read_bytes(),
                    b"old version",
                )
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": [{"reason": "scenario_version_changed"}]}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak.subprocess, "run", side_effect=merge):
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
                self.assertEqual(command[1], "replay")
                return subprocess.CompletedProcess(command, 1)

            with patch.object(soak.subprocess, "run", side_effect=replay):
                self.assertEqual(
                    soak.merge_corpus(Path("vopr"), inputs, output, retained), 1
                )
            self.assertIn(
                "No corpus candidate exactly replays",
                (output / "merge.log").read_text(),
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
