import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import zig_vopr_soak as soak


class SoakTests(unittest.TestCase):
    def test_finding_retains_seed_and_failure_evidence_and_fails(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            corpus, output = root / "corpus", root / "run"
            corpus.mkdir()
            (corpus / "retained.voprtrace").write_bytes(b"retained history")
            (corpus / "results.json").write_text("stale report")

            def campaign(command, **kwargs):
                self.assertIn("--fail-on-findings", command)
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
                (output / "index.json").write_text(
                    '{"artifacts": [], "quarantine": [{"reason": "replay_diverged"}]}'
                )
                return subprocess.CompletedProcess(command, 0)

            with patch.object(soak.subprocess, "run", side_effect=merge):
                self.assertEqual(soak.merge_corpus(Path("vopr"), inputs, output), 1)
            self.assertTrue((output / "index.json").exists())

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
