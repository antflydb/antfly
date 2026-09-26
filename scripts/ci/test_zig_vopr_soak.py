import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import zig_vopr_soak as soak


class SoakTests(unittest.TestCase):
    def test_batched_campaign_runs_both_shards_after_a_failure(self):
        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-vopr-soak.yml"
        ).read_text()
        step = workflow.split("      - name: Run retained-corpus campaign\n", 1)[1]
        step = step.split("      - name:", 1)[0]
        command = textwrap.dedent(step.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stub = root / "stub.py"
            stub.write_text(
                "import json, sys\n"
                "from pathlib import Path\n"
                "args = sys.argv[1:]\n"
                "output = Path(args[args.index('--output') + 1])\n"
                "output.mkdir(parents=True)\n"
                "(output / 'run.json').write_text(json.dumps(args))\n"
                "sys.exit(37 if output.name == '0' else 0)\n"
            )
            command = command.replace(
                "python3 ../scripts/ci/zig_vopr_soak.py", f"python3 {stub}"
            )
            result = subprocess.run(
                ["bash", "-e", "-o", "pipefail", "-c", command],
                cwd=root,
                env={
                    **os.environ,
                    "RUNNER_TEMP": directory,
                    "SCENARIO": "standby",
                    "HISTORY_BUDGET": "0",
                    "DEFAULT_HISTORY_BUDGET": "1000",
                    "RUN_NUMBER": "42",
                    "REQUIRE_SEED": "false",
                },
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            for shard, policy in (("0", "bounded-fair"), ("1", "adversarial")):
                args = json.loads((root / "vopr-run" / shard / "run.json").read_text())
                self.assertEqual(args[args.index("--exploration-policy") + 1], policy)
                self.assertEqual(args[args.index("--histories") + 1], "1000")

    def test_production_pipelines_preserve_the_producer_failure(self):
        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-vopr-soak.yml"
        ).read_text()
        # Match GitHub's invocation for an explicit bash shell; its implicit
        # bash fallback omits pipefail, which previously hid the failed soak.
        explicit_bash = re.search(
            r"(?m)^defaults:\n  run:\n(?:    #[^\n]*\n)*    shell: bash$",
            workflow,
        )
        shell = ["bash", "--noprofile", "--norc", "-e"]
        if explicit_bash:
            shell += ["-o", "pipefail"]
        for step_name in (
            "Build production executable and qualify owner publication",
            "Soak public overwrite restore with concurrent readers and status",
            "Soak cross-shard Autograph resolution, promotion, and hydration",
            "Soak three by three cluster backup delete and restore",
            "Soak large catalog control and diagnostic isolation",
            "Soak concurrent aggregations and transaction session recovery",
        ):
            with (
                self.subTest(step=step_name),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                (root / "production-e2e-soak").mkdir()
                (root / "scripts/ci").mkdir(parents=True)
                (root / "tools").mkdir()
                for helper in (
                    "run_e2e_case.py",
                    "zig_vopr_soak.py",
                    "measure_disk_usage.py",
                ):
                    shutil.copyfile(
                        Path(__file__).with_name(helper), root / "scripts/ci" / helper
                    )
                (root / "tools/run_bounded_zig_build.py").write_text(
                    "import os, sys\n"
                    "args = sys.argv[1:]\n"
                    "assert '--max-rss-cap' in args and '-j1' not in args\n"
                    "os.execvp('zig', ['zig', *args[args.index('--') + 1:]])\n"
                )
                for filename in (
                    "zig",
                    "scripts/ci/zig-e2e-regression-loop.sh",
                    "scripts/ci/zig-e2e-autograph-soak.sh",
                    "scripts/ci/zig-e2e-cluster-restore-soak.sh",
                    "scripts/ci/zig-e2e-catalog-soak.sh",
                    "scripts/ci/zig-e2e-query-transaction-soak.sh",
                ):
                    stub = root / filename
                    stub.write_text(
                        "#!/usr/bin/env bash\necho injected-soak-failure\nexit 37\n"
                    )
                    stub.chmod(0o755)
                step = workflow.split(f"      - name: {step_name}\n", 1)[1]
                step = step.split("      - name:", 1)[0]
                command = textwrap.dedent(step.split("        run: |\n", 1)[1])
                result = subprocess.run(
                    shell + ["-c", command],
                    cwd=root,
                    env={
                        **os.environ,
                        "RUNNER_TEMP": directory,
                        "GITHUB_WORKSPACE": directory,
                        "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, 37, result.stdout + result.stderr)
                logs = list((root / "production-e2e-soak").glob("*.log"))
                self.assertEqual(len(logs), 1)
                self.assertIn("injected-soak-failure", logs[0].read_text())

    def test_qualification_and_runner_build_use_bounded_parallelism(self):
        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-vopr-soak.yml"
        ).read_text()
        self.assertLess(
            workflow.index("      - name: Audit replayable VOPR sources\n"),
            workflow.index("      - name: Test production transport and runtime scheduling\n"),
        )
        for step_name, targets in (
            ("Audit replayable VOPR sources", ["vopr-determinism-audit"]),
            (
                "Test production transport and runtime scheduling",
                [
                    "antfly-raft-transport-test",
                    "standby-vopr-test",
                    "vopr-runtime-test",
                    "restore-admission-vopr-test",
                    "secrets-vopr-test",
                ],
            ),
            ("Build campaign runner", ["vopr-build"]),
        ):
            with (
                self.subTest(step=step_name),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                (root / "tools").mkdir()
                (root / "scripts/ci").mkdir(parents=True)
                shutil.copyfile(
                    Path(__file__).with_name("measure_disk_usage.py"),
                    root / "scripts/ci/measure_disk_usage.py",
                )
                (root / "tools/run_bounded_zig_build.py").write_text(
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    "Path('invocation.json').write_text(json.dumps(sys.argv[1:]))\n"
                    "sys.exit(37)\n"
                )
                step = workflow.split(f"      - name: {step_name}\n", 1)[1]
                step = step.split("      - name:", 1)[0]
                if "        run: |\n" in step:
                    command = textwrap.dedent(step.split("        run: |\n", 1)[1])
                else:
                    command = " ".join(
                        textwrap.dedent(step.split("        run: >-\n", 1)[1]).splitlines()
                    )
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", command],
                    cwd=root,
                    env={
                        **os.environ,
                        "VOPR_LOCAL_CACHE_DIR": "local-cache",
                        "VOPR_GLOBAL_CACHE_DIR": "global-cache",
                        "GITHUB_WORKSPACE": directory,
                        "RUNNER_TEMP": directory,
                    },
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 37, result.stdout + result.stderr)
                args = json.loads((root / "invocation.json").read_text())
                self.assertEqual(
                    args[:4], ["--max-rss-cap", "23622320128", "--", "build"]
                )
                self.assertEqual(args[4 : 4 + len(targets)], targets)
                self.assertNotIn("secrets-test", args)
                self.assertFalse(
                    any(re.fullmatch(r"-j(?:[0-9]+)?", arg) for arg in args)
                )
                self.assertIn("-Doptimize=ReleaseSafe", args)
                self.assertEqual(
                    args[-4:],
                    [
                        "--cache-dir",
                        "local-cache",
                        "--global-cache-dir",
                        "global-cache",
                    ],
                )

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

    def test_timeout_allows_nested_supervisor_to_finish_after_parent_exits(self):
        import signal
        import time

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            heartbeat = root / "heartbeat"
            pid_file = root / "server.pid"
            finished = root / "supervisor-finished"
            child = root / "server.py"
            child.write_text(
                "import os, signal, time\nfrom pathlib import Path\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                f"Path({str(pid_file)!r}).write_text(str(os.getpid()))\n"
                f"p = Path({str(heartbeat)!r})\n"
                "while True:\n p.write_text(str(time.monotonic())); time.sleep(.01)\n"
            )
            supervisor = root / "supervisor.py"
            supervisor.write_text(
                "import sys\nfrom pathlib import Path\n"
                f"sys.path.insert(0, {str(Path(soak.__file__).parent)!r})\n"
                "from zig_vopr_soak import run_process\n"
                f"result = run_process([sys.executable, {str(child)!r}], "
                "stdout=None, timeout=60, grace=.3, clean_descendants=True)\n"
                f"Path({str(finished)!r}).write_text(str(result.returncode))\n"
            )
            # Like the profile wrapper, this parent exits immediately on TERM.
            # Its supervisor owns a server in a separate process group.
            parent = root / "parent.py"
            parent.write_text(
                "import subprocess, sys, time\n"
                f"subprocess.Popen([sys.executable, {str(supervisor)!r}])\n"
                "time.sleep(60)\n"
            )
            try:
                result = soak.run_process(
                    [sys.executable, str(parent)],
                    stdout=subprocess.DEVNULL,
                    timeout=2,
                    grace=2,
                    clean_descendants=True,
                )
                self.assertEqual(result.returncode, 124)
                self.assertTrue(heartbeat.exists(), "server must have started")
                self.assertTrue(
                    finished.exists(), "inner supervisor must finish cleanup"
                )
                self.assertEqual(finished.read_text(), "130")
                stopped = heartbeat.read_text()
                time.sleep(0.1)
                self.assertEqual(heartbeat.read_text(), stopped)
            finally:
                if pid_file.exists():
                    try:
                        os.kill(int(pid_file.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_completed_parent_cannot_leave_writing_server_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            heartbeat = root / "heartbeat"
            child = root / "server.py"
            child.write_text(
                "import signal, time\nfrom pathlib import Path\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                f"p = Path({str(heartbeat)!r})\n"
                "while True:\n p.write_text(str(time.monotonic())); time.sleep(.01)\n"
            )
            parent = root / "parent.py"
            parent.write_text(
                "import subprocess, sys, time\nfrom pathlib import Path\n"
                f"subprocess.Popen([sys.executable, {str(child)!r}])\n"
                f"while not Path({str(heartbeat)!r}).exists(): time.sleep(.01)\n"
            )
            with (root / "log").open("w") as log:
                result = soak.run_process(
                    [sys.executable, str(parent)],
                    stdout=log,
                    timeout=5,
                    grace=0.1,
                    clean_descendants=True,
                )
            self.assertEqual(result.returncode, 0)
            import time

            time.sleep(0.1)
            stopped = heartbeat.read_text()
            time.sleep(0.1)
            self.assertEqual(heartbeat.read_text(), stopped)

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
                {
                    "name": "production-standby-scaling.history-completes",
                    "status": "fail",
                }
            ]
        }
        self.assertEqual(soak.result_status(result, report), "incomplete_history")
        report["properties"].append(
            {"name": "production-standby-scaling.owners-quiesce", "status": "fail"}
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
            ("standby", 1000),
            ("raft", 1000),
            ("distributed-data", 12),
            ("standby-scaling", 2),
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
                        Path("vopr"), "standby", 1, 1, root / "empty", root / "run"
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
                    "scenario": "production-standby-scaling",
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
